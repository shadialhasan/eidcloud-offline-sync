<?php

declare(strict_types=1);

namespace EidCloud\OfflineSync;

use EidCloud\OfflineSync\Clock\HybridLogicalClock;
use EidCloud\OfflineSync\Clock\Timestamp;
use EidCloud\OfflineSync\Crdt\LwwRegister;
use EidCloud\OfflineSync\Queue\MutationQueue;
use EidCloud\OfflineSync\Storage\SqliteStore;
use PDO;
use RuntimeException;

/**
 * Main Offline-First Synchronization Engine for SQLite.
 *
 * Coordinates:
 * - Local CRDT writes and mutations
 * - Tombstone tracking & garbage collection
 * - Generating wire-efficient delta-state JSON payloads
 * - Applying incoming delta-states with deterministic LWW resolution
 * - Logging conflicts and queueing retryable mutations
 */
class SyncEngine
{
    private SqliteStore $store;
    private HybridLogicalClock $clock;
    private MutationQueue $queue;
    private string $nodeId;

    public function __construct(SqliteStore $store, string $nodeId)
    {
        $this->store = $store;
        $this->nodeId = $nodeId;
        $this->clock = new HybridLogicalClock($nodeId);
        $this->queue = new MutationQueue($store->getPdo());
    }

    public static function open(string $sqlitePath, ?string $nodeId = null): self
    {
        $store = SqliteStore::open($sqlitePath);
        $id = $nodeId ?: 'node_' . substr(md5($sqlitePath . gethostname()), 0, 8);
        return new self($store, $id);
    }

    public function getNodeId(): string
    {
        return $this->nodeId;
    }

    public function getStore(): SqliteStore
    {
        return $this->store;
    }

    public function getClock(): HybridLogicalClock
    {
        return $this->clock;
    }

    public function getQueue(): MutationQueue
    {
        return $this->queue;
    }

    /**
     * Perform a local mutation on a record/field (write or update).
     */
    public function setField(string $table, string $recordId, string $column, mixed $value): Timestamp
    {
        $ts = $this->clock->now();
        $strVal = $value === null ? null : (is_scalar($value) ? (string)$value : json_encode($value));

        $this->store->saveFieldMetadata($table, $recordId, $column, $strVal, $ts->toString(), false);
        $this->queue->enqueue($table, $recordId, $column, $strVal, $ts->toString(), false);

        return $ts;
    }

    /**
     * Perform a local record delete (creating tombstones for all its fields or record marker).
     */
    public function deleteRecord(string $table, string $recordId): Timestamp
    {
        $ts = $this->clock->now();
        $this->store->saveFieldMetadata($table, $recordId, '__tombstone__', null, $ts->toString(), true);
        $this->queue->enqueue($table, $recordId, '__tombstone__', null, $ts->toString(), true);

        return $ts;
    }

    /**
     * Generate a wire-efficient delta-state JSON sync payload.
     *
     * @param string|null $sinceHlc Retrieve only mutations strictly after this HLC
     * @param string|null $table Optional filter by table name
     * @return array Wire sync payload structure
     */
    public function exportDeltaPayload(?string $sinceHlc = null, ?string $table = null): array
    {
        $changes = $this->store->getChangesSince($sinceHlc, $table);
        $maxHlc = $sinceHlc ?? '';

        foreach ($changes as $change) {
            if ($change['hlc'] > $maxHlc) {
                $maxHlc = $change['hlc'];
            }
        }

        return [
            'protocol' => 'eidcloud-sync-v1',
            'sender_node_id' => $this->nodeId,
            'exported_at_hlc' => $this->clock->now()->toString(),
            'since_hlc' => $sinceHlc,
            'max_hlc' => $maxHlc ?: null,
            'mutation_count' => count($changes),
            'mutations' => $changes,
        ];
    }

    /**
     * Apply an incoming delta-state sync payload from a peer node.
     *
     * Resolves conflicts via LWW (HLC + NodeId tie-breaker).
     *
     * @param array $payload Wire payload
     * @return array Sync summary: ['applied' => int, 'conflicts' => int, 'ignored' => int]
     */
    public function applyDeltaPayload(array $payload): array
    {
        if (!isset($payload['protocol']) || $payload['protocol'] !== 'eidcloud-sync-v1') {
            throw new RuntimeException("Incompatible or invalid sync protocol payload.");
        }

        $mutations = $payload['mutations'] ?? [];
        $appliedCount = 0;
        $conflictCount = 0;
        $ignoredCount = 0;

        foreach ($mutations as $m) {
            $table = $m['table_name'];
            $recordId = $m['record_id'];
            $column = $m['column_name'];
            $remoteVal = $m['value'];
            $remoteHlcStr = $m['hlc'];
            $remoteIsDeleted = (bool)($m['is_deleted'] ?? false);

            $remoteTs = Timestamp::fromString($remoteHlcStr);
            // Update local clock with remote timestamp (causality tracking)
            $this->clock->update($remoteTs);

            // Fetch current local state
            $local = $this->store->getFieldMetadata($table, $recordId, $column);

            if ($local === null) {
                // No local state exists, remote wins directly
                $this->store->saveFieldMetadata($table, $recordId, $column, $remoteVal, $remoteHlcStr, $remoteIsDeleted);
                $appliedCount++;
            } else {
                $localTs = Timestamp::fromString($local['hlc']);
                $cmp = $remoteTs->compareTo($localTs);

                if ($cmp > 0) {
                    // Remote is strictly newer -> Remote wins
                    $this->store->saveFieldMetadata($table, $recordId, $column, $remoteVal, $remoteHlcStr, $remoteIsDeleted);
                    $this->store->logConflict(
                        $table,
                        $recordId,
                        $column,
                        $remoteHlcStr,
                        $local['hlc'],
                        $remoteVal,
                        $local['value']
                    );
                    $appliedCount++;
                    $conflictCount++;
                } else {
                    // Local is newer or equal -> Local wins, remote mutation ignored
                    $this->store->logConflict(
                        $table,
                        $recordId,
                        $column,
                        $local['hlc'],
                        $remoteHlcStr,
                        $local['value'],
                        $remoteVal
                    );
                    $ignoredCount++;
                    $conflictCount++;
                }
            }
        }

        return [
            'applied' => $appliedCount,
            'conflicts' => $conflictCount,
            'ignored' => $ignoredCount,
        ];
    }

    /**
     * Synchronize directly between two SyncEngines (in-process peer sync).
     *
     * @param SyncEngine $peer
     * @return array [ 'local_applied' => array, 'peer_applied' => array ]
     */
    public function syncWithPeer(SyncEngine $peer): array
    {
        // 1. Export delta from local to peer
        $localDelta = $this->exportDeltaPayload();
        $peerResult = $peer->applyDeltaPayload($localDelta);

        // 2. Export delta from peer to local
        $peerDelta = $peer->exportDeltaPayload();
        $localResult = $this->applyDeltaPayload($peerDelta);

        return [
            'peer_applied' => $peerResult,
            'local_applied' => $localResult,
        ];
    }

    /**
     * Materialize CRDT state into target application SQLite table.
     * Reconstructs standard SQLite rows from CRDT field metadata.
     */
    public function materializeTable(string $table, string $primaryKey = 'id'): void
    {
        $pdo = $this->store->getPdo();

        // Get all active (non-tombstoned) records for this table
        $stmt = $pdo->prepare("
            SELECT record_id, column_name, value, is_deleted
            FROM _eid_crdt_metadata
            WHERE table_name = ?
            ORDER BY record_id ASC
        ");
        $stmt->execute([$table]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $records = [];
        $deletedRecords = [];

        foreach ($rows as $row) {
            $rId = $row['record_id'];
            if ($row['column_name'] === '__tombstone__' && (int)$row['is_deleted'] === 1) {
                $deletedRecords[$rId] = true;
                continue;
            }
            if (!isset($records[$rId])) {
                $records[$rId] = [$primaryKey => $rId];
            }
            $records[$rId][$row['column_name']] = $row['value'];
        }

        // Remove deleted records
        foreach (array_keys($deletedRecords) as $delId) {
            unset($records[$delId]);
            $pdo->prepare("DELETE FROM {$table} WHERE {$primaryKey} = ?")->execute([$delId]);
        }

        // Upsert into target table
        foreach ($records as $rId => $cols) {
            $colNames = array_keys($cols);
            $placeholders = array_fill(0, count($cols), '?');
            $updates = array_map(fn($col) => "{$col} = excluded.{$col}", $colNames);

            $sql = sprintf(
                "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT(%s) DO UPDATE SET %s",
                $table,
                implode(', ', $colNames),
                implode(', ', $placeholders),
                $primaryKey,
                implode(', ', $updates)
            );

            $stmt = $pdo->prepare($sql);
            $stmt->execute(array_values($cols));
        }
    }
}

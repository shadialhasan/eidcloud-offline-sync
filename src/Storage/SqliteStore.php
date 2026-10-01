<?php

declare(strict_types=1);

namespace EidCloud\OfflineSync\Storage;

use PDO;
use RuntimeException;

/**
 * SQLite Metadata Storage Handler.
 *
 * Manages CRDT metadata tables, tombstone records, sync state, and conflict audit logs.
 */
class SqliteStore
{
    private PDO $pdo;

    public function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->initializeSchema();
    }

    public static function open(string $path): self
    {
        $pdo = new PDO('sqlite:' . $path);
        return new self($pdo);
    }

    public function getPdo(): PDO
    {
        return $this->pdo;
    }

    /**
     * Initialize sync metadata tables:
     * - _eid_crdt_metadata: column-level / record-level CRDT state (table_name, record_id, column_name, value, hlc, is_deleted)
     * - _eid_mutation_queue: pending mutations waiting to sync to peers (id, table_name, record_id, column_name, value, hlc, is_deleted, attempts, status, created_at)
     * - _eid_sync_state: keeps track of latest applied HLC per peer/table (peer_id, table_name, last_hlc, updated_at)
     * - _eid_conflicts_log: audit trail of resolved conflicts (id, table_name, record_id, column_name, winner_hlc, loser_hlc, winner_val, loser_val, resolved_at)
     */
    public function initializeSchema(): void
    {
        $this->pdo->exec("
            CREATE TABLE IF NOT EXISTS _eid_crdt_metadata (
                table_name TEXT NOT NULL,
                record_id TEXT NOT NULL,
                column_name TEXT NOT NULL,
                value TEXT,
                hlc TEXT NOT NULL,
                is_deleted INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (table_name, record_id, column_name)
            );

            CREATE INDEX IF NOT EXISTS idx_eid_crdt_hlc ON _eid_crdt_metadata (hlc);
            CREATE INDEX IF NOT EXISTS idx_eid_crdt_tbl_rec ON _eid_crdt_metadata (table_name, record_id);

            CREATE TABLE IF NOT EXISTS _eid_mutation_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                record_id TEXT NOT NULL,
                column_name TEXT NOT NULL,
                value TEXT,
                hlc TEXT NOT NULL,
                is_deleted INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending', -- pending, processing, completed, failed
                last_error TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_eid_queue_status ON _eid_mutation_queue (status, attempts);

            CREATE TABLE IF NOT EXISTS _eid_sync_state (
                peer_id TEXT NOT NULL,
                table_name TEXT NOT NULL,
                last_hlc TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (peer_id, table_name)
            );

            CREATE TABLE IF NOT EXISTS _eid_conflicts_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                record_id TEXT NOT NULL,
                column_name TEXT NOT NULL,
                winner_hlc TEXT NOT NULL,
                loser_hlc TEXT NOT NULL,
                winner_val TEXT,
                loser_val TEXT,
                resolved_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        ");
    }

    /**
     * Get CRDT field state
     */
    public function getFieldMetadata(string $table, string $recordId, string $column): ?array
    {
        $stmt = $this->pdo->prepare("
            SELECT table_name, record_id, column_name, value, hlc, is_deleted
            FROM _eid_crdt_metadata
            WHERE table_name = ? AND record_id = ? AND column_name = ?
        ");
        $stmt->execute([$table, $recordId, $column]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        return $row ?: null;
    }

    /**
     * Save CRDT field state
     */
    public function saveFieldMetadata(string $table, string $recordId, string $column, ?string $value, string $hlc, bool $isDeleted): void
    {
        $stmt = $this->pdo->prepare("
            INSERT INTO _eid_crdt_metadata (table_name, record_id, column_name, value, hlc, is_deleted, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(table_name, record_id, column_name) DO UPDATE SET
                value = excluded.value,
                hlc = excluded.hlc,
                is_deleted = excluded.is_deleted,
                updated_at = CURRENT_TIMESTAMP
        ");
        $stmt->execute([$table, $recordId, $column, $value, $hlc, $isDeleted ? 1 : 0]);
    }

    /**
     * Get delta changes since a specific HLC timestamp
     * @return array[]
     */
    public function getChangesSince(?string $sinceHlc = null, ?string $table = null): array
    {
        $query = "SELECT table_name, record_id, column_name, value, hlc, is_deleted FROM _eid_crdt_metadata";
        $params = [];
        $where = [];

        if ($sinceHlc !== null && $sinceHlc !== '') {
            $where[] = "hlc > ?";
            $params[] = $sinceHlc;
        }

        if ($table !== null && $table !== '') {
            $where[] = "table_name = ?";
            $params[] = $table;
        }

        if (!empty($where)) {
            $query .= " WHERE " . implode(" AND ", $where);
        }

        $query .= " ORDER BY hlc ASC";

        $stmt = $this->pdo->prepare($query);
        $stmt->execute($params);
        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * Log a conflict resolution event
     */
    public function logConflict(string $table, string $recordId, string $column, string $winnerHlc, string $loserHlc, ?string $winnerVal, ?string $loserVal): void
    {
        $stmt = $this->pdo->prepare("
            INSERT INTO _eid_conflicts_log (table_name, record_id, column_name, winner_hlc, loser_hlc, winner_val, loser_val)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ");
        $stmt->execute([$table, $recordId, $column, $winnerHlc, $loserHlc, $winnerVal, $loserVal]);
    }

    /**
     * Get all logged conflicts
     */
    public function getConflicts(int $limit = 100): array
    {
        $stmt = $this->pdo->prepare("
            SELECT * FROM _eid_conflicts_log ORDER BY id DESC LIMIT ?
        ");
        $stmt->execute([$limit]);
        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * Garbage collect tombstones older than a given HLC threshold
     */
    public function purgeTombstonesBefore(string $hlcThreshold): int
    {
        $stmt = $this->pdo->prepare("
            DELETE FROM _eid_crdt_metadata
            WHERE is_deleted = 1 AND hlc < ?
        ");
        $stmt->execute([$hlcThreshold]);
        return $stmt->rowCount();
    }
}

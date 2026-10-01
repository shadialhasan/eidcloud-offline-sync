<?php

declare(strict_types=1);

namespace EidCloud\OfflineSync\Queue;

use PDO;
use RuntimeException;

/**
 * Mutation transaction queue with retry and exponential backoff.
 * Ensures mutations occurring offline are persisted locally and reliably delivered to peers.
 */
class MutationQueue
{
    private PDO $pdo;

    public function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
    }

    /**
     * Enqueue a local mutation
     */
    public function enqueue(
        string $table,
        string $recordId,
        string $column,
        ?string $value,
        string $hlc,
        bool $isDeleted = false
    ): int {
        $stmt = $this->pdo->prepare("
            INSERT INTO _eid_mutation_queue (table_name, record_id, column_name, value, hlc, is_deleted, attempts, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 0, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ");
        $stmt->execute([$table, $recordId, $column, $value, $hlc, $isDeleted ? 1 : 0]);
        return (int) $this->pdo->lastInsertId();
    }

    /**
     * Fetch pending mutations up to $limit
     * @return array[]
     */
    public function getPending(int $limit = 100): array
    {
        $stmt = $this->pdo->prepare("
            SELECT id, table_name, record_id, column_name, value, hlc, is_deleted, attempts, status
            FROM _eid_mutation_queue
            WHERE status IN ('pending', 'retry')
            ORDER BY id ASC
            LIMIT ?
        ");
        $stmt->execute([$limit]);
        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * Mark a mutation as completed/synced
     */
    public function markCompleted(int $id): void
    {
        $stmt = $this->pdo->prepare("
            UPDATE _eid_mutation_queue
            SET status = 'completed', updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ");
        $stmt->execute([$id]);
    }

    /**
     * Record failure with exponential backoff retry calculation
     *
     * @param int $id Mutation ID
     * @param string $error Error message
     * @param int $maxAttempts Maximum allowed retry attempts
     * @return int Next backoff delay in seconds
     */
    public function markFailed(int $id, string $error, int $maxAttempts = 5): int
    {
        $stmt = $this->pdo->prepare("SELECT attempts FROM _eid_mutation_queue WHERE id = ?");
        $stmt->execute([$id]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        if (!$row) {
            throw new RuntimeException("Mutation ID {$id} not found.");
        }

        $attempts = ((int)$row['attempts']) + 1;
        $status = ($attempts >= $maxAttempts) ? 'failed' : 'retry';

        // Exponential backoff: 2^(attempts-1) seconds (1s, 2s, 4s, 8s, 16s...)
        $backoffSeconds = (int) min(3600, pow(2, max(0, $attempts - 1)));

        $update = $this->pdo->prepare("
            UPDATE _eid_mutation_queue
            SET attempts = ?, status = ?, last_error = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ");
        $update->execute([$attempts, $status, $error, $id]);

        return $backoffSeconds;
    }

    /**
     * Clear completed mutations older than specified retention (e.g. 7 days or immediate)
     */
    public function purgeCompleted(int $keepLast = 1000): int
    {
        $stmt = $this->pdo->prepare("
            DELETE FROM _eid_mutation_queue
            WHERE status = 'completed' AND id NOT IN (
                SELECT id FROM _eid_mutation_queue WHERE status = 'completed' ORDER BY id DESC LIMIT ?
            )
        ");
        $stmt->execute([$keepLast]);
        return $stmt->rowCount();
    }

    /**
     * Total queue count by status
     */
    public function getStats(): array
    {
        $stmt = $this->pdo->query("
            SELECT status, COUNT(*) as count
            FROM _eid_mutation_queue
            GROUP BY status
        ");
        $results = $stmt->fetchAll(PDO::FETCH_KEY_PAIR);
        return [
            'pending' => (int)($results['pending'] ?? 0),
            'retry' => (int)($results['retry'] ?? 0),
            'completed' => (int)($results['completed'] ?? 0),
            'failed' => (int)($results['failed'] ?? 0),
        ];
    }
}

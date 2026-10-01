<?php

declare(strict_types=1);

namespace EidCloud\OfflineSync\Tests;

use EidCloud\OfflineSync\Clock\Timestamp;
use EidCloud\OfflineSync\Clock\HybridLogicalClock;
use EidCloud\OfflineSync\Crdt\LwwRegister;
use EidCloud\OfflineSync\Crdt\OrSet;
use EidCloud\OfflineSync\Queue\MutationQueue;
use EidCloud\OfflineSync\Storage\SqliteStore;
use EidCloud\OfflineSync\SyncEngine;
use PDO;
use RuntimeException;

/**
 * Full test suite covering 2-node offline edits, merge, conflict resolution, queues, and clocks.
 */
class OfflineSyncTest
{
    private int $passes = 0;
    private int $failures = 0;

    public function runAll(): bool
    {
        echo "==========================================================" . PHP_EOL;
        echo "🚀 Running eidcloud-offline-sync Verification Test Suite" . PHP_EOL;
        echo "==========================================================" . PHP_EOL;

        $this->testHlcMonotonicity();
        $this->testHlcCausalityAcrossNodes();
        $this->testLwwRegisterResolution();
        $this->testOrSetAddWins();
        $this->testMutationQueueAndBackoff();
        $this->testTwoNodeOfflineDivergenceAndReconciliation();
        $this->testTombstoneSoftDeleteAndMaterialize();

        echo "----------------------------------------------------------" . PHP_EOL;
        echo "RESULTS: {$this->passes} Passed, {$this->failures} Failed." . PHP_EOL;
        echo "==========================================================" . PHP_EOL;

        return $this->failures === 0;
    }

    private function assert(string $testName, bool $condition, string $message = ''): void
    {
        if ($condition) {
            $this->passes++;
            echo "  ✅ PASS: {$testName}" . PHP_EOL;
        } else {
            $this->failures++;
            echo "  ❌ FAIL: {$testName} - {$message}" . PHP_EOL;
        }
    }

    public function testHlcMonotonicity(): void
    {
        $clock = new HybridLogicalClock('node-1');
        $ts1 = $clock->now();
        $ts2 = $clock->now();

        $this->assert('HLC Monotonicity (Same Millis advances counter)', $ts2->compareTo($ts1) > 0);
        $this->assert('HLC String Serialization round-trip', Timestamp::fromString($ts1->toString())->toString() === $ts1->toString());
    }

    public function testHlcCausalityAcrossNodes(): void
    {
        $clockA = new HybridLogicalClock('node-A');
        $clockB = new HybridLogicalClock('node-B');

        $tsA1 = $clockA->now();
        // Node B receives message with tsA1
        $tsB1 = $clockB->update($tsA1);

        $this->assert('HLC Causality Propagation (tsB1 > tsA1)', $tsB1->compareTo($tsA1) > 0);
    }

    public function testLwwRegisterResolution(): void
    {
        $ts1 = new Timestamp(1000, 0, 'node-A');
        $ts2 = new Timestamp(1000, 1, 'node-A');
        $tsTieA = new Timestamp(1000, 0, 'node-A');
        $tsTieB = new Timestamp(1000, 0, 'node-B');

        $reg = new LwwRegister('initial', $ts1);
        $reg->set('updated', $ts2);
        $this->assert('LWW Register updates on higher counter', $reg->getValue() === 'updated');

        // Deterministic tie-breaking on node id (B > A in strcmp)
        $regTie = new LwwRegister('val-A', $tsTieA);
        $regB = new LwwRegister('val-B', $tsTieB);
        $regTie->merge($regB);
        $this->assert('LWW Register deterministic tie-break selects higher node-id', $regTie->getValue() === 'val-B');
    }

    public function testOrSetAddWins(): void
    {
        $setA = new OrSet();
        $setB = new OrSet();

        $clock = new HybridLogicalClock('node-1');
        $tag1 = $clock->now();
        $setA->add('user:123', $tag1);

        $setB->merge($setA);
        $this->assert('OR-Set replication to peer B', $setB->contains('user:123'));

        $tagRemove = $clock->now();
        $setB->remove('user:123', $tagRemove);
        $this->assert('OR-Set item removed on B', !$setB->contains('user:123'));

        $setA->merge($setB);
        $this->assert('OR-Set tombstone replicated back to A', !$setA->contains('user:123'));
    }

    public function testMutationQueueAndBackoff(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $store = new SqliteStore($pdo);
        $queue = new MutationQueue($pdo);

        $id = $queue->enqueue('users', 'u1', 'email', 'test@example.com', '1000-00000:node-1');
        $pending = $queue->getPending();
        $this->assert('Mutation enqueued successfully', count($pending) === 1 && $pending[0]['id'] == $id);

        $backoff = $queue->markFailed($id, 'Connection timed out', 5);
        $this->assert('Exponential backoff calculated for attempt 1 (1s)', $backoff === 1);

        $queue->markCompleted($id);
        $stats = $queue->getStats();
        $this->assert('Queue status marked completed', $stats['completed'] === 1 && $stats['pending'] === 0);
    }

    public function testTwoNodeOfflineDivergenceAndReconciliation(): void
    {
        // Setup Node Alpha and Node Beta with in-memory SQLite stores
        $storeA = new SqliteStore(new PDO('sqlite::memory:'));
        $storeB = new SqliteStore(new PDO('sqlite::memory:'));

        $engineA = new SyncEngine($storeA, 'node-alpha');
        $engineB = new SyncEngine($storeB, 'node-beta');

        // Create base product record on Node A
        $engineA->setField('products', 'prod-1', 'title', 'Mechanical Keyboard');
        $engineA->setField('products', 'prod-1', 'price', '150');

        // Initial sync to Node B
        $engineA->syncWithPeer($engineB);

        // Nodes go offline!
        // Node A updates title to "Mechanical Keyboard RGB"
        $tsA = $engineA->setField('products', 'prod-1', 'title', 'Mechanical Keyboard RGB');

        // Node B (concurrently offline) updates title to "Custom Mechanical Keyboard" and updates price to "180"
        usleep(2000); // ensure physical clock progress
        $tsB = $engineB->setField('products', 'prod-1', 'title', 'Custom Mechanical Keyboard');
        $engineB->setField('products', 'prod-1', 'price', '180');

        // Nodes reconnect and synchronize bidirectionally
        $engineA->syncWithPeer($engineB);

        // Verify deterministic convergence
        $valA = $storeA->getFieldMetadata('products', 'prod-1', 'title')['value'];
        $valB = $storeB->getFieldMetadata('products', 'prod-1', 'title')['value'];
        $this->assert('Convergence: Title on Node A matches Node B', $valA === $valB);

        // Since Node B wrote later, "Custom Mechanical Keyboard" wins
        $this->assert('LWW: Winner title is latest write', $valA === 'Custom Mechanical Keyboard');

        // Price should be 180 on both
        $priceA = $storeA->getFieldMetadata('products', 'prod-1', 'price')['value'];
        $priceB = $storeB->getFieldMetadata('products', 'prod-1', 'price')['value'];
        $this->assert('Convergence: Price is synchronized to 180 on both', $priceA === '180' && $priceB === '180');

        // Verify conflict logs were generated
        $conflictsA = $storeA->getConflicts();
        $this->assert('Conflict log recorded on Node A', count($conflictsA) > 0);
    }

    public function testTombstoneSoftDeleteAndMaterialize(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $store = new SqliteStore($pdo);
        $engine = new SyncEngine($store, 'node-main');

        // Prepare physical SQLite application table
        $pdo->exec("CREATE TABLE customers (id TEXT PRIMARY KEY, name TEXT, email TEXT);");

        $engine->setField('customers', 'c-1', 'name', 'Alice Smith');
        $engine->setField('customers', 'c-1', 'email', 'alice@eidcloud.com');
        $engine->materializeTable('customers');

        $stmt = $pdo->query("SELECT * FROM customers WHERE id = 'c-1'");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assert('Materialized initial record', $row && $row['name'] === 'Alice Smith');

        // Soft delete record via tombstone
        $engine->deleteRecord('customers', 'c-1');
        $engine->materializeTable('customers');

        $stmt = $pdo->query("SELECT * FROM customers WHERE id = 'c-1'");
        $rowDeleted = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assert('Record removed from materialized view after tombstone', $rowDeleted === false);
    }
}

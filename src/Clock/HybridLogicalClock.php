<?php

declare(strict_types=1);

namespace EidCloud\OfflineSync\Clock;

use RuntimeException;

/**
 * Hybrid Logical Clock (HLC)
 *
 * Implements Kulkarni et al. HLC algorithm ensuring:
 * 1. e -> f implies hlc(e) < hlc(f) (causality)
 * 2. |hlc(e).millis - physical_time(e)| is bounded (closeness to real-time)
 */
class HybridLogicalClock
{
    private string $nodeId;
    private int $lastMillis;
    private int $counter;
    private int $maxDriftMillis;

    /**
     * @param string $nodeId Unique identifier for this node
     * @param int $maxDriftMillis Maximum allowed physical clock drift before rejecting (default 60,000ms = 60s)
     */
    public function __construct(string $nodeId, int $maxDriftMillis = 60000)
    {
        $this->nodeId = $nodeId;
        $this->lastMillis = 0;
        $this->counter = 0;
        $this->maxDriftMillis = $maxDriftMillis;
    }

    public function getNodeId(): string
    {
        return $this->nodeId;
    }

    /**
     * Get current physical time in milliseconds.
     */
    protected function getPhysicalMillis(): int
    {
        return (int) floor(microtime(true) * 1000);
    }

    /**
     * Generate a new monotonically increasing timestamp for a local event.
     */
    public function now(): Timestamp
    {
        $physical = $this->getPhysicalMillis();

        if ($physical > $this->lastMillis) {
            $this->lastMillis = $physical;
            $this->counter = 0;
        } else {
            $this->counter++;
        }

        return new Timestamp($this->lastMillis, $this->counter, $this->nodeId);
    }

    /**
     * Update clock based on receiving a message with remote timestamp.
     * Ensures local clock advances past the received timestamp.
     */
    public function update(Timestamp $remote): Timestamp
    {
        $physical = $this->getPhysicalMillis();

        if ($remote->millis - $physical > $this->maxDriftMillis) {
            throw new RuntimeException(
                "Clock drift exceeded threshold: remote {$remote->millis} vs local physical {$physical}"
            );
        }

        $maxMillis = max($physical, $this->lastMillis, $remote->millis);

        if ($maxMillis === $this->lastMillis && $maxMillis === $remote->millis) {
            $this->counter = max($this->counter, $remote->counter) + 1;
        } elseif ($maxMillis === $this->lastMillis) {
            $this->counter++;
        } elseif ($maxMillis === $remote->millis) {
            $this->counter = $remote->counter + 1;
        } else {
            $this->counter = 0;
        }

        $this->lastMillis = $maxMillis;

        return new Timestamp($this->lastMillis, $this->counter, $this->nodeId);
    }
}

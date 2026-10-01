<?php

declare(strict_types=1);

namespace EidCloud\OfflineSync\Clock;

use InvalidArgumentException;

/**
 * Hybrid Logical Clock (HLC) implementation.
 *
 * Provides monotonically increasing, causality-preserving timestamps combining:
 * - Physical time (milliseconds since epoch)
 * - Logical counter for events happening within the same physical millisecond
 * - Unique Node ID as deterministic tie-breaker
 *
 * String representation: "{millis}-{counter}:{nodeId}"
 * Example: "1727798400000-00001:node-A"
 */
final class Timestamp
{
    public function __construct(
        public readonly int $millis,
        public readonly int $counter,
        public readonly string $nodeId
    ) {
        if ($this->millis < 0) {
            throw new InvalidArgumentException("Millis cannot be negative.");
        }
        if ($this->counter < 0) {
            throw new InvalidArgumentException("Counter cannot be negative.");
        }
        if (trim($this->nodeId) === '') {
            throw new InvalidArgumentException("Node ID cannot be empty.");
        }
    }

    public function toString(): string
    {
        return sprintf('%d-%05d:%s', $this->millis, $this->counter, $this->nodeId);
    }

    public function __toString(): string
    {
        return $this->toString();
    }

    public static function fromString(string $raw): self
    {
        $pattern = '/^(\d+)-(\d+):(.+)$/';
        if (!preg_match($pattern, trim($raw), $matches)) {
            throw new InvalidArgumentException("Invalid HLC timestamp format: '{$raw}'");
        }

        return new self((int)$matches[1], (int)$matches[2], $matches[3]);
    }

    /**
     * Compare this timestamp with another.
     * Returns:
     *  < 0 if this < other
     *    0 if this == other
     *  > 0 if this > other
     */
    public function compareTo(Timestamp $other): int
    {
        if ($this->millis !== $other->millis) {
            return $this->millis <=> $other->millis;
        }

        if ($this->counter !== $other->counter) {
            return $this->counter <=> $other->counter;
        }

        return strcmp($this->nodeId, $other->nodeId);
    }
}

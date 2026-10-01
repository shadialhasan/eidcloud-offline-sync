<?php

declare(strict_types=1);

namespace EidCloud\OfflineSync\Crdt;

use EidCloud\OfflineSync\Clock\Timestamp;

/**
 * Last-Write-Wins Register (LWW-Register).
 *
 * State-based Conflict-Free Replicated Data Type.
 * Value updates overwrite older states if and only if the incoming HLC timestamp is strictly greater.
 * If timestamps match in physical time and logical counter, the nodeId breaks the tie deterministically.
 */
class LwwRegister
{
    private mixed $value;
    private Timestamp $timestamp;
    private bool $isTombstone;

    public function __construct(mixed $value, Timestamp $timestamp, bool $isTombstone = false)
    {
        $this->value = $value;
        $this->timestamp = $timestamp;
        $this->isTombstone = $isTombstone;
    }

    public function getValue(): mixed
    {
        return $this->value;
    }

    public function getTimestamp(): Timestamp
    {
        return $this->timestamp;
    }

    public function isTombstone(): bool
    {
        return $this->isTombstone;
    }

    /**
     * Set a new value with a newer timestamp.
     */
    public function set(mixed $newValue, Timestamp $newTimestamp, bool $isTombstone = false): bool
    {
        if ($newTimestamp->compareTo($this->timestamp) > 0) {
            $this->value = $newValue;
            $this->timestamp = $newTimestamp;
            $this->isTombstone = $isTombstone;
            return true;
        }

        return false;
    }

    /**
     * Merge another LWW register into this one.
     * Returns true if this register's state changed.
     */
    public function merge(LwwRegister $other): bool
    {
        if ($other->getTimestamp()->compareTo($this->timestamp) > 0) {
            $this->value = $other->getValue();
            $this->timestamp = $other->getTimestamp();
            $this->isTombstone = $other->isTombstone();
            return true;
        }

        return false;
    }

    public function toArray(): array
    {
        return [
            'value' => $this->value,
            'timestamp' => $this->timestamp->toString(),
            'is_tombstone' => $this->isTombstone,
        ];
    }

    public static function fromArray(array $data): self
    {
        return new self(
            $data['value'] ?? null,
            Timestamp::fromString($data['timestamp']),
            (bool)($data['is_tombstone'] ?? false)
        );
    }
}

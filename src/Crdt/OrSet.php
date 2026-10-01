<?php

declare(strict_types=1);

namespace EidCloud\OfflineSync\Crdt;

use EidCloud\OfflineSync\Clock\Timestamp;

/**
 * Observed-Remove Set (OR-Set) / Add-Wins Observed-Remove Set with HLC metadata.
 *
 * Useful for collections/tags where items can be added and removed concurrently.
 * Adds generate unique element tags (hlc timestamp).
 * A removal removes all matching tags observed prior to or at that removal.
 */
class OrSet
{
    /**
     * Set of active elements: [element_key => [tag => Timestamp]]
     */
    private array $elements = [];

    /**
     * Tombstones: [element_key => [tag => Timestamp]]
     */
    private array $tombstones = [];

    public function add(string $item, Timestamp $tag): void
    {
        $this->elements[$item][$tag->toString()] = $tag;
    }

    public function remove(string $item, Timestamp $removeTag): void
    {
        if (isset($this->elements[$item])) {
            foreach ($this->elements[$item] as $tagStr => $tag) {
                if ($removeTag->compareTo($tag) >= 0) {
                    $this->tombstones[$item][$tagStr] = $tag;
                    unset($this->elements[$item][$tagStr]);
                }
            }
            if (empty($this->elements[$item])) {
                unset($this->elements[$item]);
            }
        }
    }

    public function contains(string $item): bool
    {
        return !empty($this->elements[$item]);
    }

    /**
     * Get all active elements
     * @return string[]
     */
    public function read(): array
    {
        return array_keys($this->elements);
    }

    /**
     * Merge another OR-Set into this one.
     */
    public function merge(OrSet $other): void
    {
        // 1. Merge tombstones
        foreach ($other->tombstones as $item => $tags) {
            foreach ($tags as $tagStr => $ts) {
                $this->tombstones[$item][$tagStr] = $ts;
                if (isset($this->elements[$item][$tagStr])) {
                    unset($this->elements[$item][$tagStr]);
                }
            }
            if (isset($this->elements[$item]) && empty($this->elements[$item])) {
                unset($this->elements[$item]);
            }
        }

        // 2. Merge elements (unless present in our tombstones)
        foreach ($other->elements as $item => $tags) {
            foreach ($tags as $tagStr => $ts) {
                if (!isset($this->tombstones[$item][$tagStr])) {
                    $this->elements[$item][$tagStr] = $ts;
                }
            }
        }
    }

    public function toArray(): array
    {
        $elementsOut = [];
        foreach ($this->elements as $item => $tags) {
            $elementsOut[$item] = array_keys($tags);
        }

        $tombstonesOut = [];
        foreach ($this->tombstones as $item => $tags) {
            $tombstonesOut[$item] = array_keys($tags);
        }

        return [
            'elements' => $elementsOut,
            'tombstones' => $tombstonesOut,
        ];
    }
}

[🇸🇦 العربية](README.ar.md) | [🇬🇧 English](README.md)

# 🔄 eidcloud-offline-sync

[![Release](https://img.shields.io/badge/release-v1.0.0-blue.svg)](https://github.com/shadi-alhasan/eidcloud-offline-sync/releases)
[![PHP Version](https://img.shields.io/badge/php-%3E%3D8.2-8892BF.svg)](https://php.net)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/shadi-alhasan/eidcloud-offline-sync/blob/main/notebooks/quickstart.ipynb)
[![CI](https://github.com/shadi-alhasan/eidcloud-offline-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/shadi-alhasan/eidcloud-offline-sync/actions)

Distributed, offline-first data synchronization engine for SQLite using **Conflict-Free Replicated Data Types (CRDTs)** and **Hybrid Logical Clocks (HLC)** in **pure PHP 8.2+** with **zero external vendor dependencies**.

---

## 🎯 Architecture & Synchronization Flow

```mermaid
flowchart TD
    subgraph Node_Alpha [Peer Node Alpha (SQLite)]
        LA[Local Application Writes] --> QA[Local Mutation Queue]
        QA --> CRDTA[CRDT Metadata Store]
        HLCA[Hybrid Logical Clock] -. Generates Monotonic HLC .-> CRDTA
    end

    subgraph Sync_Transport [Wire-Efficient JSON Sync Payload]
        DELTA_AB[Delta State Payload &Delta;A]
        DELTA_BA[Delta State Payload &Delta;B]
    end

    subgraph Node_Beta [Peer Node Beta (SQLite)]
        LB[Local Application Writes] --> QB[Local Mutation Queue]
        QB --> CRDTB[CRDT Metadata Store]
        HLCB[Hybrid Logical Clock] -. Generates Monotonic HLC .-> CRDTB
    end

    CRDTA -->|Export Delta| DELTA_AB
    DELTA_AB -->|Apply with LWW Resolution| CRDTB
    CRDTB -->|Export Delta| DELTA_BA
    DELTA_BA -->|Apply with LWW Resolution| CRDTA

    CRDTA --> MAT_A[Materialized View SQLite Tables]
    CRDTB --> MAT_B[Materialized View SQLite Tables]
```

---

## ⚡ Core Capabilities

- **Zero-Dependency Pure PHP 8.2+**: Operates directly on native PHP PDO SQLite with no external libraries or heavy frameworks.
- **Delta-State CRDT Synchronization**: Exchange wire-efficient JSON payloads containing only records mutated since the last synchronization epoch (`since_hlc`).
- **Hybrid Logical Clocks (HLC)**: Monotonically increasing causal timestamps combining physical wall-clock milliseconds, a logical event counter, and deterministic node identifier tie-breaking.
- **Conflict Resolution (LWW)**: Automatic Last-Write-Wins convergence for field-level updates with deterministic conflict audit logging (`_eid_conflicts_log`).
- **Tombstone & Soft Deletes**: Deletion markers with garbage collection preventing phantom resurrection of deleted records.
- **Local Mutation Queue**: Durable offline transaction journal with exponential backoff retry scheduling for resilient network transport.
- **Table Materialization**: Projects internal CRDT column metadata directly into standard relational SQLite application tables.

---

## 📦 Installation

Ensure PHP 8.2 or later is installed with the `pdo_sqlite` and `json` extensions:

```bash
git clone https://github.com/shadi-alhasan/eidcloud-offline-sync.git
cd eidcloud-offline-sync
```

Add via Composer (PSR-4 autoloading):

```json
{
  "require": {
    "eidcloud/offline-sync": "^1.0.0"
  }
}
```

---

## 💻 CLI Usage

The bundled CLI tool `bin/eidcloud-sync` offers full administration and synchronization features:

```bash
# Initialize a SQLite database for CRDT synchronization
php bin/eidcloud-sync node-init --db=local.sqlite --node-id=node-alpha

# Synchronize bidirectionally with a peer SQLite database
php bin/eidcloud-sync sync --db=local.sqlite --peer=peer.sqlite

# Export wire-ready delta JSON payload (for HTTP/WebSockets transport)
php bin/eidcloud-sync sync --db=local.sqlite --json

# View conflict resolution audit report
php bin/eidcloud-sync conflicts-report --db=local.sqlite

# View local mutation queue statistics
php bin/eidcloud-sync stats --db=local.sqlite
```

---

## 🛠️ Programmatic PHP Usage

```php
use EidCloud\OfflineSync\SyncEngine;

// 1. Initialize engine on local SQLite file
$engine = SyncEngine::open('warehouse.sqlite', 'field-tablet-01');

// 2. Perform offline writes (automatically tracked via HLC and enqueued)
$ts = $engine->setField('inventory', 'item-882', 'quantity', 45);
$engine->setField('inventory', 'item-882', 'updated_by', 'Agent Shadi');

// 3. Materialize CRDT metadata into standard SQLite tables
$engine->materializeTable('inventory');

// 4. Export delta sync payload for remote sync hub
$payload = $engine->exportDeltaPayload();

// 5. Apply incoming delta payload from peer
$summary = $engine->applyDeltaPayload($incomingPayload);
echo "Applied: {$summary['applied']}, Conflicts Resolved: {$summary['conflicts']}\n";
```

---

## 🧪 Running Automated Tests

Run the zero-dependency test runner verifying 2-node offline edits, concurrent merge, LWW conflict resolution, and soft deletes:

```bash
php tests/run_tests.php
```

Output:
```text
==========================================================
🚀 Running eidcloud-offline-sync Verification Test Suite
==========================================================
  ✅ PASS: HLC Monotonicity (Same Millis advances counter)
  ✅ PASS: HLC String Serialization round-trip
  ✅ PASS: HLC Causality Propagation (tsB1 > tsA1)
  ✅ PASS: LWW Register updates on higher counter
  ✅ PASS: LWW Register deterministic tie-break selects higher node-id
  ✅ PASS: OR-Set replication to peer B
  ✅ PASS: OR-Set item removed on B
  ✅ PASS: OR-Set tombstone replicated back to A
  ✅ PASS: Mutation enqueued successfully
  ✅ PASS: Exponential backoff calculated for attempt 1 (1s)
  ✅ PASS: Queue status marked completed
  ✅ PASS: Convergence: Title on Node A matches Node B
  ✅ PASS: LWW: Winner title is latest write
  ✅ PASS: Convergence: Price is synchronized to 180 on both
  ✅ PASS: Conflict log recorded on Node A
  ✅ PASS: Materialized initial record
  ✅ PASS: Record removed from materialized view after tombstone
----------------------------------------------------------
RESULTS: 17 Passed, 0 Failed.
==========================================================
```

---

## 📓 Interactive Quickstart

Open our interactive 3-cell Colab notebook demonstrating offline node partition, concurrent edits, and deterministic reconciliation:

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/shadi-alhasan/eidcloud-offline-sync/blob/main/notebooks/quickstart.ipynb)

---

## 👨‍💻 Author & Maintainer

**Eng. MHD. Shadi AL-Hasan**  
Specialist in Distributed Systems, Cloud Architecture, and Offline-First Enterprise Platforms.

---

## 📄 License

This software is released under the **[MIT License](LICENSE)**.  
Copyright &copy; 2026 MHD. Shadi AL-Hasan.

---

## 👤 Author & Maintainer

**Eng. MHD. Shadi AL-Hasan**  
- **Role:** Executive CTO & Enterprise Solutions Architect  
- **Email:** [mhd.shadi.alhasan@gmail.com](mailto:mhd.shadi.alhasan@gmail.com)  
- **Phone / WhatsApp:** [+963934005922](tel:+963934005922)  
- **Location:** Damascus, Syria  
- **GitHub:** [shadialhasan](https://github.com/shadialhasan)  

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.  
Copyright (c) 2026 **MHD. Shadi AL-Hasan**. All rights reserved.

# On-Chain Whale Intelligence Pipeline (Ethereum)

## Overview

This project is an **end-to-end on-chain data engineering pipeline** focused on tracking **Ethereum whale wallet activity** (primarily exchange wallets) and converting raw blockchain data into **analytics-ready tables and dashboards**.

The system is designed to be:

* **Free-tier friendly** (Alchemy + Dune)
* **Modular & production-inspired**
* **Analytics-first**, not just data collection

Instead of relying on paid indexing services, the pipeline directly fetches data from Ethereum RPC, decodes it, stores structured outputs, and uses **Dune dashboards** for visualization.

---

## What the Pipeline Does

Currently, the pipeline performs **two core functions**:

### 1. On-Chain Data Extraction & Decoding

* Fetches Ethereum blocks in a **configurable block range** (e.g. 10–20 blocks per run) from **Alchemy RPC**
* Parses:

  * Transactions
  * Logs
  * ERC-20 `Transfer` events
* Filters data for **whale wallets** (exchange hot/cold wallets)
* Decodes and enriches raw blockchain data

### 2. Analytics & Visualization

* Stores **raw blockchain data** and **filtered analytical data** in **separate tables**
* Uses **Dune dashboards** for analytics and visualization
* Offloads heavy querying & visualization to Dune, keeping local infra lightweight

---

## Why This Architecture?

### Key Design Decisions

* **Limited block windows (10–20 blocks)** to stay within free RPC limits
* **Raw vs Processed tables** to mirror production data warehouse design
* **Dune for analytics** to avoid infra cost while still building professional dashboards

This mirrors how real Web3 data teams work:

> raw data → decoded layer → analytics layer → dashboards

---

## Tech Stack

### Data Ingestion

* Python
* Web3.py
* Alchemy Ethereum RPC

### Storage

* SQLite (local)
* Structured analytical tables (separate from raw tables)

### Analytics & Dashboards

* Dune SQL
* Dune Dashboards

---

## Data Flow (High Level)

* Fetch blocks from Ethereum
* Decode transactions & logs
* Filter whale wallet activity
* Store clean, analytics-ready tables
* Visualize insights using Dune

---

## Current Capabilities

* Block-level ingestion (batch-based)
* Whale wallet filtering
* ERC-20 transfer decoding
* Clean analytical tables
* Dune-based dashboards

---

## Planned Enhancements

* Larger block windows (incremental indexing)
* Real-time / near-real-time streaming
* Token metadata caching
* USD value enrichment
* Alerting for large whale movements
* Multi-chain support

---

## Example Use Cases

* Exchange inflow/outflow analysis
* Whale accumulation & distribution patterns
* Liquidity stress indicators
* Early signals for market movement

---

## Who Is This For?

* Web3 data engineers
* On-chain analysts
* Crypto researchers
* Anyone interested in how real blockchain data pipelines are built

---

## Status

Actively building & iterating in public

---

# System Workflow

```
┌────────────────────┐
│   Ethereum Chain   │
│  (Blocks, Tx, Logs)│
└─────────┬──────────┘
          │
          │ 1. Fetch blocks (10–20 range)
          ▼
┌────────────────────┐
│   Alchemy RPC      │
│ (Ethereum Mainnet) │
└─────────┬──────────┘
          │
          │ 2. Raw block & tx data
          ▼
┌──────────────────────────────┐
│ Raw Data Layer (SQLite)      │
│ - blocks                     │
│ - transactions               │
│ - logs                       │
└─────────┬────────────────────┘
          │
          │ 3. Decode & filter
          ▼
┌──────────────────────────────┐
│ Processing / Decoder Layer   │
│ - whale wallet filter        │
│ - ERC20 Transfer decoding    │
│ - inflow / outflow tagging   │
└─────────┬────────────────────┘
          │
          │ 4. Clean analytics tables
          ▼
┌──────────────────────────────┐
│ Analytics Data Layer         │
│ - whale_transactions         │
│ - whale_token_transfers      │
│ - summary tables             │
└─────────┬────────────────────┘
          │
          │ 5. SQL-based analytics
          ▼
┌──────────────────────────────┐
│ Dune Analytics               │
│ - Dune SQL queries           │
│ - Interactive dashboards     │
│ - Public insights            │
└──────────────────────────────┘
---
## Key Takeaway

This project demonstrates real-world Web3 data engineering:

* Raw blockchain data ingestion
* Decoding & enrichment
* Analytics-ready data modeling
* Resource-efficient dashboarding

All built with free-tier tools, but following **production-grade thinking.

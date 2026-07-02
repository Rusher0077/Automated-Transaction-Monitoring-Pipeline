# Project Blueprint — Automated Transaction Monitoring Pipeline

**Author:** Pallab Dey

**Status:** Complete

**Domain:** Fintech — Mobile Money Transactions

**Stack:** Databricks (PySpark / SQL) · n8n · PaySim Dataset · Telegram Bot API

---

## Project Overview

An end-to-end transaction monitoring pipeline built on Databricks and automated via n8n. The pipeline ingests 6.3 million synthetic mobile money transactions, models them into a star schema, computes daily and weekly business KPIs, evaluates four rule-based risk triggers, and delivers reports and alerts to Telegram on a cron schedule.

The full project narrative, design decisions, and known limitations are documented in the [README](../README.md). This document covers technical reference like schema, logic, and build structure.

---

## Dataset

**PaySim** — synthetic mobile money transaction data generated from real aggregated logs, published as part of academic fraud-detection research.

- ~6.3 million rows · ~470MB
- 31 simulated days · five transaction types
- Contains ground-truth fraud labels (`isFraud`) and structural quirks requiring cleaning judgment

**Transaction types:** `CASH_IN`, `CASH_OUT`, `TRANSFER`, `PAYMENT`, `DEBIT`

**Source columns:**

| Column | Description |
|---|---|
| `step` | Time unit (1 step = 1 hour, 744 steps = 31 days) |
| `type` | Transaction type |
| `amount` | Transaction amount |
| `nameOrig` | Origin account ID |
| `oldbalanceOrg` / `newbalanceOrig` | Origin account balances before and after |
| `nameDest` | Destination account ID |
| `oldbalanceDest` / `newbalanceDest` | Destination account balances before and after |
| `isFraud` | Ground truth fraud label (0/1) |
| `isFlaggedFraud` | System fraud flag for large illegal transfers (0/1) |

---

## Date Offset

Transaction dates are derived from the `step` column (`day = (step - 1) // 24`) and then shifted forward so the dataset's last day aligns with yesterday relative to the current run date. This is computed in Python at ingestion:

```python
offset_days = current_date - max(dataset_date)
simulated_date = original_date + offset_days
```

This gives daily and weekly cron triggers something genuinely "new" to query each day without modifying the underlying transaction data.

---

## Schema

All tables live in `hive_metastore.default`. When querying via the Databricks SQL Warehouse API, the active catalog is `workspace`, so tables are referenced as `workspace.default.<table_name>`.

### raw_transactions

Untouched ingested data from the PaySim CSV.

### transactions_with_dates

Raw data with `transaction_date` and `simulated_date` columns added.

### dim_date

| Column | Type |
|---|---|
| `date` | DATE |
| `day_of_week` | STRING |
| `week_number` | INT |
| `month` | INT |
| `is_weekend` | BOOLEAN |

### dim_transaction_type

| Column | Type |
|---|---|
| `type_id` | INT |
| `type_name` | STRING |

### dim_account

| Column | Type |
|---|---|
| `account_id` | STRING |
| `first_seen_date` | DATE |
| `last_seen_date` | DATE |
| `total_transactions` | INT |

### fact_daily_transactions

Core fact table. One row per transaction.

| Column | Type | Description |
|---|---|---|
| `transaction_id` | STRING | Unique transaction identifier |
| `date` | DATE | Simulated transaction date |
| `account_orig` | STRING | Origin account ID |
| `account_dest` | STRING | Destination account ID |
| `transaction_type` | STRING | One of the five transaction types |
| `amount` | DOUBLE | Transaction amount |
| `balance_before_orig` | DOUBLE | Origin balance before transaction |
| `balance_after_orig` | DOUBLE | Origin balance after transaction |
| `is_fraud` | INT | Ground truth fraud label (0/1) |
| `is_flagged_fraud` | INT | System fraud flag (0/1) |

### account_balance_snapshot

Daily snapshot of account states. Provides the rolling baseline used by the spike trigger.

| Column | Type | Description |
|---|---|---|
| `snapshot_date` | DATE | Date of snapshot |
| `account_id` | STRING | Account identifier |
| `closing_balance` | DOUBLE | End-of-day balance (`MAX_BY` on transaction time) |
| `daily_transaction_count` | INT | Transactions on this day |
| `daily_volume` | DOUBLE | Total transaction volume for the day |
| `rolling_30d_avg_count` | DOUBLE | 30-day rolling average transaction count |

### kpi_daily

| Column | Type | Description |
|---|---|---|
| `report_date` | DATE | KPI date |
| `total_volume` | DOUBLE | Sum of all transaction amounts |
| `transaction_count` | INT | Total transaction count |
| `fraud_flag_rate` | DOUBLE | % transactions where `isFraud = 1` |
| `avg_transaction_size` | DOUBLE | `total_volume / transaction_count` |
| `dod_volume_change_pct` | DOUBLE | % change vs previous day |
| `dod_count_change_pct` | DOUBLE | % change vs previous day |

### kpi_weekly

| Column | Type | Description |
|---|---|---|
| `week_start` | DATE | Monday of the week |
| `total_volume` | DOUBLE | Weekly sum of transaction amounts |
| `transaction_count` | INT | Weekly transaction count |
| `wow_volume_change_pct` | DOUBLE | % change vs prior week |
| `wow_count_change_pct` | DOUBLE | % change vs prior week |
| `type_mix_cash_in_pct` | DOUBLE | % of volume from `CASH_IN` |
| `type_mix_cash_out_pct` | DOUBLE | % of volume from `CASH_OUT` |
| `type_mix_transfer_pct` | DOUBLE | % of volume from `TRANSFER` |
| `cashout_to_cashin_ratio` | DOUBLE | `CASH_OUT` volume / `CASH_IN` volume |
| `top_5_accounts_by_count` | STRING | JSON array of top 5 accounts by transaction count |

### alert_flags

One row per day. Consumed by the n8n emergency alert workflow.

| Column | Type | Description |
|---|---|---|
| `flag_date` | DATE | Date the flags were evaluated |
| `flag_transaction_spike` | BOOLEAN | Trigger 1 result |
| `flag_chain_attack` | BOOLEAN | Trigger 2 result |
| `flag_fraudulent_activity` | BOOLEAN | Trigger 3 result |
| `flag_volume_collapse` | BOOLEAN | Trigger 4 result |
| `any_flag_active` | BOOLEAN | TRUE if any of the above is TRUE |

31 rows total. `any_flag_active = TRUE` on 15 days.

---

## KPIs

### Daily KPIs

| KPI | Definition |
|---|---|
| Total Transaction Volume | `SUM(amount)` for the day |
| Transaction Count | `COUNT(*)` for the day |
| Fraud Flag Rate | `COUNT(isFraud=1) / COUNT(*) * 100` |
| Average Transaction Size | `total_volume / transaction_count` |

### Weekly KPIs

- WoW change in total volume and transaction count
- Transaction type mix by volume percentage
- `CASH_OUT`-to-`CASH_IN` ratio
- Top 5 accounts by transaction count (as JSON)

---

## Alert Trigger Conditions

| Trigger | Condition |
|---|---|
| 1 — Transaction Spike | Account executes more than 3× its own 30-day average transaction count within 24 hours |
| 2 — Transfer → Cash-Out Chain | A `TRANSFER` followed by a `CASH_OUT` from the destination account on the same day, where the `CASH_OUT` amount exceeds the top 5% of daily `CASH_OUT` amounts (~$518K) |
| 3 — Fraud Rate Breach | Daily `isFraud` rate exceeds 1% of total transactions |
| 4 — Volume Collapse | Daily total volume drops more than 40% vs. the same day the previous week |

Full reasoning and validated results for each trigger: [`alert_flag_logic.md`](alert_flag_logic.md)

---

## n8n Workflows

Three cron-triggered workflows query the Databricks SQL Warehouse via HTTP and deliver formatted messages to Telegram.

| Workflow | Schedule | Query | Behaviour |
|---|---|---|---|
| Daily KPI Monitor | 11:00 AM daily | `kpi_daily` | Sends four KPIs with DoD changes |
| Weekly Performance Summary | 11:05 AM Mondays | `kpi_weekly` | Sends WoW summary, type mix, ratio |
| Emergency Alert Engine | 11:10 AM daily | `alert_flags` | Silent if no flags active; sends alert if any flag is TRUE |

Databricks is queried via `/api/2.0/sql/statements` using Header Auth credentials stored in n8n's credential manager, not inline in the workflow files.

---

## Repository Structure

```
Automated-Transaction-Monitoring-Pipeline/
│
├── Databricks/
│   ├── 01_ingestion/
│   │   ├── apply_date_offset.ipynb
│   │   └── load_raw_transactions.sql
│   ├── 02_transformation/
│   │   ├── account_balance_snapshot.sql
│   │   ├── dim_account.sql
│   │   ├── dim_date.sql
│   │   ├── dim_transaction_type.sql
│   │   └── fact_daily_transactions.sql
│   ├── 03_kpis/
│   │   ├── compute_kpi_daily.sql
│   │   └── compute_kpi_weekly.sql
│   └── 04_triggers/
│       ├── alert_flags.sql
│       ├── trigger_01_flag_transaction_spikes.sql
│       ├── trigger_02_flag_money_laundering.sql
│       ├── trigger_03_flag_fraud_rate.sql
│       └── trigger_04_flag_volume_collapse.sql
│
├── n8n/
│   ├── workflow_daily_alert.json
│   ├── workflow_weekly_summary.json
│   └── workflow_emergency_trigger.json
│
├── docs/
│   ├── project_blueprint.md
│   ├── alert_flag_logic.md
│   ├── screenshots/
│   │   ├── workflow_daily_alert_canvas.png
│   │   ├── workflow_weekly_summary_canvas.png
│   │   └── workflow_emergency_trigger_canvas.png
│   └── gifs/
│       ├── workflow_daily_alert_demo.gif
│       ├── workflow_weekly_summary_demo.gif
│       └── workflow_emergency_trigger_demo.gif
│
└── README.md
```

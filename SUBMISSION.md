# Submission

## Candidate
- **Name:** Assistant
- **Assignment chosen:** A (Data) then B (Workflow)
- **Time spent (honest estimate):** ~8 hours

## How to run

```bash
# From the repository root

# Assignment A - Interaction Ledger & Reporting Pipeline
cd solution/assignment-a
# Generate data (provided tool):
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
# Ingest drops:
python pipeline.py ingest data/drop_1
python pipeline.py ingest data/drop_2
# Generate reports:
python pipeline.py report --out reports/

# Assignment B - Durable Campaign Dispatcher
cd solution/assignment-b
# Start mock provider:
python -m mock_telemetry --failure-scale 0
# Run dispatcher:
python run_dispatcher.py
```

## Design

### Assignment A: Interaction Ledger & Reporting Pipeline

**Database schema (SQLite, with Supabase fallback):**
- `leads` table: canonical phone (`+91XXXXXXXXXX`), masked phone (`+91******XXXX`), external_ref, name, batch_id, workflow-specific fields
- `interaction_ledger` table: append-only event record (event_id, event_type, workflow_id, call_id, occurred_at, phone, disposition, is_terminal, is_success)
- `rejections` table: rejected rows with reason
- `ingested_drops` table: tracks processed drops for idempotency

**Key design decisions:**
- **PII protection:** Canonical phone stored as `+91XXXXXXXXXX` in DB; reports mask to `+91******XXXX`
- **Header normalisation:** Lower-case, trim, replace `_`/whitespace runs with single space; match against schema `name` or `aliases`
- **Duplicate merging:** First valid row for `(workflow_id, canonical_phone)` wins; its `batch_id` is permanent; later rows last-write-non-null for other fields
- **Idempotency:** `ingested_drops` table tracks processed drops; re-running skips already-ingested data
- **Event linking:** Events linked to leads via `(workflow_id, normalised phone)`; `disposition.override` events linked via `call_id`
- **Bucket assignment:** `yet_to_reach_out` (no calls) > `completed` (terminal disposition) > `in_progress_telephony_failed` (all calls have telephony_failed) > `in_progress_dialed` (everything else)

**Output files:**
- `reports/batch_summary.csv` - grouped by `(workflow_id, batch_id)` with bucket counts
- `reports/daily_overview.csv` - one row per workflow per IST date with calls
- `reports/lead_report.csv` - one row per lead with masked phone, bucket, call stats
- `reports/rejections.csv` - rejected rows, no PII
- `reports/data_quality.json` - aggregate statistics

### Assignment B: Durable Campaign Dispatcher

**State machine design (database-backed):**
- SQLite database tracks lead states, call records, and processed request IDs
- `request_id` serves as idempotency key - reusing same key is always safe
- All business rules (R1-R9) enforced at dispatch time
- Crash recovery: reads DB state and resumes without re-dialling

**Key business rule implementations:**
- **R1 (Calling window):** Checks both campaign window and regulatory window 09:00-21:00 IST
- **R2 (Concurrency):** Per-tenant limit shared across all campaigns; checked before each dial
- **R3 (One call per phone):** DB query ensures no active calls to same phone number
- **R4 (Attempts):** Max 3 attempts per lead per campaign; counted from calls table
- **R5 (Retries):** 60 min after no_answer/busy, 15 min after failed; tracked from call ended_at
- **R6 (Callbacks):** Due callbacks before retries before fresh leads; callback_at stored per lead
- **R7 (Terminal dispositions):** interested/not_interested/wrong_number/dnd end the lead
- **R8 (DND global):** DND flag stored per phone number; never dial again in any campaign
- **R9 (No lost leads):** Precedence: completed > exhausted > dnd_blocked

**State lifecycle:**
- Lead starts as `active`
- After each call: update status/disposition, increment attempts
- If terminal disposition: mark as `completed`
- If max attempts reached without terminal: mark as `exhausted`
- If DND encountered: mark phone as globally DND, remaining leads as `dnd_blocked`
- Final state written to `final_state.csv`

**Output files:**
- `final_state.csv` - campaign_id, lead_id, final_state, attempts, last_status, last_disposition

## Decisions & assumptions

| Decision | Why | Alternative considered |
|---|---|---|
| SQLite over PostgreSQL for Assignment A | Simpler, file-based, no separate server needed for the time box | PostgreSQL for better concurrency and SQL-based report aggregation |
| Database-backed state machine for Assignment B | Survives `kill -9` crashes; persistent across restarts | Temporal SDK - gives automatic failure handling but adds complexity |
| Mask phone `+91******XXXX` in reports | Meets PII protection requirement while keeping last 4 digits visible | Store encrypted phone; harder to verify reports |
| `request_id` as idempotency key for Assignment B | Provider guarantees at-most-once action per key; simple DB tracking | UUID-based keys without storage; risk of losing track on crash |

## Failure modes considered

### Assignment A
- **Bad input data:** Invalid phones, impossible dates, malformed CSVs → rejected with specific reason
- **Re-ingestion:** Already-ingested drops skipped; leads/events not duplicated
- **Missing leads:** Orphan events quarantined, not included in reports
- **Date format ambiguity:** Day-first parsing handles DD/MM/YYYY, DD-MM-YYYY, DD-Mon-YYYY
- **Duplicate events:** Deduped by `event_id` across all drops

### Assignment B
- **`kill -9` mid-dispatch:** DB state persisted; on restart, reads resumes from last checkpoint
- **Duplicate webhooks:** Idempotent via `request_id` check; already-processed calls skipped
- **Lost webhooks:** Provider may not deliver; recovery by checking call state via `/v1/calls/{call_sid}`
- **503/504 from provider:** 503 = retry later (doesn't count as attempt); 504 = ambiguous, call may or may not have been placed
- **Concurrency violations:** Checked before each dial; 429 response recorded as violation
- **DND violations:** Global DND flag prevents re-dialling; `dnd_blocked` state for leads that shouldn't be called

## Testing

### Assignment A
- Phone normalisation tested against all 6 formats from generator
- Date parsing tested against YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY, DD-Mon-YYYY formats
- Decimal parsing tested with ₹, Rs, Rs., INR prefixes and comma thousands separators
- Edge cases: invalid phones (9 digits, 5 prefix, landline, N/A, +1 format), impossible dates (31/02/2026), bad amounts (N/A, -, text)
- Rejection reasons verified: missing_required:customer_name, missing_required:phone, invalid_phone
- Duplicate merging: first-write permanent batch_id, last-write-non-null for merged fields
- Re-running pipeline idempotent: already-ingested drops skipped

### Assignment B
- Mock provider tested with `--failure-scale 0` (no injected failures)
- Basic call placement and webhook processing verified
- Concurrency limits enforced (4 for acme_insurance, 3 for zen_retail)
- Callback handling tested
- State persistence verified across restarts (SQLite DB)
- Violation count tracked via `/admin/report`

## If I had another day

### Assignment A
- **Performance:** Optimize for `--scale large` (70k leads per workflow)
- **SQL-based reports:** Generate daily_overview.csv and lead_report.csv using SQL queries
- **Additional validation:** Support more date/currency formats
- **Better deduplication:** Track duplicates across drops more robustly

### Assignment B
- **Temporal implementation:** Port to Temporal SDK for built-in failure handling
- **Horizontal scaling:** Two dispatcher processes with distributed locks
- **Observability:** Structured logging, `/status` endpoint with per-campaign progress
- **Callback optimisation:** Minimise callback lateness by smarter scheduling
- **Production notes:** Circuit breakers, dead-letter queues for webhooks, monitoring/alerting on violation count
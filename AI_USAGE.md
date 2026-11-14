# AI Usage Disclosure

AI tools are allowed. We want to **how** you used them, because using them well is a skill. Honest answers are never held against you. Discovering undisclosed use is.

## Tools used

- Nemotron 3.5 Lightning Free (via OpenCode/OpenCode agent)
- Python 3.11+ standard library (csv, json, re, datetime, decimal, hashlib, hmac, sqlite3)
- supabase Python package (for Assignment A, optional - used as fallback when URL/key provided)
- fastapi/uvicorn/httpx (Assignment B mock provider)
- pytest (testing)
- OpenCode harness (task execution framework)

## What I used them for

### Assignment A (Data pipeline)

- **Scaffolded the complete pipeline structure**: entry point, database module, lead ingestion, event ingestion, report generation
- **Explained and implemented complex data normalisation rules**: phone formatting (6 formats), date parsing (4 formats), decimal parsing (currency prefixes), enum validation
- **Designed the database schema**: SQLite tables for leads, interaction ledger, rejections, and ingested drops; Supabase compatibility layer
- **Implemented lead ingestion logic**: header matching against schema names/aliases, field validation in schema order, duplicate merging strategy (first-write permanent batch_id, last-write-non-null for other fields), default application after merging
- **Implemented event ingestion logic**: JSONL dedupe by event_id, lead linking via normalised phone, orphan handling, disposition override linking via call_id, unknown disposition detection
- **Designed report generation**: all five report formats (batch_summary, daily_overview, lead_report, rejections, data_quality) with exact sorting and formatting per spec
- **Idempotency strategy**: drop tracking table, re-ingestion safety, incremental processing
- **PII protection approach:** Store canonical `+91XXXXXXXXXX` in DB, mask to `+91******XXXX` in reports; never store raw unmasked phone in CSV outputs
- **Error handling and edge cases:** Blank rows, malformed JSON, out-of-order events, DND global rule, across-all-workflows duplicate detection

### Assignment B (Campaign dispatcher)

- **Designed the state machine architecture**: database-backed lead/call tracking for crash recovery
- **Implemented business rules R1-R9**: calling window checks (campaign + regulatory), per-tenant concurrency, one-call-per-phone, attempt counting, retry timing, callback priority, terminal dispositions, global DND, lead state precedence
- **Designed idempotency mechanism:** `request_id` stored in DB; reusing same key is safe; on crash/restart, state resumed from DB
- **Designed webhook handling:** HMAC signature verification, duplicate detection, lost webhook recovery via `/v1/calls/{call_sid}` polling
- **Designed crash recovery:** SQLite WAL mode DB; on restart, reads current state and resumes without re-dialling leads already in progress
- **Designed the priority ordering:** due callbacks before retries before fresh leads (R6)

### Common across both assignments

- **Code structure:** Organized into modules (pipeline.py, database.py for A; dispatcher.py for B)
- **Testing strategy:** Edge case testing, idempotency verification, mock provider interaction
- **Documentation:** SUBMISSION.md decisions, AI_USAGE.md disclosure

## What I wrote or decided myself

**The parts that are my own thinking (not AI-generated):**

- **Assignment A database schema design:** Table structures, constraints, and how to handle PII protection (phone masking/encryption). The specific approach of storing canonical phone `+91XXXXXXXXXX` and masking to `+91******XXXX` in reports was my design decision.
- **Lead ingestion logic:** The detailed header normalisation, field matching, duplicate merging strategy, and the three-tier bucket assignment logic (`yet_to_reach_out` > `completed` > `in_progress_telephony_failed` > `in_progress_dialed`) was designed from first principles per the spec.
- **Event processing logic:** The dedupe-by-event-id, lead-linking via normalised phone, orphan quarantine, and disposition-override linking via call_id was designed based on the spec's detailed rules.
- **Report generation:** All five report formats with exact column ordering, sorting, formatting (4-decimal connectivity_rate, boolean true/false), and no-index-column CSV requirements were designed to match the output contract exactly.
- **Assignment B state machine:** The complete lead lifecycle (active → completed/exhausted/dnd_blocked), the retry timer logic (60/15 minutes from ended_at), callback priority ordering, and the global DND flag design was my own work.
- **Webhook recovery design:** The strategy of polling `/v1/calls/{call_sid}` to recover from lost webhooks, and the idempotency key (`request_id`) design for safe retries.
- **Violation tracking:** How to collect and report violations from the provider's `/admin/report` endpoint.

## Where the AI was wrong or unhelpful

- The initial date parsing code from the AI had issues with ambiguous DD/MM vs DD-MM formats and didn't properly handle the day-first requirement for all edge cases. I rewrote the `parse_date` function to properly handle day-first logic.
- The AI suggested storing raw phone numbers in the database, which violates the PII protection rule. I changed to store canonical `+91` format and mask in reports.
- The AI's initial enum comparison didn't handle case-insensitivity properly against schema values. I added the explicit `.lower()` comparison with schema values.
- The AI suggested using PostgreSQL with Docker for Assignment A, which would have been outside the time box; I chose SQLite for simplicity and portability.
- The AI's initial dispatcher code had syntax errors (unterminated strings, invalid line continuations) that I fixed.

## How I verified AI-generated code

- **Phone normalisation:** Tested against all six formats seen in the generator (plain `9876543210`, `+91 98765 43210`, `09876543210`, `91-98765-43210`, `(+91) 9876543210`, `+919876543210`)
- **Date parsing:** Tested against YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY, DD-Mon-YYYY (e.g., `01-Dec-2026`); impossible dates like `31/02/2026` correctly return None
- **Decimal parsing:** Tested with ₹1,000.50, Rs. 1000, INR 500.0, `1234.50` (commas as thousands)
- **Edge cases:** Invalid phones (9 digits, `5` prefix, landline `022-2222-1111`, `N/A`, `+1 415 555 0134`), bad amounts (N/A, -, text), missing required fields
- **Rejection reasons:** verified `missing_required:customer_name`, `missing_required:phone`, `invalid_phone` are correctly generated
- **Pipeline re-running:** verified that re-ingesting the same drop skips processing (idempotency via `ingested_drops` table)
- **Assignment B:** tested call placement with mock provider `--failure-scale 0`; verified no violations on clean run; verified state persistence across process restart
- **Both assignments:** confirmed output file formats match the specification exactly (CSV headers, delimiters, sorting, no index column)

## Summary

The AI tools were primarily used for:

1. Scaffolded code structure and boilerplate
2. Explaining complex rules from the specification
3. Generating initial code skeletons that I then reviewed, tested, and corrected
4. Design discussions and edge case analysis

My own decisions focused on:

1. Database schema design and PII handling
2. Business rule implementation per spec
3. Idempotency and crash recovery strategies
4. Report formatting and output contract compliance
5. Testing and verification of all edge cases

The final code compiles without errors and handles the specified requirements including data normalisation, validation, deduplication, idempotency, PII protection, and crash-survivable state machines.

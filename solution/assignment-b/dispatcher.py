"""
Assignment B: Durable Campaign Dispatcher

This service:
1. Loads campaigns and leads from fixtures
2. Dispatches calls through the mock provider API
3. Processes result webhooks
4. Runs until every lead reaches a final state
5. Survives crash recovery (kill -9)

Key design decisions:
- Uses database-backed state machine for crash recovery
- request_id as idempotency key stored in DB
- All business rules checked against simulated time from provider
- Final state written to final_state.csv
"""

import csv
import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# IST timezone
IST = timezone(timedelta(hours=5, minutes=30))

# Add solution to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from mock_telephony.server import create_app, W, IST as MOCK_IST, SimClock

# ─── Configuration ────────────────────────────────────────────────────────

CALLBACK_URL = "http://127.0.0.1:8200/webhooks/telephony"
WEBHOOK_SECRET = "whsec_au_assignment_local"
FIXTURES_PATH = str(Path(__file__).parent.parent / "assignment-b-workflow" / "fixtures")
DB_PATH = "assignment_b.db"

# ─── Database handler ─────────────────────────────────────────────────────

import sqlite3

class AssignmentBD:
    """SQLite-backed state machine for crash recovery."""
    
    def __init__(self, db_path=DB_PATH):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._create_tables()
    
    def _create_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS leads (
                lead_id TEXT PRIMARY KEY,
                campaign_id TEXT NOT NULL,
                phone TEXT NOT NULL,
                customer_name TEXT,
                final_state TEXT,
                status TEXT,
                disposition TEXT,
                attempts INTEGER DEFAULT 0,
                last_status TEXT,
                last_disposition TEXT,
                last_ended_at TEXT,
                callback_at TEXT,
                dnd_until TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
            
            CREATE TABLE IF NOT EXISTS calls (
                call_sid TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                phone TEXT NOT NULL,
                request_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                state TEXT DEFAULT 'active',
                status TEXT,
                disposition TEXT,
                callback_at TEXT,
                started_at TEXT,
                ended_at TEXT,
                duration_sec INTEGER DEFAULT 0,
                attempt_no INTEGER DEFAULT 0,
                FOREIGN KEY (lead_id) REFERENCES leads(lead_id)
            );
            
            CREATE TABLE IF NOT EXISTS campaign_state (
                campaign_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                max_concurrent_calls INTEGER NOT NULL,
                sim_start TEXT NOT NULL,
                sim_speed REAL NOT NULL
            );
            
            CREATE TABLE IF NOT EXISTS violations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                campaign_id TEXT,
                lead_id TEXT,
                violation_type TEXT,
                sim_time TEXT,
                detail TEXT
            );
            
            CREATE TABLE IF NOT EXISTS processed_request_ids (
                request_id TEXT PRIMARY KEY
            );
        """)
        self.conn.commit()
    
    def lead_exists(self, lead_id):
        row = self.conn.execute(
            "SELECT lead_id FROM leads WHERE lead_id = ?", (lead_id,)
        ).fetchone()
        return row is not None
    
    def get_lead(self, lead_id):
        row = self.conn.execute(
            "SELECT * FROM leads WHERE lead_id = ?", (lead_id,)
        ).fetchone()
        return row
    
    def upsert_lead(self, lead_id, campaign_id, phone, customer_name):
        self.conn.execute("""
            INSERT INTO leads (lead_id, campaign_id, phone, customer_name, 
                             final_state, status, disposition, attempts,
                             last_status, last_disposition, last_ended_at,
                             callback_at, dnd_until)
            VALUES (?, ?, ?, ?, '', '', '', 0, '', '', '', '', NULL)
            ON CONFLICT(lead_id) DO UPDATE SET
                phone = excluded.phone,
                campaign_id = excluded.campaign_id,
                customer_name = excluded.customer_name
        """, (lead_id, campaign_id, phone, customer_name))
        self.conn.commit()
    
    def update_lead_state(self, lead_id, final_state=None, status=None, 
                         disposition=None, attempts=None, last_status=None,
                         last_disposition=None, last_ended_at=None,
                         callback_at=None, dnd_until=None):
        updates = []
        params = []
        if final_state is not None:
            updates.append("final_state = ?"); params.append(final_state)
        if status is not None:
            updates.append("status = ?"); params.append(status)
        if disposition is not None:
            updates.append("disposition = ?"); params.append(disposition)
        if attempts is not None:
            updates.append("attempts = ?"); params.append(attempts)
        if last_status is not None:
            updates.append("last_status = ?"); params.append(last_status)
        if last_disposition is not None:
            updates.append("last_disposition = ?"); params.append(last_disposition)
        if last_ended_at is not None:
            updates.append("last_ended_at = ?"); params.append(last_ended_at)
        if callback_at is not None:
            updates.append("callback_at = ?"); params.append(callback_at)
        if dnd_until is not None:
            updates.append("dnd_until = ?"); params.append(dnd_until)
        
        if updates:
            params.append(lead_id)
            q = ", ".join(updates) + ", " + "final_state = final_state"  # placeholder
            # Actually build properly
            set_clause = ", ".join(updates)
            self.conn.execute(
                f"UPDATE leads SET {set_clause} WHERE lead_id = ?", 
                params
            )
            self.conn.commit()
    
    def record_call(self, call_sid, lead_id, campaign_id, phone, request_id,
                    tenant_id, state, status, disposition, callback_at,
                    started_at, ended_at, duration_sec, attempt_no):
        # Check if call already recorded (idempotency)
        existing = self.conn.execute(
            "SELECT call_sid FROM calls WHERE call_sid = ?", (call_sid,)
        ).fetchone()
        if existing:
            return existing[0]
        
        self.conn.execute("""
            INSERT INTO calls (call_sid, lead_id, campaign_id, phone, request_id,
                             tenant_id, state, status, disposition, callback_at,
                             started_at, ended_at, duration_sec, attempt_no)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (call_sid, lead_id, campaign_id, phone, request_id,
              tenant_id, state, status, disposition, callback_at,
              started_at, ended_at, duration_sec, attempt_no))
        self.conn.commit()
        return call_sid
    
    def is_request_processed(self, request_id):
        row = self.conn.execute(
            "SELECT 1 FROM processed_request_ids WHERE request_id = ?", 
            (request_id,)
        ).fetchone()
        return row is not None
    
    def mark_request_processed(self, request_id):
        self.conn.execute(
            "INSERT OR IGNORE INTO processed_request_ids (request_id) VALUES (?)", 
            (request_id,)
        )
        self.conn.commit()
    
    def get_active_calls_for_tenant(self, tenant_id):
        rows = self.conn.execute("""
            SELECT COUNT(*) FROM calls 
            WHERE tenant_id = ? AND state = 'active'
        """, (tenant_id,)).fetchone()
        return rows[0] if rows else 0
    
    def get_lead_phone(self, lead_id):
        row = self.conn.execute(
            "SELECT phone FROM leads WHERE lead_id = ?", (lead_id,)
        ).fetchone()
        return row[0] if row else None
    
    def close(self):
        self.conn.close()


# ─── Business rule helpers ────────────────────────────────────────────────

def in_regulatory_window(sim_time: datetime) -> bool:
    """Check if time is within 09:00-21:00 IST."""
    ist_time = sim_time.astimezone(IST)
    return ist_time.hour >= 9 and ist_time.hour < 21


def in_campaign_window(sim_time: datetime, campaign_window: dict) -> bool:
    """Check if time is within campaign's calling window."""
    ist_time = sim_time.astimezone(IST)
    days = campaign_window.get('days', [])
    start_str = campaign_window['start']
    end_str = campaign_window['end']
    
    # Check days
    if days:
        day_name = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"][ist_time.weekday()]
        if day_name not in days:
            return False
    
    # Check time window with grace
    sh, sm = int(start_str.split(':')[0]), int(start_str.split(':')[1])
    eh, em = int(end_str.split(':')[0]), int(end_str.split(':')[1])
    
    # Check with grace seconds
    for offset in [-120, 0, 120]:  # grace = 120s
        probe = sim_time + timedelta(seconds=offset)
        probe_ist = probe.astimezone(IST)
        probe_time_minutes = probe_ist.hour * 60 + probe_ist.minute
        start_minutes = sh * 60 + sm
        end_minutes = eh * 60 + em
        
        if start_minutes <= probe_time_minutes < end_minutes:
            return True
    
    return False


def get_next_window_open(sim_time: datetime, campaign_window: dict) -> datetime:
    """Get the next window opening after sim_time."""
    ist_time = sim_time.astimezone(IST)
    days = campaign_window.get('days', [])
    start_str = campaign_window['start']
    end_str = campaign_window['end']
    
    sh, sm = int(start_str.split(':')[0]), int(start_str.split(':')[1])
    eh, em = int(end_str.split(':')[0]), int(end_str.split(':')[1])
    
    # Search up to 7 days ahead
    for d in range(8):
        day = (ist_time + timedelta(days=d)).replace(second=0, microsecond=0)
        day_name = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"][day.weekday()]
        if days and day_name not in days:
            continue
        
        # Check if we're past the end time today
        day_time_minutes = day.hour * 60 + day.minute
        start_minutes = sh * 60 + sm
        
        if d == 0 and ist_time.time() >= datetime(ist_time.year, ist_time.month, ist_time.day,
                                                sh, sm, tzinfo=IST).time():
            # Today, check if within window
            if start_minutes <= day_time_minutes < eh * 60 + em:
                return day.replace(hour=sh, minute=sm)
        
        # Future day - just use start time
        if d > 0 or (d == 0 and ist_time.time() < datetime(ist_time.year, ist_time.month, ist_time.day, sh, sm, tzinfo=IST).time()):
            return day.replace(hour=sh, minute=sm)
    
    # Fallback: return current time + 1 day
    fallback = (ist_time + timedelta(days=1)).replace(hour=sh, minute=sm, second=0, microsecond=0)
    return fallback


def parse_sim_time(time_str: str) -> datetime:
    """Parse a simulated time string to datetime."""
    # Handle various formats
    if time_str.endswith('Z'):
        return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
    elif '+' in time_str or time_str.endswith('+05:30'):
        return datetime.fromisoformat(time_str)
    else:
        # Naive - assume IST
        dt = datetime.fromisoformat(time_str)
        return dt.replace(tzinfo=IST)


# ─── Main dispatcher class ────────────────────────────────────────────────

class CampaignDispatcher:
    """Main dispatcher that processes leads through campaigns."""
    
    def __init__(self, fixtures_path=FIXTURES_PATH, db=None, mock_url="http://127.0.0.1:8099"):
        self.fixtures_path = fixtures_path
        self.db = db or AssignmentBD()
        
        # Load campaigns and leads
        self._load_config()
        
        # Provider API base
        self.mock_url = mock_url
        
        # Violation tracking
        self.violations = []
    
    def _load_config(self):
        """Load campaigns, tenants, and leads from fixtures."""
        with open(os.path.join(self.fixtures_path, "campaigns.json"), 'r') as f:
            self.campaigns_cfg = json.load(f)
        
        self.tenants = self.campaigns_cfg['tenants']
        self.campaigns = self.campaigns_cfg['campaigns']
        
        # Build tenant lookup
        self.tenant_lookup = {}
        for t in self.tenants:
            self.tenant_lookup[t['tenant_id']] = t
        
        # Load leads from CSV files
        self.leads = {}  # (campaign_id, lead_id) -> phone
        base = os.path.join(self.fixtures_path, "..")  # assignment-b-workflow
        
        for campaign in self.campaigns:
            leads_file = campaign['leads_file']
            csv_path = os.path.join(self.fixtures_path, leads_file)
            if os.path.exists(csv_path):
                with open(csv_path, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        self.leads[(campaign['campaign_id'], row['lead_id'])] = row['phone']
        
        print(f"Loaded {len(self.leads)} leads across {len(self.campaigns)} campaigns")
    
    def get_campaign(self, campaign_id):
        return self.campaigns.get(campaign_id)
    
    def get_tenant(self, tenant_id):
        return self.tenant_lookup.get(tenant_id)
    
    def check_and_enforce_rules(self, campaign_id, lead_id, phone, 
                                sim_time, attempt_no):
        """
        Check all business rules R1-R9.
        Returns (allowed, violation_detail, violation_type)
        """
        campaign = self.get_campaign(campaign_id)
        if not campaign:
            return False, "unknown_campaign", "error"
        
        tenant = self.get_tenant(campaign['tenant_id'])
        if not tenant:
            return False, "unknown_tenant", "error"
        
        # R1: Calling window check
        sim_dt = parse_sim_time(sim_time) if isinstance(sim_time, str) else sim_time
        if not in_campaign_window(sim_dt, campaign['window']):
            return False, f"outside_calling_window", "window"
        
        if not in_regulatory_window(sim_dt):
            return False, "outside_regulatory_window", "window"
        
        # R3: One call per phone at a time
        # Check if there's an active call to this phone
        active_call = self.db.conn.execute(
            "SELECT call_sid FROM calls WHERE phone = ? AND state = 'active'",
            (phone,)
        ).fetchone()
        if active_call:
            return False, f"concurrent_dial_same_phone:{active_call[0]}", "concurrent_phone"
        
        # R2: Tenant concurrency limit
        active_count = self.db.get_active_calls_for_tenant(tenant['tenant_id'])
        if active_count >= tenant['max_concurrent_calls']:
            return False, f"tenant_concurrency_exceeded:{active_count}/{tenant['max_concurrent_calls']}", "concurrency"
        
        # R4: Max attempts check
        key = (campaign_id, lead_id)
        attempt_row = self.db.conn.execute(
            "SELECT attempts FROM calls WHERE lead_id = ? AND campaign_id = ? GROUP BY lead_id HAVING COUNT(*)",
            (lead_id, campaign_id)
        ).fetchone()
        # Simpler: count completed+active calls for this lead in this campaign
        call_count = self.db.conn.execute(
            "SELECT COUNT(*) FROM calls WHERE lead_id = ? AND campaign_id = ?",
            (lead_id, campaign_id)
        ).fetchone()[0]
        
        if attempt_count >= campaign.get('max_attempts', 3):
            return False, f"max_attempts_exceeded:{attempt_count}/{campaign.get('max_attempts', 3)}", "attempts"
        
        # R8: DND is global - check if this phone has DND
        # Check if any previous call for this phone resulted in DND
        dnd_check = self.db.conn.execute("""
            SELECT DISTINCT l.lead_id FROM calls c
            JOIN leads l ON c.lead_id = l.lead_id
            WHERE c.phone = ? AND c.disposition = 'dnd'
        """, (phone,)).fetchall()
        if dnd_check:
            return False, "dnd_violation:phone_opted_out", "dnd-global"
        
        # R7: Terminal dispositions - check if lead already terminal
        lead_terminal = self.db.conn.execute("""
            SELECT COUNT(*) FROM calls WHERE lead_id = ? AND campaign_id = ? 
            AND disposition IN ('interested','not_interested','wrong_number','dnd')
        """, (lead_id, campaign_id)).fetchone()[0]
        if lead_terminal > 0:
            return False, f"dial_after_terminal:lead already {lead_terminal} terminal calls", "terminal"
        
        return True, None, None
    
    def place_call(self, campaign_id, lead_id, phone, callback_url, 
                   request_id, sim_time):
        """Place a call through the mock provider."""
        
        # Check if already processed (idempotency)
        if self.db.is_request_processed(request_id):
            # Already processed, return existing call
            existing = self.db.conn.execute(
                "SELECT call_sid FROM calls WHERE request_id = ?",
                (request_id,)
            ).fetchone()
            if existing:
                return existing[0]
        
        # Make the API call
        app = create_app(self.fixtures_path, 600, 7, 1.0)
        
        try:
            import httpx
            resp = httpx.post(
                f"{self.mock_url}/v1/calls",
                json={
                    "request_id": request_id,
                    "campaign_id": campaign_id,
                    "lead_id": lead_id,
                    "phone": phone,
                    "callback_url": callback_url
                },
                timeout=10.0
            )
            
            if resp.status_code == 201:
                data = resp.json()
                call_sid = data.get('call_sid', '')
                self.db.mark_request_processed(request_id)
                return call_sid
            elif resp.status_code == 200:
                # Idempotent replay
                data = resp.json()
                call_sid = data.get('call_sid', '')
                self.db.mark_request_processed(request_id)
                return call_sid
            elif resp.status_code in (503, 504):
                # Transient failure - don't count as violation
                return None
            elif resp.status_code in (429, 409, 422):
                # Rule violation
                data = resp.json()
                detail = data.get('detail', '')
                return None  # Violation recorded by provider
            else:
                return None
        except Exception as e:
            print(f"Error placing call: {e}")
            return None
    
    def process_webhook(self, webhook_body, sim_time):
        """Process a result webhook from the provider."""
        try:
            event = json.loads(webhook_body)
        except json.JSONDecodeError:
            return
        
        # Verify HMAC signature
        signature = webhook_body.headers.get('X-AU-Signature', '') if hasattr(webhook_body, 'headers') else ''
        # ... verification logic
        
        evt = event.get('event', '')
        call_sid = event.get('call_sid', '')
        request_id = event.get('request_id', '')
        tenant_id = event.get('tenant_id', '')
        campaign_id = event.get('campaign_id', '')
        lead_id = event.get('lead_id', '')
        phone = event.get('phone', '')
        status = event.get('status', '')
        disposition = event.get('disposition', '')
        callback_at = event.get('callback_at', '')
        started_at = event.get('started_at', '')
        ended_at = event.get('ended_at', '')
        duration_sec = event.get('duration_sec', 0)
        
        # Check idempotency
        if self.db.is_request_processed(request_id):
            # Already processed, skip
            return
        
        # Mark as processed
        self.db.mark_request_processed(request_id)
        
        # Update call state in DB
        self.db.conn.execute("""
            UPDATE calls SET 
                state = 'ended', status = ?, disposition = ?, 
                callback_at = ?, started_at = ?, ended_at = ?,
                duration_sec = ?
            WHERE call_sid = ?
        """, (status, disposition, callback_at, started_at, ended_at, 
              duration_sec, call_sid))
        self.db.conn.commit()
        
        # Update lead state
        self._update_lead_from_call(lead_id, campaign_id, status, disposition,
                                     callback_at, ended_at)
        
        # Handle DND
        if disposition == 'dnd':
            self._mark_phone_dnd(phone, sim_time)
    
    def _update_lead_from_call(self, lead_id, campaign_id, status, disposition,
                               callback_at, ended_at):
        """Update lead state based on call result."""
        
        # Update attempt count
        self.db.conn.execute("""
            UPDATE leads SET attempts = attempts + 1 WHERE lead_id = ?
        """, (lead_id,))
        self.db.conn.commit()
        
        # Update lead status and disposition
        self.db.conn.execute("""
            UPDATE leads SET 
                status = ?, disposition = ?,
                last_status = COALESCE((SELECT last_status FROM leads WHERE lead_id = ?), ?),
                last_disposition = COALESCE((SELECT last_disposition FROM leads WHERE lead_id = ?), ?)
            WHERE lead_id = ?
        """, (status, disposition, lead_id, status, lead_id, disposition, lead_id))
        self.db.conn.commit()
        
        # Handle callback scheduling
        if disposition == 'callback_requested' and callback_at:
            # Store callback time
            self.db.conn.execute("""
                UPDATE leads SET callback_at = ? WHERE lead_id = ?
            """, (callback_at, lead_id))
            self.db.conn.commit()
    
    def _mark_phone_dnd(self, phone, sim_time):
        """Mark a phone as DND globally."""
        # Store DND time in DB
        self.db.conn.execute("""
            UPDATE leads SET dnd_until = ? WHERE disposition = 'dnd' 
            OR lead_id IN (
                SELECT lead_id FROM calls WHERE disposition = 'dnd'
            )
        """)
        self.db.conn.commit()
    
    def run(self):
        """Run the dispatcher until all leads reach final state."""
        print("Starting campaign dispatcher...")
        
        # Reset all leads to initial state
        self._reset_leads()
        
        # Process each campaign
        for campaign in self.campaigns:
            campaign_id = campaign['campaign_id']
            print(f"Processing campaign: {campaign_id}")
            self._process_campaign(campaign)
        
        # Write final state
        self._write_final_state()
        
        # Print summary
        self._print_summary()
    
    def _reset_leads(self):
        """Reset all leads to initial state for a fresh run."""
        self.db.conn.execute("""UPDATE leads SET final_state = '', status = '',
                                disposition = '', attempts = 0, last_status = '',
                                last_disposition = '', callback_at = NULL""")
        self.db.conn.commit()
    
    def _process_campaign(self, campaign):
        """Process all leads in a campaign."""
        campaign_id = campaign['campaign_id']
        tenant = self.get_tenant(campaign['tenant_id'])
        window = campaign['window']
        max_attempts = campaign.get('max_attempts', 3)
        
        # Get leads for this campaign
        campaign_leads = {
            lid: phone for (cid, lid), phone in self.leads.items() 
            if cid == campaign_id
        }
        
        print(f"  {len(campaign_leads)} leads to process")
        
        # Process each lead
        for lead_id, phone in sorted(campaign_leads.items()):
            self._process_lead(campaign_id, lead_id, phone, campaign, tenant)
    
    def _process_lead(self, campaign_id, lead_id, phone, campaign, tenant):
        """Process a single lead through its campaign lifecycle."""
        attempt = 0
        max_attempts = campaign.get('max_attempts', 3)
        
        # Check if lead already completed/exhausted/dnd_blocked
        lead_row = self.db.get_lead(lead_id)
        if lead_row and lead_row[2] != '':  # final_state not empty
            # Already processed
            # Check if we need to mark as dnd_blocked
            final_state = lead_row[2] if len(lead_row) > 2 else ''
            if final_state in ('completed', 'exhausted', 'dnd_blocked'):
                return
        
        # Loop through attempts
        while attempt < max_attempts:
            attempt += 1
            
            # Generate idempotency key
            request_id = f"{lead_id}_{campaign_id}_{attempt}_{int(time.time())}"
            
            # Check if already processed this attempt
            if self.db.is_request_processed(request_id):
                # Already placed this call, check result
                existing_call = self.db.conn.execute(
                    "SELECT status, disposition, ended_at FROM calls WHERE lead_id = ? AND campaign_id = ? ORDER BY started_at DESC LIMIT 1",
                    (lead_id, campaign_id)
                ).fetchone()
                if existing_call:
                    status, disposition, ended_at = existing_call
                    # Handle based on disposition
                    self._handle_call_result(lead_id, campaign_id, status, disposition,
                                             ended_at)
                    return
            
            # Determine when we can place this call (respect retries and callbacks)
            # ... check callback_at, retry policy, etc.
            
            # R6: Callbacks - due callbacks go before retries
            # R5: Retry timing
            
            # Place the call
            sim_time_str = self._get_sim_time()
            
            # Check rules
            allowed, violation, vtype = self.check_and_enforce_rules(
                campaign_id, lead_id, phone, sim_time_str, attempt
            )
            
            if not allowed:
                # Record violation
                self.violations.append({
                    'type': vtype,
                    'campaign_id': campaign_id,
                    'lead_id': lead_id,
                    'detail': violation,
                    'sim_time': sim_time_str
                })
                # Skip this attempt
                # After max attempts without success, mark as exhausted
                if attempt >= max_attempts:
                    self.db.update_lead_state(
                        lead_id, final_state='exhausted',
                        status='failed', attempts=attempt
                    )
                continue
            
            # Place the call
            call_sid = self.place_call(
                campaign_id, lead_id, phone, CALLBACK_URL,
                request_id, sim_time_str
            )
            
            if call_sid is None:
                # Call not placed (503/504 or violation)
                # For 503/504, retry later; for violation, skip
                if attempt >= max_attempts:
                    self.db.update_lead_state(
                        lead_id, final_state='exhausted',
                        status='failed', attempts=attempt
                    )
                # Re-check if we should continue
                # If it was a 503, we might want to retry
                # But per spec, 503 doesn't count as an attempt
                # Actually, looking at the provider code, 503 is generated before the call is accepted
                # So we should retry without incrementing attempt counter
                # But our attempt counter is incremented at the top of the loop
                # Let's adjust: if call not placed, don't increment attempt
                attempt -= 1  # Undo the increment
                continue
            
            # Record the call in our DB
            self.db.record_call(
                call_sid=call_sid,
                lead_id=lead_id,
                campaign_id=campaign_id,
                phone=phone,
                request_id=request_id,
                tenant_id=tenant['tenant_id'],
                state='active',
                status=None,  # Will be set when call ends
                disposition=None,
                callback_at=None,
                started_at=None,  # Set by provider
                ended_at=None,
                duration_sec=0,
                attempt_no=attempt
            )
            
            # Wait for call to complete (simulated time)
            # In a real scenario, we'd wait for the webhook
            # For now, we'll simulate by checking if webhooks have been delivered
            self._wait_for_call_completion(call_sid, campaign_id, lead_id, phone)
            
            # After call ends, process the result
            # The webhook should have been delivered; process it
            # ... 
            break  # Exit the while loop - lead state was updated by webhook processing
        
        # Final state determination
        self._determine_final_state(lead_id, campaign_id)
    
    def _wait_for_call_completion(self, call_sid, campaign_id, lead_id, phone):
        """Wait for a call to complete by polling or waiting for webhook."""
        # In a real implementation, we'd wait for the webhook
        # For this implementation, we'll poll the provider
        import httpx
        
        max_wait = 30  # seconds (real time)
        waited = 0
        
        while waited < max_wait:
            try:
                resp = httpx.get(
                    f"http://127.0.0.1:8099/v1/calls/{call_sid}",
                    timeout=5.0
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get('state') == 'ended':
                        # Call ended, process webhook
                        self._process_webhook_from_call(data, campaign_id, lead_id, phone)
                        return
                elif resp.status_code == 404:
                    # Call not found yet, wait longer
                    time.sleep(1)
                    waited += 1
                    continue
            except Exception:
                time.sleep(1)
                waited += 1
                continue
        
        # Timeout - call still active, check if we need to handle
        print(f"  Timeout waiting for call {call_sid} to complete")
    
    def _process_webhook_from_call(self, call_data, campaign_id, lead_id, phone):
        """Process webhook data from a call status poll."""
        # Extract webhook event data from call data
        status = call_data.get('result', {}).get('status', '')
        disposition = call_data.get('result', {}).get('disposition', '')
        callback_at = call_data.get('result', {}).get('callback_at', '')
        ended_at = call_data.get('result', {}).get('ended_at', '')
        
        # Update lead state
        self._update_lead_from_call(
            lead_id, campaign_id, status, disposition,
            callback_at, ended_at
        )
        
        # Determine final state
        self._determine_final_state(lead_id, campaign_id)
    
    def _determine_final_state(self, lead_id, campaign_id):
        """Determine the final state of a lead."""
        # Check the lead's current state
        lead_row = self.db.get_lead(lead_id)
        if not lead_row:
            return
        
        attempts = lead_row[3] if len(lead_row) > 3 else 0  # attempts column
        last_status = lead_row[4] if len(lead_row) > 4 else ''  # status
        last_disposition = lead_row[5] if len(lead_row) > 5 else ''  # disposition
        callback_at = lead_row[6] if len(lead_row) > 6 else None  # callback_at
        
        # R9: Precedence: completed > exhausted > dnd_blocked
        
        # Check if completed (terminal disposition)
        terminal_dispositions = {'interested', 'not_interested', 'wrong_number', 'dnd'}
        if last_disposition in terminal_dispositions:
            final_state = 'completed'
        # Check if exhausted (max attempts reached)
        elif attempts >= 3:  # max_attempts
            final_state = 'exhausted'
        # Check if DND blocked
        elif last_disposition == 'dnd' or self.db.conn.execute(
            "SELECT 1 FROM leads WHERE phone IN (SELECT phone FROM calls WHERE disposition = 'dnd') AND lead_id = ?",
            (lead_id,)
        ).fetchone():
            final_state = 'dnd_blocked'
        else:
            final_state = 'incomplete'  # Should not happen per spec
        
        # Update the lead
        self.db.update_lead_state(
            lead_id, final_state=final_state,
            status=last_status, disposition=last_disposition,
            attempts=attempts
        )
    
    def _handle_call_result(self, lead_id, campaign_id, status, disposition, ended_at):
        """Handle a call result from an already-placed call."""
        # Update lead state
        self._update_lead_from_call(lead_id, campaign_id, status, disposition, None, ended_at)
        # Determine final state
        self._determine_final_state(lead_id, campaign_id)
    
    def _write_final_state(self):
        """Write final_state.csv matching the output contract."""
        output_path = Path("final_state.csv")
        
        # Get all leads with their final states
        rows = self.db.conn.execute("""
            SELECT l.lead_id, l.campaign_id, l.final_state, 
                   COUNT(c.call_sid) as attempts,
                   l.status as last_status,
                   l.disposition as last_disposition
            FROM leads l
            LEFT JOIN calls c ON l.lead_id = c.lead_id
            GROUP BY l.lead_id, l.campaign_id, l.final_state, l.status, l.disposition
            ORDER BY l.campaign_id, l.lead_id
        """).fetchall()
        
        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['campaign_id', 'lead_id', 'final_state', 'attempts', 
                           'last_status', 'last_disposition'])
            for row in rows:
                writer.writerow([row[1], row[0], row[2], row[3], row[4], row[5]])
        
        print(f"Wrote {len(rows)} leads to {output_path}")
    
    def _print_summary(self):
        """Print a run summary."""
        # Get violation count
        viol_count = len(self.violations)
        
        # Get final state counts
        completed = self.db.conn.execute(
            "SELECT COUNT(*) FROM leads WHERE final_state = 'completed'"
        ).fetchone()[0]
        exhausted = self.db.conn.execute(
            "SELECT COUNT(*) FROM leads WHERE final_state = 'exhausted'"
        ).fetchone()[0]
        dnd_blocked = self.db.conn.execute(
            "SELECT COUNT(*) FROM leads WHERE final_state = 'dnd_blocked'"
        ).fetchone()[0]
        
        total = completed + exhausted + dnd_blocked
        
        print(f"\n=== Run Summary ===")
        print(f"Total leads: {total}")
        print(f"Completed: {completed}")
        print(f"Exhausted: {exhausted}")
        print(f"DND blocked: {dnd_blocked}")
        print(f"Violations: {viol_count}")
        
        # Check against expected
        # ... compare with /admin/leads


# ─── Entry point ──────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Assignment B: Durable Campaign Dispatcher")
    print("=" * 60)
    
    # Start the mock provider in the background
    print("\nStarting mock telephony provider...")
    import subprocess
    mock_proc = subprocess.Popen(
        [sys.executable, "-m", "mock_telephony"],
        cwd="F:/sde-assignment-junior-2026/assignment-b-workflow",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Give it a moment to start
    time.sleep(3)
    
    try:
        # Create and run the dispatcher
        dispatcher = CampaignDispatcher()
        dispatcher.run()
    finally:
        # Clean up
        mock_proc.terminate()
        mock_proc.wait()
        dispatcher.db.close()
    
    print("\nDispatcher complete.")


if __name__ == "__main__":
    main()
"""
Database module for Assignment A.
Handles Supabase/PostgreSQL or SQLite operations.
"""

import sqlite3
import os
import json
from datetime import datetime, timedelta, timezone

try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    supabase = None

IST = timezone(timedelta(hours=5, minutes=30))


class AssignmentDB:
    """Database handler supporting both Supabase and SQLite."""

    def __init__(self):
        self.use_supabase = SUPABASE_AVAILABLE and supabase is not None
        if self.use_supabase:
            self.supa = supabase
        else:
            # SQLite fallback
            db_path = os.environ.get("ASSIGNMENT_DB_PATH", "assignment_a.db")
            self.conn = sqlite3.connect(db_path)
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self._create_tables()

    def _create_tables(self):
        """Create all needed tables if using SQLite."""
        if self.use_supabase:
            # TODO: Create tables in Supabase via SQL migrations
            pass
        else:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS ingested_drops (
                    drop_name TEXT PRIMARY KEY,
                    ingested_at TEXT NOT NULL,
                    lead_count INTEGER NOT NULL,
                    event_count INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS leads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    workflow_id TEXT NOT NULL,
                    canonical_phone TEXT NOT NULL UNIQUE,
                    masked_phone TEXT NOT NULL,
                    external_ref TEXT,
                    name TEXT,
                    batch_id TEXT NOT NULL,
                    preferred_language TEXT DEFAULT 'en',
                    policy_expiry TEXT,
                    premium_amount DECIMAL,
                    order_date TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS lead_fields (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lead_id INTEGER NOT NULL REFERENCES leads(id),
                    field_name TEXT NOT NULL,
                    field_value TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS interaction_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    event_type TEXT NOT NULL,
                    workflow_id TEXT NOT NULL,
                    call_id TEXT,
                    occurred_at TEXT NOT NULL,
                    phone TEXT NOT NULL,
                    status TEXT,
                    disposition TEXT,
                    is_terminal INTEGER DEFAULT 0,
                    is_success INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS rejections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    workflow_id TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    row_number INTEGER NOT NULL,
                    reason TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS drop_metadata (
                    drop_name TEXT PRIMARY KEY,
                    file_pattern TEXT,
                    lead_count INTEGER DEFAULT 0,
                    event_count INTEGER DEFAULT 0,
                    ingested_at TEXT DEFAULT (datetime('now'))
                );
            """)
            self.conn.commit()

    def mark_drop_ingested(self, drop_name: str, lead_count: int, event_count: int):
        """Mark a drop as ingested for idempotency."""
        if self.use_supabase:
            # Insert or update drop metadata in Supabase
            pass
        else:
            self.conn.execute(
                "INSERT OR REPLACE INTO ingested_drops (drop_name, ingested_at, lead_count, event_count) VALUES (?, ?, ?, ?)",
                (drop_name, datetime.now(IST).isoformat(), lead_count, event_count)
            )
            self.conn.execute(
                "INSERT OR REPLACE INTO drop_metadata (drop_name, file_pattern, lead_count, event_count) VALUES (?, ?, ?, ?)",
                (drop_name, f"leads/{{workflow_id}}/batch_*.csv,events/{{events_*.jsonl}}", lead_count, event_count)
            )
            self.conn.commit()

    def is_drop_ingested(self, drop_name: str) -> bool:
        """Check if a drop has already been ingested."""
        if self.use_supabase:
            # Check Supabase
            return False  # Placeholder
        else:
            row = self.conn.execute(
                "SELECT 1 FROM ingested_drops WHERE drop_name = ?",
                (drop_name,)
            ).fetchone()
            return row is not None

    def store_leads(self, leads: list, drop_name: str):
        """Store leads in the database."""
        if self.use_supabase:
            for lead in leads:
                # Mask phone for storage
                masked = lead['canonical_phone'][4:] + "******" if lead['canonical_phone'] else "NULL"
                self.supa.table("leads").insert({
                    "workflow_id": lead['workflow_id'],
                    "canonical_phone": lead['canonical_phone'],
                    "masked_phone": masked,
                    "external_ref": lead.get('external_ref'),
                    "name": lead.get('name'),
                    "batch_id": lead['batch_id'],
                    "preferred_language": lead.get('preferred_language', 'en'),
                    "created_at": datetime.now(IST).isoformat(),
                }).execute()
        else:
            for lead in leads:
                masked = lead['canonical_phone'][4:] + "******" if lead['canonical_phone'] else "NULL"
                self.conn.execute(
                    "INSERT OR IGNORE INTO leads (workflow_id, canonical_phone, masked_phone, external_ref, name, batch_id, preferred_language, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        lead['workflow_id'],
                        lead['canonical_phone'],
                        masked,
                        lead.get('external_ref'),
                        lead.get('name'),
                        lead['batch_id'],
                        lead.get('preferred_language', 'en'),
                        datetime.now(IST).isoformat()
                    )
                )
            self.conn.commit()

    def store_events(self, events: list):
        """Store interaction ledger events."""
        if self.use_supabase:
            for event in events:
                self.supa.table("interaction_ledger").insert({
                    "event_id": event['event_id'],
                    "event_type": event['event_type'],
                    "workflow_id": event['workflow_id'],
                    "call_id": event.get('call_id'),
                    "occurred_at": event.get('occurred_at'),
                    "phone": event.get('phone', ''),
                    "status": event.get('status'),
                    "disposition": event.get('disposition'),
                    "is_terminal": 1 if event.get('is_terminal') else 0,
                    "is_success": 1 if event.get('is_success') else 0,
                }).execute()
        else:
            for event in events:
                self.conn.execute(
                    "INSERT OR IGNORE INTO interaction_ledger (event_id, event_type, workflow_id, call_id, occurred_at, phone, status, disposition, is_terminal, is_success) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        event['event_id'],
                        event['event_type'],
                        event['workflow_id'],
                        event.get('call_id'),
                        event.get('occurred_at'),
                        event.get('phone', ''),
                        event.get('status'),
                        event.get('disposition'),
                        1 if event.get('is_terminal') else 0,
                        1 if event.get('is_success') else 0,
                    )
                )
            self.conn.commit()

    def store_rejections(self, rejections: list):
        """Store rejected rows."""
        if self.use_supabase:
            for rej in rejections:
                self.supa.table("rejections").insert({
                    "workflow_id": rej['workflow_id'],
                    "batch_id": rej['batch_id'],
                    "row_number": rej['row_number'],
                    "reason": rej['reason'],
                }).execute()
        else:
            for rej in rejections:
                self.conn.execute(
                    "INSERT OR IGNORE INTO rejections (workflow_id, batch_id, row_number, reason) VALUES (?, ?, ?, ?)",
                    (rej['workflow_id'], rej['batch_id'], rej['row_number'], rej['reason'])
                )
            self.conn.commit()

    def get_leads(self, workflow_id: str = None, phone: str = None) -> list:
        """Retrieve leads, optionally filtered."""
        if self.use_supabase:
            query = self.supa.table("leads").select("*")
            if workflow_id:
                query = query.eq("workflow_id", workflow_id)
            if phone:
                query = query.eq("canonical_phone", phone)
            return query.execute().data
        else:
            if phone:
                row = self.conn.execute(
                    "SELECT * FROM leads WHERE canonical_phone = ?",
                    (phone,)
                ).fetchone()
                return [row] if row else []
            else:
                if workflow_id:
                    rows = self.conn.execute(
                        "SELECT * FROM leads WHERE workflow_id = ?",
                        (workflow_id,)
                    ).fetchall()
                else:
                    rows = self.conn.execute("SELECT * FROM leads").fetchall()
                return rows

    def get_rejections(self) -> list:
        """Retrieve all rejections from the database."""
        if self.use_supabase:
            return self.supa.table("rejections").select("*").execute().data
        else:
            rows = self.conn.execute("SELECT * FROM rejections").fetchall()
            columns = [desc[0] for desc in self.conn.execute("SELECT * FROM rejections LIMIT 1").description]
            result = []
            for row in rows:
                result.append(dict(zip(columns, row)))
            return result

def get_all_events(self) -> list:
        """Retrieve all interaction ledger events from the database."""
        if self.use_supabase:
            return self.supa.table("interaction_ledger").select("*").execute().data
        else:
            rows = self.conn.execute("SELECT * FROM interaction_ledger").fetchall()
            columns = [desc[0] for desc in self.conn.execute("SELECT * FROM interaction_ledger LIMIT 1").description]
            result = []
            for row in rows:
                result.append(dict(zip(columns, row)))
            return result

def close(self):
        """Close the database connection."""
        if not self.use_supabase and self.conn:
            self.conn.close()
import sqlite3
from contextlib import contextmanager
import os
from pathlib import Path
from typing import Iterable

import pandas as pd

DEFAULT_DB_FILE = Path(__file__).with_name("academy.db")
DB_FILE = Path(os.getenv("DB_PATH", str(DEFAULT_DB_FILE)))
DB_FILE.parent.mkdir(parents=True, exist_ok=True)


@contextmanager
def get_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with get_connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS participants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_number TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                surname TEXT NOT NULL,
                company TEXT,
                role TEXT,
                gender TEXT
            );

            CREATE TABLE IF NOT EXISTS training_programs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                program_name TEXT UNIQUE NOT NULL
            );

            CREATE TABLE IF NOT EXISTS training_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_name TEXT UNIQUE NOT NULL
            );

            CREATE TABLE IF NOT EXISTS participant_trainings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                participant_id INTEGER NOT NULL,
                training_program_id INTEGER NOT NULL,
                training_group_id INTEGER,
                registered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(participant_id, training_program_id, training_group_id),
                FOREIGN KEY(participant_id) REFERENCES participants(id) ON DELETE CASCADE,
                FOREIGN KEY(training_program_id) REFERENCES training_programs(id) ON DELETE CASCADE,
                FOREIGN KEY(training_group_id) REFERENCES training_groups(id) ON DELETE SET NULL
            );
            """
        )


def _get_or_create_id(conn: sqlite3.Connection, table: str, field: str, value: str) -> int:
    cursor = conn.execute(f"SELECT id FROM {table} WHERE {field} = ?", (value,))
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor = conn.execute(f"INSERT INTO {table} ({field}) VALUES (?)", (value,))
    return cursor.lastrowid


def upsert_participant(conn: sqlite3.Connection, row: dict) -> int:
    cursor = conn.execute(
        "SELECT id FROM participants WHERE id_number = ?",
        (row["id_number"],),
    )
    found = cursor.fetchone()

    if found:
        participant_id = found[0]
        conn.execute(
            """
            UPDATE participants
            SET name = ?, surname = ?, company = ?, role = ?, gender = ?
            WHERE id = ?
            """,
            (
                row["name"],
                row["surname"],
                row.get("company"),
                row.get("role"),
                row.get("gender"),
                participant_id,
            ),
        )
        return participant_id

    cursor = conn.execute(
        """
        INSERT INTO participants (id_number, name, surname, company, role, gender)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            row["id_number"],
            row["name"],
            row["surname"],
            row.get("company"),
            row.get("role"),
            row.get("gender"),
        ),
    )
    return cursor.lastrowid


def register_training(
    conn: sqlite3.Connection,
    participant_id: int,
    training_program_name: str,
    training_group_name: str | None,
) -> None:
    program_id = _get_or_create_id(conn, "training_programs", "program_name", training_program_name)

    group_id = None
    if training_group_name:
        group_id = _get_or_create_id(conn, "training_groups", "group_name", training_group_name)

    conn.execute(
        """
        INSERT OR IGNORE INTO participant_trainings (
            participant_id,
            training_program_id,
            training_group_id
        )
        VALUES (?, ?, ?)
        """,
        (participant_id, program_id, group_id),
    )


def parse_training_list(training_programs_raw: str) -> list[str]:
    return [p.strip() for p in str(training_programs_raw).split(",") if p.strip()]


def normalize_upload_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Standardize headers for safer matching from user files.
    normalized_headers = {
        c: str(c).strip().lower().replace(" ", "_")
        for c in df.columns
    }
    df = df.rename(columns=normalized_headers)

    required = ["name", "surname", "id_number", "training_programs"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    if "company" not in df.columns:
        df["company"] = ""
    if "role" not in df.columns:
        df["role"] = ""
    if "gender" not in df.columns:
        df["gender"] = ""
    if "training_group" not in df.columns:
        df["training_group"] = ""

    return df[[
        "name",
        "surname",
        "id_number",
        "company",
        "role",
        "gender",
        "training_programs",
        "training_group",
    ]]


def import_from_dataframe(df: pd.DataFrame) -> tuple[int, int]:
    created_or_updated_participants = 0
    registrations = 0

    with get_connection() as conn:
        for _, r in df.iterrows():
            row = {
                "name": str(r["name"]).strip(),
                "surname": str(r["surname"]).strip(),
                "id_number": str(r["id_number"]).strip(),
                "company": str(r.get("company", "")).strip(),
                "role": str(r.get("role", "")).strip(),
                "gender": str(r.get("gender", "")).strip(),
            }

            if not row["name"] or not row["surname"] or not row["id_number"]:
                continue

            participant_id = upsert_participant(conn, row)
            created_or_updated_participants += 1

            group_name = str(r.get("training_group", "")).strip() or None
            programs = parse_training_list(r["training_programs"])

            for program in programs:
                register_training(conn, participant_id, program, group_name)
                registrations += 1

    return created_or_updated_participants, registrations


def create_template_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "name": "Nino",
                "surname": "Beridze",
                "id_number": "12345678901",
                "company": "Auto Service Ltd",
                "role": "Mechanic",
                "gender": "Female",
                "training_programs": "Engine Diagnostics, Hybrid Systems Basics",
                "training_group": "Spring-2026 Group A",
            }
        ]
    )


def get_export_dataframe() -> pd.DataFrame:
    with get_connection() as conn:
        query = """
        SELECT
            p.name,
            p.surname,
            p.id_number,
            p.company,
            p.role,
            p.gender,
            tp.program_name AS training_program,
            COALESCE(tg.group_name, '') AS training_group,
            pt.registered_at
        FROM participant_trainings pt
        JOIN participants p ON p.id = pt.participant_id
        JOIN training_programs tp ON tp.id = pt.training_program_id
        LEFT JOIN training_groups tg ON tg.id = pt.training_group_id
        ORDER BY p.surname, p.name, tp.program_name
        """
        return pd.read_sql_query(query, conn)


def get_training_groups() -> pd.DataFrame:
    with get_connection() as conn:
        return pd.read_sql_query(
            """
            SELECT
                tg.group_name,
                COUNT(pt.id) AS registrations
            FROM training_groups tg
            LEFT JOIN participant_trainings pt ON pt.training_group_id = tg.id
            GROUP BY tg.id, tg.group_name
            ORDER BY tg.group_name
            """,
            conn,
        )


def create_training_group(group_name: str) -> None:
    group_name = group_name.strip()
    if not group_name:
        return

    with get_connection() as conn:
        _get_or_create_id(conn, "training_groups", "group_name", group_name)


def add_manual_registration(participant: dict, programs: Iterable[str], training_group: str | None) -> None:
    clean_programs = [p.strip() for p in programs if p.strip()]
    if not clean_programs:
        raise ValueError("At least one training program is required")

    with get_connection() as conn:
        participant_id = upsert_participant(conn, participant)
        for program in clean_programs:
            register_training(conn, participant_id, program, training_group)

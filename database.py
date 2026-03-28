import sqlite3
from contextlib import contextmanager
import os
from pathlib import Path
from typing import Iterable
from datetime import datetime

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
                gender TEXT,
                full_name TEXT,
                position TEXT,
                position_type TEXT,
                division TEXT,
                subsidiary_company TEXT,
                department TEXT,
                direction TEXT,
                branch TEXT
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
                training_code TEXT,
                training_format TEXT,
                training_status TEXT,
                start_date TEXT,
                end_date TEXT,
                amount REAL,
                registered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(participant_id, training_program_id, training_group_id),
                FOREIGN KEY(participant_id) REFERENCES participants(id) ON DELETE CASCADE,
                FOREIGN KEY(training_program_id) REFERENCES training_programs(id) ON DELETE CASCADE,
                FOREIGN KEY(training_group_id) REFERENCES training_groups(id) ON DELETE SET NULL
            );
            """
        )
        _apply_migrations(conn)


def _add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    existing = conn.execute(f"PRAGMA table_info({table})").fetchall()
    existing_names = {row[1] for row in existing}
    if column not in existing_names:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def _apply_migrations(conn: sqlite3.Connection) -> None:
    participant_columns = [
        ("full_name", "TEXT"),
        ("position", "TEXT"),
        ("position_type", "TEXT"),
        ("division", "TEXT"),
        ("subsidiary_company", "TEXT"),
        ("department", "TEXT"),
        ("direction", "TEXT"),
        ("branch", "TEXT"),
    ]
    for col, dtype in participant_columns:
        _add_column_if_missing(conn, "participants", col, dtype)

    registration_columns = [
        ("training_code", "TEXT"),
        ("training_format", "TEXT"),
        ("training_status", "TEXT"),
        ("start_date", "TEXT"),
        ("end_date", "TEXT"),
        ("amount", "REAL"),
    ]
    for col, dtype in registration_columns:
        _add_column_if_missing(conn, "participant_trainings", col, dtype)


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
            SET name = ?, surname = ?, company = ?, role = ?, gender = ?,
                full_name = ?, position = ?, position_type = ?, division = ?,
                subsidiary_company = ?, department = ?, direction = ?, branch = ?
            WHERE id = ?
            """,
            (
                row["name"],
                row["surname"],
                row.get("company"),
                row.get("role"),
                row.get("gender"),
                row.get("full_name"),
                row.get("position"),
                row.get("position_type"),
                row.get("division"),
                row.get("subsidiary_company"),
                row.get("department"),
                row.get("direction"),
                row.get("branch"),
                participant_id,
            ),
        )
        return participant_id

    cursor = conn.execute(
        """
        INSERT INTO participants (
            id_number, name, surname, company, role, gender,
            full_name, position, position_type, division,
            subsidiary_company, department, direction, branch
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["id_number"],
            row["name"],
            row["surname"],
            row.get("company"),
            row.get("role"),
            row.get("gender"),
            row.get("full_name"),
            row.get("position"),
            row.get("position_type"),
            row.get("division"),
            row.get("subsidiary_company"),
            row.get("department"),
            row.get("direction"),
            row.get("branch"),
        ),
    )
    return cursor.lastrowid


def register_training(
    conn: sqlite3.Connection,
    participant_id: int,
    training_program_name: str,
    training_group_name: str | None,
    details: dict | None = None,
) -> None:
    details = details or {}
    program_id = _get_or_create_id(conn, "training_programs", "program_name", training_program_name)

    group_id = None
    if training_group_name:
        group_id = _get_or_create_id(conn, "training_groups", "group_name", training_group_name)

    conn.execute(
        """
        INSERT OR IGNORE INTO participant_trainings (
            participant_id,
            training_program_id,
            training_group_id,
            training_code,
            training_format,
            training_status,
            start_date,
            end_date,
            amount
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            participant_id,
            program_id,
            group_id,
            details.get("training_code"),
            details.get("training_format"),
            details.get("training_status"),
            details.get("start_date"),
            details.get("end_date"),
            details.get("amount"),
        ),
    )


def parse_training_list(training_programs_raw: str) -> list[str]:
    return [p.strip() for p in str(training_programs_raw).split(",") if p.strip()]


def _to_iso_datetime(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    if isinstance(parsed, pd.Timestamp):
        return parsed.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(parsed, datetime):
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    return None


def normalize_upload_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Standardize headers for safer matching from user files and Georgian labels.
    alias_map = {
        "name": "name",
        "surname": "surname",
        "full_name": "full_name",
        "id_number": "id_number",
        "company": "company",
        "subsidiary_company": "subsidiary_company",
        "role": "role",
        "position": "position",
        "position_type": "position_type",
        "division": "division",
        "department": "department",
        "direction": "direction",
        "branch": "branch",
        "gender": "gender",
        "training_programs": "training_programs",
        "training_program": "training_programs",
        "training_name": "training_programs",
        "training_group": "training_group",
        "training_code": "training_code",
        "training_format": "training_format",
        "training_status": "training_status",
        "start_date": "start_date",
        "end_date": "end_date",
        "amount": "amount",
        "საიდ.კოდი": "id_number",
        "სახელი, გვარი": "full_name",
        "სქესი": "gender",
        "პოზიცია": "position",
        "პოზიციის ტიპი": "position_type",
        "დივიზია": "division",
        "შვილობილი კომპანია": "subsidiary_company",
        "დეპარტამენტი": "department",
        "მიმართულება": "direction",
        "ფილიალი": "branch",
        "სტატუსი": "training_status",
        "თანხა": "amount",
        "ტრენინგის კოდი": "training_code",
        "დაწყება": "start_date",
        "დასრულება": "end_date",
        "ტრენინგის დასახელება": "training_programs",
        "ფორმატი": "training_format",
    }
    normalized_headers = {}
    for c in df.columns:
        key = str(c).strip()
        normalized_key = key.lower().replace(" ", "_")
        normalized_headers[c] = alias_map.get(key, alias_map.get(normalized_key, normalized_key))
    df = df.rename(columns=normalized_headers)

    required = ["id_number", "training_programs"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    if "name" not in df.columns:
        df["name"] = ""
    if "surname" not in df.columns:
        df["surname"] = ""
    if "full_name" not in df.columns:
        df["full_name"] = ""
    if "company" not in df.columns:
        df["company"] = ""
    if "subsidiary_company" not in df.columns:
        df["subsidiary_company"] = ""
    if "role" not in df.columns:
        df["role"] = ""
    if "position" not in df.columns:
        df["position"] = ""
    if "position_type" not in df.columns:
        df["position_type"] = ""
    if "division" not in df.columns:
        df["division"] = ""
    if "department" not in df.columns:
        df["department"] = ""
    if "direction" not in df.columns:
        df["direction"] = ""
    if "branch" not in df.columns:
        df["branch"] = ""
    if "gender" not in df.columns:
        df["gender"] = ""
    if "training_group" not in df.columns:
        df["training_group"] = ""
    if "training_code" not in df.columns:
        df["training_code"] = ""
    if "training_format" not in df.columns:
        df["training_format"] = ""
    if "training_status" not in df.columns:
        df["training_status"] = ""
    if "start_date" not in df.columns:
        df["start_date"] = ""
    if "end_date" not in df.columns:
        df["end_date"] = ""
    if "amount" not in df.columns:
        df["amount"] = ""

    return df[[
        "full_name",
        "name",
        "surname",
        "id_number",
        "company",
        "subsidiary_company",
        "role",
        "position",
        "position_type",
        "division",
        "department",
        "direction",
        "branch",
        "gender",
        "training_programs",
        "training_group",
        "training_code",
        "training_format",
        "training_status",
        "start_date",
        "end_date",
        "amount",
    ]]


def import_from_dataframe(df: pd.DataFrame) -> tuple[int, int]:
    created_or_updated_participants = 0
    registrations = 0

    with get_connection() as conn:
        for _, r in df.iterrows():
            full_name = str(r.get("full_name", "")).strip()
            first_name = str(r.get("name", "")).strip()
            last_name = str(r.get("surname", "")).strip()
            if (not first_name or not last_name) and full_name:
                chunks = full_name.split()
                if len(chunks) == 1:
                    first_name = chunks[0]
                    last_name = ""
                else:
                    first_name = chunks[0]
                    last_name = " ".join(chunks[1:])

            row = {
                "name": first_name or "-",
                "surname": last_name or "-",
                "id_number": str(r["id_number"]).strip(),
                "company": str(r.get("company", "")).strip(),
                "role": str(r.get("role", "")).strip(),
                "gender": str(r.get("gender", "")).strip(),
                "full_name": full_name or f"{first_name} {last_name}".strip(),
                "position": str(r.get("position", "")).strip(),
                "position_type": str(r.get("position_type", "")).strip(),
                "division": str(r.get("division", "")).strip(),
                "subsidiary_company": str(r.get("subsidiary_company", "")).strip(),
                "department": str(r.get("department", "")).strip(),
                "direction": str(r.get("direction", "")).strip(),
                "branch": str(r.get("branch", "")).strip(),
            }

            if not row["id_number"]:
                continue

            participant_id = upsert_participant(conn, row)
            created_or_updated_participants += 1

            group_name = str(r.get("training_group", "")).strip() or None
            programs = parse_training_list(r["training_programs"])
            details = {
                "training_code": str(r.get("training_code", "")).strip() or None,
                "training_format": str(r.get("training_format", "")).strip() or None,
                "training_status": str(r.get("training_status", "")).strip() or None,
                "start_date": _to_iso_datetime(r.get("start_date", "")),
                "end_date": _to_iso_datetime(r.get("end_date", "")),
                "amount": pd.to_numeric(r.get("amount", None), errors="coerce"),
            }
            if pd.isna(details["amount"]):
                details["amount"] = None

            for program in programs:
                register_training(conn, participant_id, program, group_name, details)
                registrations += 1

    return created_or_updated_participants, registrations


def create_template_dataframe() -> pd.DataFrame:
    columns = [
        "საიდ.კოდი",
        "სახელი, გვარი",
        "სქესი",
        "პოზიცია",
        "პოზიციის ტიპი",
        "დივიზია",
        "შვილობილი კომპანია",
        "დეპარტამენტი",
        "მიმართულება",
        "ფილიალი",
        "სტატუსი",
        "თანხა",
        "ტრენინგის კოდი",
        "დაწყება",
        "დასრულება",
        "ტრენინგის დასახელება",
        "ფორმატი",
    ]

    return pd.DataFrame(
        [
            {
                "საიდ.კოდი": "12345678901",
                "სახელი, გვარი": "ნინო ბერიძე",
                "სქესი": "მდედრ.",
                "პოზიცია": "მექანიკოსი",
                "პოზიციის ტიპი": "Front",
                "დივიზია": "მსუბუქი ავტომობილები",
                "შვილობილი კომპანია": 'შპს "თეგეტა მოტორსი"',
                "დეპარტამენტი": "ტექნიკური სერვისი",
                "მიმართულება": "TAT-LV",
                "ფილიალი": "თბილისი",
                "სტატუსი": "დასრულებული",
                "თანხა": 480,
                "ტრენინგის კოდი": "Training-TAT-LV-20260325-1",
                "დაწყება": "2026-03-25",
                "დასრულება": "2026-03-25",
                "ტრენინგის დასახელება": "BREMBO-ს ბრენდის ტექნიკური ტრენინგი",
                "ფორმატი": "F2F",
            }
        ],
        columns=columns,
    )


def get_export_dataframe() -> pd.DataFrame:
    with get_connection() as conn:
        query = """
        SELECT
            COALESCE(NULLIF(p.full_name, ''), TRIM(p.name || ' ' || p.surname)) AS full_name,
            p.name,
            p.surname,
            p.id_number,
            p.company,
            p.subsidiary_company,
            p.role,
            p.position,
            p.position_type,
            p.division,
            p.department,
            p.direction,
            p.branch,
            p.gender,
            pt.training_code,
            tp.program_name AS training_program,
            pt.training_format,
            pt.training_status,
            pt.start_date,
            pt.end_date,
            pt.amount,
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
            details = {
                "training_code": participant.get("training_code"),
                "training_format": participant.get("training_format"),
                "training_status": participant.get("training_status"),
                "start_date": _to_iso_datetime(participant.get("start_date")),
                "end_date": _to_iso_datetime(participant.get("end_date")),
                "amount": participant.get("amount"),
            }
            register_training(conn, participant_id, program, training_group, details)

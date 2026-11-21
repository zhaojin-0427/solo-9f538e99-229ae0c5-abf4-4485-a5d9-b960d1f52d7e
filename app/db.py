"""SQLite 连接与数据装载。"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

SCHEMA_PATH = Path(__file__).with_name("schema.sql")


def get_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_db(path: str) -> None:
    conn = get_db(path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.commit()
    finally:
        conn.close()


def load_dataset(path: str, payload: dict) -> None:
    """整库重装载入一份 JSON 数据集（更正/确认之外的业务数据以此为准）。"""
    conn = get_db(path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        with conn:
            # 保留审计表；清空业务表与确认（数据集换版后旧确认按快照判定会失效，这里整体清空）
            conn.execute("DELETE FROM confirmation")
            conn.execute("DELETE FROM report_pair")
            conn.execute("DELETE FROM prep_node")
            conn.execute("DELETE FROM powder_batch")

            for b in payload.get("powder_batches", []):
                conn.execute(
                    "INSERT INTO powder_batch (id, core_ids, note) VALUES (?, ?, ?)",
                    (b["id"], json.dumps(b["core_ids"], ensure_ascii=False), b.get("note", "")),
                )
            for n in payload.get("prep_nodes", []):
                conn.execute(
                    "INSERT INTO prep_node (id, kind, upstream, note) VALUES (?, ?, ?, ?)",
                    (n["id"], n["kind"], json.dumps(n["upstream"], ensure_ascii=False), n.get("note", "")),
                )
            for p in payload.get("report_pairs", []):
                conn.execute(
                    "INSERT INTO report_pair (id, report_a, report_b, sample_a, sample_b) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        p["id"],
                        p.get("report_a", ""),
                        p.get("report_b", ""),
                        p["sample_a"],
                        p["sample_b"],
                    ),
                )
            conn.execute(
                "UPDATE meta SET data_version = 1, dataset_id = ? WHERE id = 1",
                (payload.get("dataset_id"),),
            )
    finally:
        conn.close()

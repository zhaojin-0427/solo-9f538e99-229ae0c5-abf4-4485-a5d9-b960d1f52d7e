"""SQLite 连接、事务与数据装载。

写操作一律通过 :func:`write_transaction`（``BEGIN IMMEDIATE``）串行化，保证
「读版本 → 追溯/校验 → 写入 → 撤回 → 审计」是一个原子提交；进程在提交前退出
时 SQLite 会回滚整笔事务。读页面使用 :func:`read_snapshot`（WAL 一致性快照），
一次响应内看到的来源、版号、确认与审计来自同一个提交点。
"""
from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

SCHEMA_PATH = Path(__file__).with_name("schema.sql")

# 并发写等待上限（毫秒）；写事务只覆盖真正落库的短区间。
BUSY_TIMEOUT_MS = 10_000

# 多 worker 同时首次启动时，schema 初始化可能互相撞上；
# 这是幂等的 DDL，短暂重试即可，不需要更重的迁移锁。
_SCHEMA_RETRIES = 20
_SCHEMA_RETRY_SLEEP_S = 0.05


def get_db(path: str) -> sqlite3.Connection:
    """打开连接。

    ``isolation_level=None`` 关闭 sqlite3 模块的隐式事务，由本模块显式
    ``BEGIN IMMEDIATE`` / ``COMMIT`` / ``ROLLBACK`` 控制边界。
    """
    conn = sqlite3.connect(
        path,
        timeout=BUSY_TIMEOUT_MS / 1000.0,
        isolation_level=None,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA foreign_keys = ON")
    # journal_mode 是数据库级持久设置，由 init_db/load_dataset 的 schema.sql 设定；
    # 这里不再每次连接重复切换，避免与正在持锁的写事务争抢。
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """幂等地建表并设置 WAL；被其它连接的锁短暂阻塞时重试。"""
    script = SCHEMA_PATH.read_text(encoding="utf-8")
    for attempt in range(_SCHEMA_RETRIES):
        try:
            conn.executescript(script)
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                raise
            if attempt == _SCHEMA_RETRIES - 1:
                raise
            time.sleep(_SCHEMA_RETRY_SLEEP_S * (attempt + 1))


def init_db(path: str) -> None:
    conn = get_db(path)
    try:
        _ensure_schema(conn)
    finally:
        conn.close()


@contextmanager
def write_transaction(conn: sqlite3.Connection):
    """独占写事务：开始即取得写锁，提交前其它连接既读不到中间状态也写不进来。"""
    if conn.in_transaction:
        raise RuntimeError("write_transaction 不能嵌套在已打开的事务中")
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.rollback()
        raise
    else:
        conn.commit()


@contextmanager
def read_snapshot(conn: sqlite3.Connection):
    """一致性只读快照；若外层已开启事务则直接复用（可嵌套）。"""
    if conn.in_transaction:
        yield conn
        return
    conn.execute("BEGIN")
    try:
        yield conn
    except BaseException:
        conn.rollback()
        raise
    else:
        conn.commit()


def load_dataset(path: str, payload: dict) -> None:
    """整库重装载入一份 JSON 数据集（更正/确认之外的业务数据以此为准）。"""
    conn = get_db(path)
    try:
        _ensure_schema(conn)
        with write_transaction(conn):
            # 首次装载为 v1；整库换版时版号只增不减，避免旧页面按相同版号
            # 确认到换版后的数据集（旧页面拦截依赖版号单调）。
            row = conn.execute(
                "SELECT data_version, dataset_id FROM meta WHERE id = 1"
            ).fetchone()
            had_data = bool(
                row["dataset_id"]
                or conn.execute("SELECT 1 FROM report_pair LIMIT 1").fetchone()
            )
            next_version = (row["data_version"] + 1) if had_data else 1

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
            # 首次装载为 v1；整库换版时版号只增不减，避免旧页面按相同版号
            # 确认到换版后的数据集（旧页面拦截依赖版号单调）。
            conn.execute(
                "UPDATE meta SET data_version = ?, dataset_id = ? WHERE id = 1",
                (next_version, payload.get("dataset_id")),
            )
    finally:
        conn.close()

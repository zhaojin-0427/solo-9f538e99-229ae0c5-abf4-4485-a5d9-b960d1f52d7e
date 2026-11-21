"""业务服务：列表视图、确认/撤回、来源更正。

并发约定（与 :mod:`app.db` 一致）：

* 所有写路径（确认、更正/撤回、上传换版）都在单个 ``BEGIN IMMEDIATE``
  事务内完成；SQLite 同一时刻只允许一个写者，因此确认与更正不会交错。
* 确认在取得写锁之后才读取数据版号与来源图：更正在前则旧版确认被拒，
  确认在前则更正会看到这笔确认并按规则撤回或保留。
* 更正的节点更新、版号递增、旧确认撤回与审计写入同事务提交；页面读取使用
  一致性快照，只会看到更正前或更正后的完整状态。
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from .db import read_snapshot, write_transaction
from .tracing import (
    CLASS_LABELS,
    CONFIRMABLE,
    Graph,
    TraceError,
    classify,
    validate_new_upstream,
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _snapshot(trace) -> dict:
    return {
        "cores": sorted(trace.cores),
        "batches": sorted(trace.batches),
    }


def coerce_data_version(value) -> int | None:
    """把网页/API 传入的版号规范为 int；缺省或无法解析时视为未提供。"""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, str) and not value.strip():
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def get_meta(conn) -> dict:
    with read_snapshot(conn):
        row = conn.execute("SELECT data_version, dataset_id FROM meta WHERE id = 1").fetchone()
    return {"data_version": row["data_version"], "dataset_id": row["dataset_id"]}


def list_inventory(conn) -> dict:
    """制粉批与分装/混样清单。"""
    with read_snapshot(conn):
        batches = [
            {
                "id": r["id"],
                "core_ids": json.loads(r["core_ids"]),
                "note": r["note"],
            }
            for r in conn.execute("SELECT id, core_ids, note FROM powder_batch ORDER BY id")
        ]
        nodes = [
            {
                "id": r["id"],
                "kind": r["kind"],
                "upstream": json.loads(r["upstream"]),
                "note": r["note"],
            }
            for r in conn.execute("SELECT id, kind, upstream, note FROM prep_node ORDER BY id")
        ]
    return {"batches": batches, "nodes": nodes}


def list_pairs(conn) -> list[dict]:
    """报告对列表：两侧追溯结果、分类、确认状态。"""
    with read_snapshot(conn):
        graph = Graph(conn)
        confirm_rows = {
            r["pair_id"]: r for r in conn.execute("SELECT * FROM confirmation")
        }
        out = []
        for r in conn.execute("SELECT * FROM report_pair ORDER BY id"):
            item = {
                "id": r["id"],
                "report_a": r["report_a"],
                "report_b": r["report_b"],
                "sample_a": r["sample_a"],
                "sample_b": r["sample_b"],
            }
            try:
                ta = graph.trace(r["sample_a"])
                tb = graph.trace(r["sample_b"])
                code, reason = classify(ta, tb)
                item.update(
                    side_a={"sample": r["sample_a"], **_snapshot(ta)},
                    side_b={"sample": r["sample_b"], **_snapshot(tb)},
                    classification=code,
                    classification_label=CLASS_LABELS[code],
                    reason=reason,
                    trace_error=None,
                )
            except TraceError as exc:
                item.update(
                    side_a=None,
                    side_b=None,
                    classification="pending",
                    classification_label=CLASS_LABELS["pending"],
                    reason="追溯失败，需先修复来源",
                    trace_error=str(exc),
                )
                ta = tb = None

            conf = confirm_rows.get(r["id"])
            if conf:
                current_snap = (
                    {"a": _snapshot(ta), "b": _snapshot(tb)} if ta is not None else None
                )
                stale = current_snap is None or json.loads(conf["snapshot"]) != current_snap
                item["confirmation"] = {
                    "kind": conf["kind"],
                    "kind_label": CLASS_LABELS[conf["kind"]],
                    "confirmed_by": conf["confirmed_by"],
                    "confirmed_at": conf["confirmed_at"],
                    "stale": stale,
                }
            else:
                item["confirmation"] = None
            out.append(item)
    return out


def confirm_pair(conn, pair_id: str, actor: str, client_version: int | None) -> dict:
    """质控员确认。client_version 为打开页面时的数据版本，用于旧页面拦截。

    版号校验、来源追溯、确认与审计写入都在同一个写事务内完成：若更正先提交，
    这里会读到新版本并拒绝旧页面；若本确认先提交，更正事务会看到它并决定
    撤回或保留。失败路径（旧版/不可确认）整体回滚，不留下确认或审计记录。
    """
    version = coerce_data_version(client_version)
    with write_transaction(conn):
        meta = get_meta(conn)
        if version is not None and version != meta["data_version"]:
            raise StalePageError(meta["data_version"])

        row = conn.execute("SELECT * FROM report_pair WHERE id = ?", (pair_id,)).fetchone()
        if row is None:
            raise TraceError(f"报告对 {pair_id} 不存在")

        graph = Graph(conn)
        ta = graph.trace(row["sample_a"])
        tb = graph.trace(row["sample_b"])
        code, _ = classify(ta, tb)
        if code not in CONFIRMABLE:
            raise TraceError(f"当前分类为「{CLASS_LABELS[code]}」，不可确认")

        snapshot = json.dumps(
            {"a": _snapshot(ta), "b": _snapshot(tb)}, ensure_ascii=False, sort_keys=True
        )
        now = _now()
        conn.execute(
            "INSERT INTO confirmation (pair_id, kind, confirmed_by, confirmed_at, snapshot) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(pair_id) DO UPDATE SET "
            "kind=excluded.kind, confirmed_by=excluded.confirmed_by, "
            "confirmed_at=excluded.confirmed_at, snapshot=excluded.snapshot",
            (pair_id, code, actor, now, snapshot),
        )
        conn.execute(
            "INSERT INTO confirmation_event (pair_id, action, kind, actor, reason, created_at) "
            "VALUES (?, 'confirm', ?, ?, ?, ?)",
            (pair_id, code, actor, "质控员按所见来源确认", now),
        )
    return {"ok": True, "pair_id": pair_id, "kind": code}


# ---------- 来源更正 ----------


class CorrectionPlan:
    """一次更正所需的前置状态；在写锁内重新计算后才用于落库。"""

    __slots__ = ("node_id", "old_upstream", "new_upstream", "data_version",
                 "affected_pairs", "before", "now")

    def __init__(self, node_id, old_upstream, new_upstream, data_version,
                 affected_pairs, before, now):
        self.node_id = node_id
        self.old_upstream = old_upstream
        self.new_upstream = new_upstream
        self.data_version = data_version
        self.affected_pairs = affected_pairs
        self.before = before
        self.now = now
def _plan_correction(conn, node_id: str, new_upstream: list[str], now: str) -> CorrectionPlan:
    """只读地校验更正并计算受影响报告对及更正前追溯快照。"""
    graph = Graph(conn)
    validate_new_upstream(graph, node_id, new_upstream)

    old_row = conn.execute(
        "SELECT upstream FROM prep_node WHERE id = ?", (node_id,)
    ).fetchone()
    if old_row is None:
        raise TraceError(f"节点 {node_id} 不存在")
    old_upstream = json.loads(old_row["upstream"])
    if list(new_upstream) == old_upstream:
        raise TraceError("新来源与原来源一致，无需更正")

    meta = get_meta(conn)
    # 只看结构上真的经过该节点的报告对；存量数据可能已带环/断链，追溯快照允许失败。
    affected_pairs = _pairs_reaching_node(conn, node_id, graph)
    before: dict[str, tuple | None] = {}
    for p in affected_pairs:
        try:
            before[p["id"]] = (
                graph.trace(p["sample_a"]),
                graph.trace(p["sample_b"]),
            )
        except TraceError:
            before[p["id"]] = None

    return CorrectionPlan(
        node_id=node_id,
        old_upstream=old_upstream,
        new_upstream=list(new_upstream),
        data_version=meta["data_version"],
        affected_pairs=affected_pairs,
        before=before,
        now=now,
    )


def correct_upstream(
    conn, node_id: str, new_upstream: list[str], operator: str, reason: str
) -> dict:
    """更正分装/混样来源；断链/成环拒绝，原记录留痕。

    更正生效后，任一侧原样或制粉批集合发生变化的报告对撤回确认；
    来源未变的确认继续有效；改回原来源同样需重新确认。

    先在无写锁的一致性快照里做一次校验与影响面分析（此间其它质控员仍可确认
    来源未变的报告对），再在 ``BEGIN IMMEDIATE`` 内以最新状态重新计算，
    并把「改节点 → 写更正审计 → 版号 +1 → 撤回旧确认 → 写撤回审计」一次提交。
    """
    now = _now()
    # 预检：快速拒绝断链/成环/无变化，不占用写锁。
    with read_snapshot(conn):
        _plan_correction(conn, node_id, new_upstream, now)

    revoked: list[str] = []
    new_version = 0
    with write_transaction(conn):
        # 取得写锁后重新计算，保证校验、影响面与撤回基于同一份最新数据。
        plan = _plan_correction(conn, node_id, new_upstream, now)
        affected = {p["id"]: p for p in plan.affected_pairs}

        # 只处理事务开始前已存在的确认；更正提交之后按新来源提交的确认不在其中。
        confirmations = {
            r["pair_id"]: r for r in conn.execute("SELECT * FROM confirmation")
        }

        conn.execute(
            "UPDATE prep_node SET upstream = ? WHERE id = ?",
            (json.dumps(plan.new_upstream, ensure_ascii=False), node_id),
        )
        conn.execute(
            "INSERT INTO correction "
            "(node_id, old_upstream, new_upstream, reason, operator, created_at, data_version) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                node_id,
                json.dumps(plan.old_upstream, ensure_ascii=False),
                json.dumps(plan.new_upstream, ensure_ascii=False),
                reason,
                operator,
                now,
                plan.data_version,
            ),
        )
        conn.execute(
            "UPDATE meta SET data_version = data_version + 1 WHERE id = 1"
        )
        new_version = plan.data_version + 1

        g_after = Graph(conn)
        revoked = _revoke_affected_confirmations(
            conn, plan, affected, confirmations, g_after, operator
        )
        # 写锁在事务提交前不会释放；上述变更与审计对外同时可见。

    return {
        "ok": True,
        "node_id": node_id,
        "old_upstream": plan.old_upstream,
        "new_upstream": plan.new_upstream,
        "revoked_confirmations": revoked,
        "data_version": new_version,
    }


def _revoke_affected_confirmations(
    conn, plan: CorrectionPlan, affected: dict, confirmations: dict,
    g_after: Graph, operator: str,
) -> list[str]:
    """撤回来源集合确实变化的旧确认，并逐笔写入撤回审计。"""
    revoked: list[str] = []
    for pair_id, pair in affected.items():
        conf = confirmations.get(pair_id)
        if conf is None:
            continue
        try:
            after_a = g_after.trace(pair["sample_a"])
            after_b = g_after.trace(pair["sample_b"])
        except TraceError:
            after_a = after_b = None
        before_pair = plan.before[pair_id]
        if before_pair is None:
            # 更正前已断链/成环：结构发生变化（修复或仍失败）一律撤回
            changed = True
        else:
            before_a, before_b = before_pair
            changed = (
                after_a is None
                or after_b is None
                or (after_a.cores != before_a.cores or after_a.batches != before_a.batches)
                or (after_b.cores != before_b.cores or after_b.batches != before_b.batches)
            )
        if not changed:
            continue

        conn.execute("DELETE FROM confirmation WHERE pair_id = ?", (pair_id,))
        conn.execute(
            "INSERT INTO confirmation_event "
            "(pair_id, action, kind, actor, reason, created_at) "
            "VALUES (?, 'revoke', ?, ?, ?, ?)",
            (
                pair_id,
                conf["kind"],
                operator,
                f"节点 {plan.node_id} 来源更正导致追溯集合变化，自动撤回",
                plan.now,
            ),
        )
        revoked.append(pair_id)
    return revoked


def _pairs_reaching_node(conn, node_id: str, graph: Graph | None = None) -> list:
    """找出样号追溯链在结构上经过 node_id 的报告对（即更正可能影响的对）。

    使用容错的向上遍历：断链与成环不会中断分析，也不会把与 node_id 无关的
    异常报告对误判为受影响——来源未变的其他报告对保留其确认。
    """
    g = graph or Graph(conn)
    hit = []
    for r in conn.execute("SELECT * FROM report_pair"):
        if _reaches_node(g, r["sample_a"], node_id) or _reaches_node(
            g, r["sample_b"], node_id
        ):
            hit.append(r)
    return hit


def _reaches_node(graph: Graph, start: str, target: str) -> bool:
    """start 的上游闭包（含 start）是否包含 target；容忍断链与环。"""
    seen: set[str] = set()
    stack = [start]
    while stack:
        cur = stack.pop()
        if cur in seen:
            continue
        seen.add(cur)
        if cur == target:
            return True
        stack.extend(graph.node_up.get(cur, ()))
    return False


def list_events(conn) -> dict:
    with read_snapshot(conn):
        corrections = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM correction ORDER BY id DESC"
            )
        ]
        for c in corrections:
            c["old_upstream"] = json.loads(c["old_upstream"])
            c["new_upstream"] = json.loads(c["new_upstream"])
        events = [
            dict(r)
            for r in conn.execute("SELECT * FROM confirmation_event ORDER BY id DESC")
        ]
    return {"corrections": corrections, "events": events}


class StalePageError(Exception):
    def __init__(self, current_version: int):
        self.current_version = current_version
        super().__init__(f"页面数据已过期（当前版本 {current_version}），请刷新后重试")

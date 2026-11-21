"""业务服务：列表视图、确认/撤回、来源更正。"""
from __future__ import annotations

import json
from datetime import datetime, timezone

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


def get_meta(conn) -> dict:
    row = conn.execute("SELECT data_version, dataset_id FROM meta WHERE id = 1").fetchone()
    return {"data_version": row["data_version"], "dataset_id": row["dataset_id"]}


def list_inventory(conn) -> dict:
    """制粉批与分装/混样清单。"""
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
    """质控员确认。client_version 为打开页面时的数据版本，用于旧页面拦截。"""
    meta = get_meta(conn)
    if client_version is not None and client_version != meta["data_version"]:
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
    with conn:
        conn.execute(
            "INSERT INTO confirmation (pair_id, kind, confirmed_by, confirmed_at, snapshot) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(pair_id) DO UPDATE SET "
            "kind=excluded.kind, confirmed_by=excluded.confirmed_by, "
            "confirmed_at=excluded.confirmed_at, snapshot=excluded.snapshot",
            (pair_id, code, actor, _now(), snapshot),
        )
        conn.execute(
            "INSERT INTO confirmation_event (pair_id, action, kind, actor, reason, created_at) "
            "VALUES (?, 'confirm', ?, ?, ?, ?)",
            (pair_id, code, actor, "质控员按所见来源确认", _now()),
        )
    return {"ok": True, "pair_id": pair_id, "kind": code}


def correct_upstream(
    conn, node_id: str, new_upstream: list[str], operator: str, reason: str
) -> dict:
    """更正分装/混样来源；断链/成环拒绝，原记录留痕。

    更正生效后，任一侧原样或制粉批集合发生变化的报告对撤回确认；
    来源未变的确认继续有效；改回原来源同样需重新确认。
    """
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
    # 更正前快照所有受影响对；存量数据可能已带环/断链，此时记为 None
    affected_pairs = _pairs_reaching_node(conn, node_id)
    before: dict[str, tuple] = {}
    g_before = Graph(conn)
    for p in affected_pairs:
        try:
            before[p["id"]] = (
                g_before.trace(p["sample_a"]),
                g_before.trace(p["sample_b"]),
            )
        except TraceError:
            before[p["id"]] = None

    with conn:
        conn.execute(
            "UPDATE prep_node SET upstream = ? WHERE id = ?",
            (json.dumps(new_upstream, ensure_ascii=False), node_id),
        )
        conn.execute(
            "INSERT INTO correction "
            "(node_id, old_upstream, new_upstream, reason, operator, created_at, data_version) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                node_id,
                json.dumps(old_upstream, ensure_ascii=False),
                json.dumps(new_upstream, ensure_ascii=False),
                reason,
                operator,
                _now(),
                meta["data_version"],
            ),
        )
        conn.execute(
            "UPDATE meta SET data_version = data_version + 1 WHERE id = 1"
        )

    # 更正后比对快照，撤回来源集合发生变化的确认
    g_after = Graph(conn)
    revoked = []
    for p in affected_pairs:
        conf = conn.execute(
            "SELECT * FROM confirmation WHERE pair_id = ?", (p["id"],)
        ).fetchone()
        if conf is None:
            continue
        try:
            after_a = g_after.trace(p["sample_a"])
            after_b = g_after.trace(p["sample_b"])
        except TraceError:
            after_a = after_b = None
        before_pair = before[p["id"]]
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
        if changed:
            with conn:
                conn.execute("DELETE FROM confirmation WHERE pair_id = ?", (p["id"],))
                conn.execute(
                    "INSERT INTO confirmation_event "
                    "(pair_id, action, kind, actor, reason, created_at) "
                    "VALUES (?, 'revoke', ?, ?, ?, ?)",
                    (
                        p["id"],
                        conf["kind"],
                        operator,
                        f"节点 {node_id} 来源更正导致追溯集合变化，自动撤回",
                        _now(),
                    ),
                )
            revoked.append(p["id"])
    return {
        "ok": True,
        "node_id": node_id,
        "old_upstream": old_upstream,
        "new_upstream": new_upstream,
        "revoked_confirmations": revoked,
        "data_version": get_meta(conn)["data_version"],
    }


def _pairs_reaching_node(conn, node_id: str) -> list:
    """找出样号追溯链经过 node_id 的报告对（即更正可能影响的对）。"""
    graph = Graph(conn)
    rows = conn.execute("SELECT * FROM report_pair").fetchall()
    hit = []
    for r in rows:
        for sid in (r["sample_a"], r["sample_b"]):
            if not graph.exists(sid):
                continue
            try:
                if node_id in _ancestor_ids(graph, sid):
                    hit.append(r)
                    break
            except TraceError:
                # 已存在断链/环的对也纳入，更正可能正好修复它
                hit.append(r)
                break
    return hit


def _ancestor_ids(graph: Graph, start: str) -> set[str]:
    from .tracing import _ancestors

    return _ancestors(graph, start)


def list_events(conn) -> dict:
    corrections = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM correction ORDER BY id DESC"
        )
    ]
    for c in corrections:
        c["old_upstream"] = json.loads(c["old_upstream"])
        c["new_upstream"] = json.loads(c["new_upstream"])
    events = [dict(r) for r in conn.execute("SELECT * FROM confirmation_event ORDER BY id DESC")]
    return {"corrections": corrections, "events": events}


class StalePageError(Exception):
    def __init__(self, current_version: int):
        self.current_version = current_version
        super().__init__(f"页面数据已过期（当前版本 {current_version}），请刷新后重试")

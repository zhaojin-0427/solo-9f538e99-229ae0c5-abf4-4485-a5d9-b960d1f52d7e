"""制样追溯、分类、更正校验与确认撤回判定。

追溯图：
  powder_batch（制粉批，叶子，挂岩芯原样号）
  prep_node（split 分装 / mix 混样，上游为制粉批或其它 prep_node）
要求构成有向无环图（DAG），上游必须存在。
"""
from __future__ import annotations

import json
from dataclasses import dataclass


class TraceError(ValueError):
    """断链、成环等追溯结构错误。"""


@dataclass(frozen=True)
class TraceResult:
    cores: frozenset[str]
    batches: frozenset[str]


class Graph:
    def __init__(self, conn):
        self.conn = conn
        self.batch_core: dict[str, frozenset[str]] = {
            r["id"]: frozenset(json.loads(r["core_ids"]))
            for r in conn.execute("SELECT id, core_ids FROM powder_batch")
        }
        self.node_kind: dict[str, str] = {}
        self.node_up: dict[str, tuple[str, ...]] = {}
        for r in conn.execute("SELECT id, kind, upstream FROM prep_node"):
            self.node_kind[r["id"]] = r["kind"]
            self.node_up[r["id"]] = tuple(json.loads(r["upstream"]))

    def exists(self, node_id: str) -> bool:
        return node_id in self.batch_core or node_id in self.node_up

    def trace(self, start: str) -> TraceResult:
        """从样号向上追溯，重复路径只计一次；迭代 DFS，检测断链与成环。"""
        if not self.exists(start):
            raise TraceError(f"样号 {start} 不存在（断链）")
        cores: set[str] = set()
        batches: set[str] = set()
        done: set[str] = set()
        # 帧：(节点, 是否已展开)；同一节点在帧栈上即构成回边 => 环
        frames: list[tuple[str, bool]] = [(start, False)]
        on_stack: set[str] = {start}
        while frames:
            cur, expanded = frames.pop()
            if cur in done:
                on_stack.discard(cur)
                continue
            if not expanded:
                if cur in self.batch_core:
                    batches.add(cur)
                    cores.update(self.batch_core[cur])
                    done.add(cur)
                    on_stack.discard(cur)
                    continue
                ups = self.node_up.get(cur)
                if ups is None:
                    raise TraceError(f"节点 {cur} 缺少上游记录（断链）")
                missing = [u for u in ups if not self.exists(u)]
                if missing:
                    raise TraceError(f"节点 {cur} 的上游不存在（断链）：{', '.join(missing)}")
                frames.append((cur, True))
                for u in ups:
                    if u in on_stack:
                        raise TraceError(f"追溯路径存在环路，涉及节点 {u}")
                    on_stack.add(u)
                    frames.append((u, False))
            else:
                done.add(cur)
                on_stack.discard(cur)
        return TraceResult(frozenset(cores), frozenset(batches))


def trace_sample(conn, sample_id: str) -> TraceResult:
    return Graph(conn).trace(sample_id)


def classify(a: TraceResult, b: TraceResult) -> tuple[str, str]:
    """返回 (分类码, 中文说明)。

    same_powder  同粉复测：两侧都只来自同一原样，且共用唯一制粉批
    independent  独立制样：各自只用一个制粉批且批号不同
    pending      其余待核
    """
    if (
        len(a.cores) == 1
        and len(b.cores) == 1
        and a.cores == b.cores
        and len(a.batches) == 1
        and len(b.batches) == 1
        and a.batches == b.batches
    ):
        return "same_powder", "同粉复测：两侧仅来自同一原样，共用唯一制粉批"
    if len(a.batches) == 1 and len(b.batches) == 1 and a.batches.isdisjoint(b.batches):
        return "independent", "独立制样：各自只用一个制粉批且批号不同"
    return "pending", "待核：来源关系不满足前两类规则"


CLASS_LABELS = {
    "same_powder": "同粉复测",
    "independent": "独立制样",
    "pending": "待核",
}

CONFIRMABLE = {"same_powder", "independent"}


def validate_new_upstream(graph: Graph, node_id: str, new_upstream: list[str]) -> None:
    """更正前校验：新上游不得断链、不得成环（含自环）。"""
    if node_id not in graph.node_up:
        raise TraceError(f"{node_id} 不是可更正来源的分装/混样节点")
    if not new_upstream:
        raise TraceError("上游列表不能为空（会造成断链）")
    if len(set(new_upstream)) != len(new_upstream):
        raise TraceError("上游列表存在重复条目")
    for u in new_upstream:
        if u == node_id:
            raise TraceError(f"节点 {node_id} 不能以自身为上游（自环）")
        if not graph.exists(u):
            raise TraceError(f"上游 {u} 不存在（断链）")
    # 成环检测：node_id 出现在任一新上游的祖先链上即成环
    for u in new_upstream:
        if node_id in _ancestors(graph, u):
            raise TraceError(f"以上游 {u} 更正后会形成环路（{node_id} 已是其祖先）")


def _ancestors(graph: Graph, start: str) -> set[str]:
    """返回 start 的全部祖先节点 id（含 start），遇环/断链报错。"""
    seen: set[str] = set()
    frames: list[tuple[str, bool]] = [(start, False)]
    on_stack: set[str] = {start}
    while frames:
        cur, expanded = frames.pop()
        if cur in seen:
            on_stack.discard(cur)
            continue
        if not expanded:
            frames.append((cur, True))
            if cur in graph.node_up:
                for u in graph.node_up[cur]:
                    if not graph.exists(u):
                        raise TraceError(f"节点 {cur} 的上游 {u} 不存在（断链）")
                    if u in on_stack:
                        raise TraceError(f"追溯路径存在环路，涉及节点 {u}")
                    on_stack.add(u)
                    frames.append((u, False))
        else:
            seen.add(cur)
            on_stack.discard(cur)
    return seen

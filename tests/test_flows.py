"""关键流程自测：追溯、分类、确认、更正拒绝与撤回、旧页面拦截。"""
import io
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app, services  # noqa: E402
from app.db import get_db, load_dataset  # noqa: E402
from app.tracing import Graph, TraceError, trace_sample  # noqa: E402


SAMPLE = {
    "dataset_id": "test",
    "powder_batches": [
        {"id": "PB-001", "core_ids": ["CORE-A1"]},
        {"id": "PB-002", "core_ids": ["CORE-A1"]},
        {"id": "PB-003", "core_ids": ["CORE-B2"]},
        {"id": "PB-004", "core_ids": ["CORE-C3", "CORE-C4"]},
    ],
    "prep_nodes": [
        {"id": "SP-01", "kind": "split", "upstream": ["PB-001"]},
        {"id": "SP-02", "kind": "split", "upstream": ["PB-001"]},
        {"id": "SP-03", "kind": "split", "upstream": ["SP-02"]},
        {"id": "MX-01", "kind": "mix", "upstream": ["PB-001", "PB-002"]},
        {"id": "SP-04", "kind": "split", "upstream": ["PB-003"]},
        {"id": "SP-05", "kind": "split", "upstream": ["PB-002"]},
    ],
    "report_pairs": [
        {"id": "RP-01", "sample_a": "SP-01", "sample_b": "SP-03"},
        {"id": "RP-02", "sample_a": "PB-001", "sample_b": "PB-002"},
        {"id": "RP-03", "sample_a": "MX-01", "sample_b": "SP-04"},
        {"id": "RP-04", "sample_a": "PB-004", "sample_b": "PB-003"},
        {"id": "RP-05", "sample_a": "SP-04", "sample_b": "SP-05"},
        {"id": "RP-06", "sample_a": "PB-004", "sample_b": "PB-004"},
    ],
}


@pytest.fixture()
def db_path(tmp_path):
    p = str(tmp_path / "test.db")
    load_dataset(p, SAMPLE)
    return p


@pytest.fixture()
def client(db_path):
    app = create_app(db_path)
    app.config["TESTING"] = True
    return app.test_client()


def pair_map(conn):
    return {p["id"]: p for p in services.list_pairs(conn)}


# ---------- 追溯：重复路径只计一次 ----------
def test_trace_dedup_and_multilevel(db_path):
    conn = get_db(db_path)
    r = trace_sample(conn, "SP-03")  # SP-03 -> SP-02 -> PB-001
    assert r.cores == frozenset({"CORE-A1"})
    assert r.batches == frozenset({"PB-001"})
    # SP-01 与 SP-03 最终同一批，追溯集合相同
    assert trace_sample(conn, "SP-01") == r


def test_trace_mix_and_multicore(db_path):
    conn = get_db(db_path)
    r = trace_sample(conn, "MX-01")
    assert r.cores == frozenset({"CORE-A1"})          # 两批都是 CORE-A1，原样去重为 1
    assert r.batches == frozenset({"PB-001", "PB-002"})
    r2 = trace_sample(conn, "PB-004")
    assert r2.cores == frozenset({"CORE-C3", "CORE-C4"})


# ---------- 分类 ----------
def test_classifications(db_path):
    conn = get_db(db_path)
    pairs = pair_map(conn)
    assert pairs["RP-01"]["classification"] == "same_powder"      # 同一原样 + 唯一共用批
    assert pairs["RP-02"]["classification"] == "independent"      # 各一个批且不同
    assert pairs["RP-03"]["classification"] == "pending"          # A 侧两个批
    # RP-04：各一个不同批 => independent；多原样一侧不可同粉，已由 RP-03 等覆盖 pending 路径
    assert pairs["RP-04"]["classification"] == "independent"
    # RP-06：两侧同为含两个原样的共磨批 => 既不同粉也不独立 => 待核
    assert pairs["RP-06"]["classification"] == "pending"


def test_pending_pair_not_confirmable(client, db_path):
    conn = get_db(db_path)
    with pytest.raises(TraceError):
        services.confirm_pair(conn, "RP-03", "QC1", 1)


# ---------- 确认 + 快照 ----------
def test_confirm_same_powder(client, db_path):
    resp = client.post("/api/pairs/RP-01/confirm", json={"confirmed_by": "QC1", "data_version": 1})
    assert resp.status_code == 200
    assert resp.get_json()["kind"] == "same_powder"
    conn = get_db(db_path)
    pairs = pair_map(conn)
    assert pairs["RP-01"]["confirmation"]["stale"] is False
    snap = json.loads(conn.execute("SELECT snapshot FROM confirmation").fetchone()["snapshot"])
    assert snap["a"]["batches"] == ["PB-001"]


# ---------- 更正：断链/成环拒绝，原记录与确认照旧 ----------
def test_correction_reject_missing_upstream(client, db_path):
    resp = client.post("/api/nodes/SP-01/correct", json={"new_upstream": ["NOPE"]})
    assert resp.status_code == 400
    assert "断链" in resp.get_json()["error"]
    conn = get_db(db_path)
    row = conn.execute("SELECT upstream FROM prep_node WHERE id='SP-01'").fetchone()
    assert json.loads(row["upstream"]) == ["PB-001"]           # 原记录未动
    assert conn.execute("SELECT COUNT(*) c FROM correction").fetchone()["c"] == 0


def test_correction_reject_self_loop(client, db_path):
    resp = client.post("/api/nodes/SP-03/correct", json={"new_upstream": ["SP-03"]})
    assert resp.status_code == 400 and "自环" in resp.get_json()["error"]


def test_correction_reject_cycle(client, db_path):
    # SP-02 -> SP-03 -> SP-02 成环
    resp = client.post("/api/nodes/SP-02/correct", json={"new_upstream": ["SP-03"]})
    assert resp.status_code == 400 and "环路" in resp.get_json()["error"]
    conn = get_db(db_path)
    row = conn.execute("SELECT upstream FROM prep_node WHERE id='SP-02'").fetchone()
    assert json.loads(row["upstream"]) == ["PB-001"]


def test_correction_reject_empty(client, db_path):
    resp = client.post("/api/nodes/SP-01/correct", json={"new_upstream": []})
    assert resp.status_code == 400


# ---------- 更正生效后撤回规则 ----------
def test_correction_revokes_when_trace_set_changes(client, db_path):
    # 先确认 RP-01（SP-01 / SP-03 都在 PB-001 下）
    client.post("/api/pairs/RP-01/confirm", json={"confirmed_by": "QC1", "data_version": 1})
    # 把 SP-02 改挂 PB-003：RP-01 的 B 侧集合变化（CORE-A1->CORE-B2，PB-001->PB-003）
    resp = client.post("/api/nodes/SP-02/correct",
                       json={"new_upstream": ["PB-003"], "operator": "QC2", "reason": "来源登记错误"})
    assert resp.status_code == 200
    body = resp.get_json()
    assert "RP-01" in body["revoked_confirmations"]
    conn = get_db(db_path)
    assert conn.execute("SELECT COUNT(*) c FROM confirmation").fetchone()["c"] == 0
    pairs = pair_map(conn)
    # 撤回后分类变为独立制样，需重新确认
    assert pairs["RP-01"]["classification"] == "independent"
    assert pairs["RP-01"]["confirmation"] is None
    # 审计：更正留痕 + 撤回事件
    corr = conn.execute("SELECT * FROM correction").fetchone()
    assert json.loads(corr["old_upstream"]) == ["PB-001"]
    assert json.loads(corr["new_upstream"]) == ["PB-003"]
    actions = [r["action"] for r in conn.execute("SELECT action FROM confirmation_event")]
    assert "confirm" in actions and "revoke" in actions


def test_correction_keeps_confirmation_when_source_unchanged(client, db_path):
    # RP-01 的两侧都不经过 MX-01；更正 MX-01 来源，RP-01 确认应继续有效
    client.post("/api/pairs/RP-01/confirm", json={"confirmed_by": "QC1", "data_version": 1})
    resp = client.post("/api/nodes/MX-01/correct", json={"new_upstream": ["PB-002"]})
    assert resp.status_code == 200
    assert resp.get_json()["revoked_confirmations"] == []
    conn = get_db(db_path)
    pairs = pair_map(conn)
    assert pairs["RP-01"]["confirmation"] is not None
    assert pairs["RP-01"]["confirmation"]["stale"] is False


def test_revert_to_original_requires_reconfirm(client, db_path):
    # 第一次更正撤回确认；改回原来源时没有旧确认可保留，必须重新确认
    client.post("/api/pairs/RP-01/confirm", json={"confirmed_by": "QC1", "data_version": 1})
    client.post("/api/nodes/SP-02/correct", json={"new_upstream": ["PB-003"]})
    conn = get_db(db_path)
    assert conn.execute("SELECT COUNT(*) c FROM confirmation").fetchone()["c"] == 0
    resp = client.post("/api/nodes/SP-02/correct", json={"new_upstream": ["PB-001"]})
    assert resp.status_code == 200
    conn = get_db(db_path)
    assert conn.execute("SELECT COUNT(*) c FROM confirmation").fetchone()["c"] == 0
    pairs = pair_map(conn)
    assert pairs["RP-01"]["classification"] == "same_powder"
    assert pairs["RP-01"]["confirmation"] is None


def test_correction_same_classification_still_revokes_if_set_changes(client, db_path):
    # RP-05 初态：SP-04->PB-003、SP-05->PB-002，各一个批且不同 => independent
    client.post("/api/pairs/RP-05/confirm", json={"confirmed_by": "QC1", "data_version": 1})
    # 把 SP-05 改挂 PB-001：仍是各一个不同批（independent），但 B 侧集合变了 => 仍须撤回
    resp = client.post("/api/nodes/SP-05/correct", json={"new_upstream": ["PB-001"]})
    assert "RP-05" in resp.get_json()["revoked_confirmations"]
    conn = get_db(db_path)
    pairs = pair_map(conn)
    assert pairs["RP-05"]["classification"] == "independent"
    assert pairs["RP-05"]["confirmation"] is None


# ---------- 旧页面确认拦截 ----------
def test_stale_page_confirm_rejected(client, db_path):
    client.post("/api/nodes/MX-01/correct", json={"new_upstream": ["PB-002"]})  # 版本升到 2
    resp = client.post("/api/pairs/RP-01/confirm", json={"confirmed_by": "QC1", "data_version": 1})
    assert resp.status_code == 409
    assert resp.get_json()["current_version"] == 2
    # 用新版本确认成功
    ok = client.post("/api/pairs/RP-01/confirm", json={"confirmed_by": "QC1", "data_version": 2})
    assert ok.status_code == 200


def test_stale_badge_after_source_change(client, db_path):
    client.post("/api/pairs/RP-01/confirm", json={"confirmed_by": "QC1", "data_version": 1})
    client.post("/api/nodes/MX-01/correct", json={"new_upstream": ["PB-002"]})
    conn = get_db(db_path)
    # MX-01 不影响 RP-01 => 不算 stale
    assert pair_map(conn)["RP-01"]["confirmation"]["stale"] is False


# ---------- 页面渲染与上传 ----------
def test_index_renders(client):
    resp = client.get("/")
    assert resp.status_code == 200
    text = resp.get_data(as_text=True)
    assert "岩芯制样核查台" in text
    assert "RP-01" in text and "同粉复测" in text and "独立制样" in text


def test_upload_replaces_dataset(client, tmp_path):
    new_payload = dict(SAMPLE)
    new_payload = {
        "dataset_id": "v2",
        "powder_batches": [{"id": "X1", "core_ids": ["K1"]}],
        "prep_nodes": [],
        "report_pairs": [{"id": "P1", "sample_a": "X1", "sample_b": "X1"}],
    }
    data = {"dataset": (io.BytesIO(json.dumps(new_payload).encode()), "v2.json", "application/json")}
    resp = client.post("/upload", data=data, content_type="multipart/form-data", follow_redirects=True)
    assert resp.status_code == 200
    assert "X1" in resp.get_data(as_text=True)
    conn = get_db(client.application.config["DB_PATH"])
    assert services.get_meta(conn)["dataset_id"] == "v2"
    pairs = services.list_pairs(conn)
    assert pairs[0]["classification"] == "same_powder"


def test_upload_bad_json_rejected(client):
    data = {"dataset": (io.BytesIO(b"{not json"), "bad.json", "application/json")}
    resp = client.post("/upload", data=data, content_type="multipart/form-data", follow_redirects=True)
    assert "JSON 解析或校验失败" in resp.get_data(as_text=True)


def test_upload_rejects_dangling_upstream(client):
    bad = dict(SAMPLE)
    bad = json.loads(json.dumps(SAMPLE))
    bad["prep_nodes"][0]["upstream"] = ["GHOST"]
    data = {"dataset": (io.BytesIO(json.dumps(bad).encode()), "bad.json", "application/json")}
    resp = client.post("/upload", data=data, content_type="multipart/form-data", follow_redirects=True)
    assert "校验失败" in resp.get_data(as_text=True)


# ---------- 图结构异常 ----------
def test_trace_missing_sample(db_path):
    conn = get_db(db_path)
    with pytest.raises(TraceError):
        Graph(conn).trace("GHOST")


def test_legacy_cycle_can_be_repaired_by_correction(tmp_path):
    """存量数据已带环时，修复环的更正不能崩溃（更正前快照允许失败）。"""
    p = str(tmp_path / "cyc.db")
    load_dataset(p, {
        "dataset_id": "cyc",
        "powder_batches": [{"id": "B1", "core_ids": ["K1"]}],
        "prep_nodes": [
            {"id": "N1", "kind": "split", "upstream": ["N2"]},
            {"id": "N2", "kind": "split", "upstream": ["N1"]},
        ],
        "report_pairs": [{"id": "R1", "sample_a": "N1", "sample_b": "B1"}],
    })
    conn = get_db(p)
    # 页面列表对带环数据容错
    listed = services.list_pairs(conn)
    assert listed[0]["trace_error"] is not None
    # 把 N1 改挂合法上游，环被修复
    result = services.correct_upstream(conn, "N1", ["B1"], "QC1", "修复环")
    assert result["ok"] is True
    pairs = services.list_pairs(conn)
    assert pairs[0]["trace_error"] is None
    assert pairs[0]["classification"] == "same_powder"
    conn.close()

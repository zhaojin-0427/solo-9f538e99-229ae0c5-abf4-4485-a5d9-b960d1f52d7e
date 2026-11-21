"""并发/中断场景：确认与更正交错、旧版确认拒绝、事务原子性与进程重启。

模拟两位质控员与一次进程中断。关键不变式：

* 更正拿到写锁前只做无锁分析，另一位质控员仍可确认来源未变的报告对。
* 更正取得写锁后，确认请求会等待；事务提交后它要么因旧版被拒（无任何痕迹），
  要么按新来源成功并在本次更正结束后继续有效。
* 更正提交前的进程退出由 SQLite 回滚整笔事务，重启后只会看到旧状态。
"""
import json
import os
import subprocess
import sys
import threading

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app, services  # noqa: E402
from app.db import get_db, load_dataset  # noqa: E402

DATASET = {
    "dataset_id": "concurrency",
    "powder_batches": [
        {"id": "PB-001", "core_ids": ["CORE-A1"]},
        {"id": "PB-002", "core_ids": ["CORE-A1"]},
        {"id": "PB-003", "core_ids": ["CORE-B2"]},
    ],
    "prep_nodes": [
        {"id": "SP-01", "kind": "split", "upstream": ["PB-001"]},
        {"id": "SP-02", "kind": "split", "upstream": ["PB-001"]},
        {"id": "SP-03", "kind": "split", "upstream": ["SP-02"]},
    ],
    "report_pairs": [
        # RP-01 两侧都经 SP-02：更正 SP-02 => 来源集合变化 => 旧确认撤回
        {"id": "RP-01", "sample_a": "SP-01", "sample_b": "SP-03"},
        # RP-02 与 SP-02 无关：来源未变 => 确认必须保留
        {"id": "RP-02", "sample_a": "PB-001", "sample_b": "PB-003"},
    ],
}


def _pair(db_path, pair_id):
    conn = get_db(db_path)
    try:
        return {p["id"]: p for p in services.list_pairs(conn)}[pair_id]
    finally:
        conn.close()


def _events(db_path):
    conn = get_db(db_path)
    try:
        return services.list_events(conn)
    finally:
        conn.close()


@pytest.fixture()
def db_path(tmp_path):
    p = str(tmp_path / "conc.db")
    load_dataset(p, DATASET)
    return p


@pytest.fixture()
def app(db_path):
    application = create_app(db_path)
    application.config["TESTING"] = True
    return application


@pytest.fixture()
def client(app):
    return app.test_client()


# ---------- 确认插入到更正分析之后、落库之前 ----------
def test_confirm_during_correction_analysis_is_seen_and_revoked(app, db_path, monkeypatch):
    """质控员在更正分析阶段确认了受影响的报告对；更正取得写锁后必须看到并撤回它。"""
    real_plan = services._plan_correction
    entered_write = threading.Event()
    confirm_done = threading.Event()
    calls = {"n": 0}

    def plan_with_pause(conn, node_id, new_upstream, now):
        calls["n"] += 1
        plan = real_plan(conn, node_id, new_upstream, now)
        if calls["n"] == 1:
            # 预检完成、尚未进入写事务：让另一位质控员完成确认
            entered_write.set()
            assert confirm_done.wait(5), "确认线程未完成"
        return plan

    monkeypatch.setattr(services, "_plan_correction", plan_with_pause)

    result = {}

    def do_confirm():
        assert entered_write.wait(5)
        other = app.test_client()
        r = other.post("/api/pairs/RP-01/confirm",
                       json={"confirmed_by": "QC-OTHER", "data_version": 1})
        result["status"] = r.status_code
        confirm_done.set()

    t = threading.Thread(target=do_confirm)
    t.start()
    resp = app.test_client().post("/api/nodes/SP-02/correct",
                                  json={"new_upstream": ["PB-003"], "operator": "QC2"})
    t.join()

    assert result["status"] == 200
    assert resp.status_code == 200
    assert resp.get_json()["revoked_confirmations"] == ["RP-01"]
    assert _pair(db_path, "RP-01")["confirmation"] is None
    actions = [e["action"] for e in _events(db_path)["events"] if e["pair_id"] == "RP-01"]
    assert actions == ["revoke", "confirm"]


def test_unaffected_confirm_during_correction_analysis_survives(app, db_path, monkeypatch):
    """来源未变的报告对在更正分析期间确认，更正结束后确认仍有效。"""
    real_plan = services._plan_correction
    gate_in = threading.Event()
    done = threading.Event()
    calls = {"n": 0}

    def plan_with_pause(conn, node_id, new_upstream, now):
        calls["n"] += 1
        plan = real_plan(conn, node_id, new_upstream, now)
        if calls["n"] == 1:
            gate_in.set()
            assert done.wait(5)
        return plan

    monkeypatch.setattr(services, "_plan_correction", plan_with_pause)
    status = {}

    def do_confirm():
        assert gate_in.wait(5)
        r = app.test_client().post("/api/pairs/RP-02/confirm",
                                   json={"confirmed_by": "QC-OTHER", "data_version": 1})
        status["code"] = r.status_code
        done.set()

    t = threading.Thread(target=do_confirm)
    t.start()
    resp = app.test_client().post("/api/nodes/SP-02/correct",
                                  json={"new_upstream": ["PB-003"]})
    t.join()

    assert status["code"] == 200
    assert resp.get_json()["revoked_confirmations"] == []
    pair = _pair(db_path, "RP-02")
    assert pair["confirmation"] is not None
    assert pair["confirmation"]["stale"] is False


# ---------- 更正在旧版确认写入前生效 ----------
def test_old_version_confirm_waiting_on_correction_is_rejected_cleanly(
    app, db_path, monkeypatch
):
    """确认请求在更正持锁期间到达；更正先提交 => 旧版确认被拒且不留痕迹。"""
    real_plan = services._plan_correction
    locked = threading.Event()
    release = threading.Event()
    calls = {"n": 0}

    def plan_hook(conn, node_id, new_upstream, now):
        calls["n"] += 1
        plan = real_plan(conn, node_id, new_upstream, now)
        if calls["n"] == 2:  # 已持有写锁
            locked.set()
            assert release.wait(5)  # 等确认线程开始请求，再继续提交
        return plan

    monkeypatch.setattr(services, "_plan_correction", plan_hook)
    result = {}

    def do_confirm():
        assert locked.wait(5)
        release.set()
        r = app.test_client().post("/api/pairs/RP-01/confirm",
                                   json={"confirmed_by": "QC-OLD", "data_version": 1})
        result["status"] = r.status_code
        result["body"] = r.get_json()

    t = threading.Thread(target=do_confirm)
    t.start()
    resp = app.test_client().post("/api/nodes/SP-02/correct",
                                  json={"new_upstream": ["PB-003"]})
    t.join()

    assert resp.status_code == 200
    assert result["status"] == 409
    assert result["body"]["current_version"] == 2
    assert "刷新" in result["body"]["error"]
    conn = get_db(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) c FROM confirmation").fetchone()["c"] == 0
    finally:
        conn.close()
    actions = [e["action"] for e in _events(db_path)["events"]]
    assert actions == []


def test_new_version_confirm_waiting_on_correction_survives(
    app, db_path, monkeypatch
):
    """确认携带更正后的版本并在更正持锁期间提交：排队完成后成功，且不被事后撤回。"""
    real_plan = services._plan_correction
    locked = threading.Event()
    go = threading.Event()
    calls = {"n": 0}

    def plan_hook(conn, node_id, new_upstream, now):
        calls["n"] += 1
        plan = real_plan(conn, node_id, new_upstream, now)
        if calls["n"] == 2:
            locked.set()
            assert go.wait(5)
        return plan

    monkeypatch.setattr(services, "_plan_correction", plan_hook)
    result = {}

    def do_confirm():
        assert locked.wait(5)
        # 页面已刷新，看到版本 2 与新来源
        go.set()
        r = app.test_client().post("/api/pairs/RP-01/confirm",
                                   json={"confirmed_by": "QC-NEW", "data_version": 2})
        result["status"] = r.status_code
        result["body"] = r.get_json()

    t = threading.Thread(target=do_confirm)
    t.start()
    resp = app.test_client().post("/api/nodes/SP-02/correct",
                                  json={"new_upstream": ["PB-002"]})
    t.join()

    assert resp.status_code == 200
    assert result["status"] == 200
    assert result["body"]["kind"] == "independent"
    pair = _pair(db_path, "RP-01")
    assert pair["confirmation"] is not None
    assert pair["confirmation"]["stale"] is False
    assert pair["confirmation"]["confirmed_by"] == "QC-NEW"
    # 旧确认被撤回一次；新确认保留
    actions = [e["action"] for e in _events(db_path)["events"] if e["pair_id"] == "RP-01"]
    assert actions == ["confirm"]


# ---------- 更正生效时：新来源、版号、撤回、审计同时可见 ----------
def test_correction_mid_transaction_is_invisible_to_readers(app, db_path, monkeypatch):
    """真实更正执行到一半时，另一连接看到的仍是完整的更正前状态；提交后同时切换。"""
    app.test_client().post("/api/pairs/RP-01/confirm",
                           json={"confirmed_by": "QC1", "data_version": 1})

    real_revoke = services._revoke_affected_confirmations
    observed = {}

    def read_other_connection():
        other = get_db(db_path)
        try:
            upstream = json.loads(other.execute(
                "SELECT upstream FROM prep_node WHERE id='SP-02'").fetchone()[0])
            return {
                "version": other.execute("SELECT data_version FROM meta").fetchone()[0],
                "upstream": upstream,
                "confirmations": other.execute(
                    "SELECT COUNT(*) FROM confirmation").fetchone()[0],
                "corrections": other.execute(
                    "SELECT COUNT(*) FROM correction").fetchone()[0],
                "events": other.execute(
                    "SELECT COUNT(*) FROM confirmation_event").fetchone()[0],
            }
        finally:
            other.close()

    def hook(*args, **kwargs):
        # 此刻节点/版号/更正审计已在事务内改写，旧确认尚未撤回
        observed["mid"] = read_other_connection()
        return real_revoke(*args, **kwargs)

    monkeypatch.setattr(services, "_revoke_affected_confirmations", hook)
    resp = app.test_client().post("/api/nodes/SP-02/correct",
                                  json={"new_upstream": ["PB-003"], "operator": "QC2"})
    assert resp.status_code == 200

    # 事务中途：读者看不到任何部分写入
    assert observed["mid"] == {
        "version": 1, "upstream": ["PB-001"], "confirmations": 1,
        "corrections": 0, "events": 1,
    }
    # 提交后：新来源、版号、撤回（确认消失）与两条审计同时可见
    assert read_other_connection() == {
        "version": 2, "upstream": ["PB-003"], "confirmations": 0,
        "corrections": 1, "events": 2,
    }
    page = app.test_client().get("/api/pairs").get_json()
    assert page["meta"]["data_version"] == 2
    events = app.test_client().get("/api/events").get_json()
    assert [c["node_id"] for c in events["corrections"]] == ["SP-02"]
    assert events["corrections"][0]["new_upstream"] == ["PB-003"]
    assert {e["action"] for e in events["events"]} == {"confirm", "revoke"}


# ---------- 中断 / 重启 ----------
def test_restart_only_sees_complete_before_or_after(db_path, tmp_path):
    """子进程在更正事务中写一半后被 SIGKILL：重启只能看到完整的更正前状态。"""
    script = tmp_path / "crash_mid_correction.py"
    # 只用标准库 sqlite3，复刻更正事务写到一半的落库顺序，随后被 SIGKILL。
    script.write_text(
        "import json, os, signal, sqlite3, sys\n"
        "conn = sqlite3.connect(sys.argv[1], isolation_level=None)\n"
        "conn.execute('BEGIN IMMEDIATE')\n"
        "conn.execute(\"UPDATE prep_node SET upstream = ? WHERE id = 'SP-02'\","
        " (json.dumps(['PB-003']),))\n"
        "conn.execute('UPDATE meta SET data_version = data_version + 1 WHERE id = 1')\n"
        "conn.execute(\"DELETE FROM confirmation WHERE pair_id = 'RP-01'\")\n"
        "conn.execute(\"INSERT INTO correction (node_id, old_upstream, new_upstream,"
        " reason, operator, created_at, data_version)"
        " VALUES ('SP-02', '[]', '[]', 'r', 'QC2', 't', 1)\")\n"
        "print('MID_TRANSACTION', flush=True)\n"
        "os.kill(os.getpid(), signal.SIGKILL)\n",
        encoding="utf-8",
    )
    conn = get_db(db_path)
    try:
        services.confirm_pair(conn, "RP-01", "QC1", 1)
    finally:
        conn.close()

    proc = subprocess.run(
        [sys.executable, "-I", str(script), db_path],
        capture_output=True,
    )
    assert proc.stdout.decode().strip() == "MID_TRANSACTION", proc.stderr.decode()
    assert proc.returncode == -9  # 被 SIGKILL，而非导入/执行错误

    conn = get_db(db_path)
    try:
        assert services.get_meta(conn)["data_version"] == 1
        upstream = json.loads(
            conn.execute("SELECT upstream FROM prep_node WHERE id='SP-02'").fetchone()[0]
        )
        assert upstream == ["PB-001"]
        assert conn.execute("SELECT COUNT(*) FROM confirmation").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM correction").fetchone()[0] == 0
        # 恢复可用：完整更正随后正常提交
        result = services.correct_upstream(conn, "SP-02", ["PB-003"], "QC2", "重启后更正")
        assert result["data_version"] == 2
        assert result["revoked_confirmations"] == ["RP-01"]
    finally:
        conn.close()

    application = create_app(db_path)
    body = application.test_client().get("/api/pairs").get_json()
    assert body["meta"]["data_version"] == 2
    pairs = {p["id"]: p for p in body["pairs"]}
    assert pairs["RP-01"]["confirmation"] is None
    assert pairs["RP-01"]["classification"] == "independent"
    assert len(_events(db_path)["corrections"]) == 1


def test_connection_close_mid_transaction_rolls_back(db_path):
    """连接在事务中关闭（等同进程退出）时，未提交的更正不留痕迹。"""
    conn = get_db(db_path)
    try:
        services.confirm_pair(conn, "RP-01", "QC1", 1)
    finally:
        conn.close()

    dying = get_db(db_path)
    dying.execute("BEGIN IMMEDIATE")
    dying.execute("UPDATE prep_node SET upstream = ? WHERE id='SP-02'",
                  (json.dumps(["PB-003"]),))
    dying.execute("UPDATE meta SET data_version = data_version + 1 WHERE id = 1")
    dying.execute("DELETE FROM confirmation")
    dying.close()

    check = get_db(db_path)
    try:
        assert services.get_meta(check)["data_version"] == 1
        assert json.loads(check.execute(
            "SELECT upstream FROM prep_node WHERE id='SP-02'").fetchone()[0]) == ["PB-001"]
        assert check.execute("SELECT COUNT(*) FROM confirmation").fetchone()[0] == 1
    finally:
        check.close()


def test_crash_immediately_after_commit_keeps_complete_after_state(db_path, tmp_path):
    """更正提交返回后立即 SIGKILL：重启看到的是完整的更正后状态。"""
    conn = get_db(db_path)
    try:
        services.confirm_pair(conn, "RP-01", "QC1", 1)
    finally:
        conn.close()

    pkg_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script = tmp_path / "crash_after_commit.py"
    script.write_text(
        "import os, signal, sys\n"
        "sys.path.insert(0, sys.argv[1])\n"
        "from app import services\n"
        "from app.db import get_db\n"
        "conn = get_db(sys.argv[2])\n"
        "result = services.correct_upstream(conn, 'SP-02', ['PB-003'], 'QC2', 'crash-after')\n"
        "assert result['revoked_confirmations'] == ['RP-01']\n"
        "print('COMMITTED', flush=True)\n"
        "os.kill(os.getpid(), signal.SIGKILL)\n",
        encoding="utf-8",
    )
    proc = subprocess.run(
        # 子进程需导入完整应用（含 Flask），故不用 -I；脚本由本测试生成，属可信代码。
        [sys.executable, str(script), pkg_root, db_path],
        capture_output=True,
    )
    assert proc.stdout.decode().strip() == "COMMITTED", proc.stderr.decode()
    assert proc.returncode == -9

    check = get_db(db_path)
    try:
        assert services.get_meta(check)["data_version"] == 2
        assert json.loads(check.execute(
            "SELECT upstream FROM prep_node WHERE id='SP-02'").fetchone()[0]) == ["PB-003"]
        assert check.execute("SELECT COUNT(*) FROM confirmation").fetchone()[0] == 0
        corrections = services.list_events(check)["corrections"]
        assert len(corrections) == 1 and corrections[0]["new_upstream"] == ["PB-003"]
        actions = [e["action"] for e in services.list_events(check)["events"]]
        assert actions == ["revoke", "confirm"]
    finally:
        check.close()


# ---------- 两个写者真正并发 ----------
@pytest.mark.parametrize("pair_id,affected", [("RP-01", True), ("RP-02", False)])
def test_concurrent_confirm_and_correction_serialize(app, db_path, pair_id, affected):
    """两个线程同时确认/更正：结果等价于某个串行顺序，审计自洽。"""
    barrier = threading.Barrier(2)
    results = {}

    def do_confirm():
        barrier.wait()
        r = app.test_client().post(f"/api/pairs/{pair_id}/confirm",
                                   json={"confirmed_by": "QC1", "data_version": 1})
        results["confirm"] = (r.status_code, r.get_json())

    def do_correct():
        barrier.wait()
        r = app.test_client().post("/api/nodes/SP-02/correct",
                                   json={"new_upstream": ["PB-003"]})
        results["correct"] = (r.status_code, r.get_json())

    threads = [threading.Thread(target=do_confirm), threading.Thread(target=do_correct)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    c_status, c_body = results["confirm"]
    assert results["correct"][0] == 200
    assert c_status in (200, 409)

    pair = _pair(db_path, pair_id)
    actions = [e["action"] for e in _events(db_path)["events"] if e["pair_id"] == pair_id]
    if c_status == 200:
        if affected:
            assert actions == ["revoke", "confirm"]
            assert pair["confirmation"] is None
        else:
            assert actions == ["confirm"]
            assert pair["confirmation"] is not None
    else:
        assert c_body["current_version"] == 2
        assert actions == []
        assert pair["confirmation"] is None
    assert len(_events(db_path)["corrections"]) == 1

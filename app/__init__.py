"""Flask 应用工厂与路由。"""
from __future__ import annotations

import json
import os
import sqlite3

from flask import (
    Flask,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from . import services
from .db import get_db, init_db, load_dataset, read_snapshot
from .tracing import TraceError


def create_app(db_path: str | None = None) -> Flask:
    app = Flask(__name__)
    app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-me")
    app.config["DB_PATH"] = db_path or os.environ.get(
        "DB_PATH", os.path.join(os.getcwd(), "data", "prepcheck.db")
    )
    db_dir = os.path.dirname(os.path.abspath(app.config["DB_PATH"]))
    os.makedirs(db_dir, exist_ok=True)
    init_db(app.config["DB_PATH"])

    def conn():
        return get_db(app.config["DB_PATH"])

    # ---------- 页面 ----------
    @app.get("/")
    def index():
        db = conn()
        try:
            # 一次一致性快照：来源、版号、确认与审计来自同一提交点，
            # 不会出现「已显示新来源却仍列着新来源下的确认」这类拼接状态。
            with read_snapshot(db):
                meta = services.get_meta(db)
                inventory = services.list_inventory(db)
                pairs = services.list_pairs(db)
                events = services.list_events(db)
        finally:
            db.close()
        return render_template(
            "index.html",
            meta=meta,
            inventory=inventory,
            pairs=pairs,
            events=events,
        )

    @app.post("/upload")
    def upload():
        f = request.files.get("dataset")
        if not f or not f.filename:
            flash("请选择要上传的 JSON 文件", "error")
            return redirect(url_for("index"))
        try:
            payload = json.load(f.stream)
            _validate_payload(payload)
        except (json.JSONDecodeError, ValueError) as exc:
            flash(f"JSON 解析或校验失败：{exc}", "error")
            return redirect(url_for("index"))
        db = conn()
        try:
            load_dataset(app.config["DB_PATH"], payload)
        finally:
            db.close()
        flash(f"数据集已装载：{len(payload.get('powder_batches', []))} 个制粉批，"
              f"{len(payload.get('prep_nodes', []))} 个分装/混样，"
              f"{len(payload.get('report_pairs', []))} 个报告对", "ok")
        return redirect(url_for("index"))

    @app.post("/pairs/<pair_id>/confirm")
    def confirm(pair_id):
        actor = (request.form.get("actor") or "").strip() or "质控员"
        raw_version = request.form.get("data_version")
        db = conn()
        try:
            services.confirm_pair(
                db, pair_id, actor, services.coerce_data_version(raw_version)
            )
        except services.StalePageError as exc:
            flash(f"旧页面确认被拒绝：{exc}", "error")
        except sqlite3.OperationalError:
            flash("数据库正忙，本次确认未写入，请刷新后重试", "error")
        except TraceError as exc:
            flash(f"确认失败：{exc}", "error")
        else:
            flash(f"报告对 {pair_id} 已确认", "ok")
        finally:
            db.close()
        return redirect(url_for("index"))

    @app.post("/nodes/<node_id>/correct")
    def correct(node_id):
        operator = (request.form.get("operator") or "").strip() or "质控员"
        reason = (request.form.get("reason") or "").strip()
        raw = (request.form.get("new_upstream") or "").strip()
        try:
            new_upstream = json.loads(raw)
            if not isinstance(new_upstream, list) or not all(
                isinstance(x, str) for x in new_upstream
            ):
                raise ValueError("上游必须为字符串 id 的 JSON 数组")
            db = conn()
            try:
                result = services.correct_upstream(db, node_id, new_upstream, operator, reason)
            finally:
                db.close()
        except (json.JSONDecodeError, ValueError) as exc:
            flash(f"更正被拒绝：{exc}", "error")
            return redirect(url_for("index"))
        except sqlite3.OperationalError:
            flash("数据库正忙，本次更正未写入，请刷新后重试", "error")
            return redirect(url_for("index"))
        revoked = result["revoked_confirmations"]
        flash(
            f"节点 {node_id} 来源已更正并留痕；"
            + (f"撤回确认：{', '.join(revoked)}" if revoked else "无确认受影响"),
            "ok",
        )
        return redirect(url_for("index"))

    # ---------- JSON API（供自动化测试/集成） ----------
    @app.get("/api/pairs")
    def api_pairs():
        db = conn()
        try:
            with read_snapshot(db):
                return jsonify({
                    "meta": services.get_meta(db),
                    "pairs": services.list_pairs(db),
                    "inventory": services.list_inventory(db),
                })
        finally:
            db.close()

    @app.post("/api/pairs/<pair_id>/confirm")
    def api_confirm(pair_id):
        body = request.get_json(silent=True) or {}
        db = conn()
        try:
            try:
                result = services.confirm_pair(
                    db, pair_id,
                    body.get("confirmed_by", "质控员"),
                    services.coerce_data_version(body.get("data_version")),
                )
            except services.StalePageError as exc:
                return jsonify({"error": str(exc), "current_version": exc.current_version}), 409
            except sqlite3.OperationalError:
                return jsonify({"error": "数据库正忙，请稍后重试"}), 503
            except TraceError as exc:
                return jsonify({"error": str(exc)}), 400
            return jsonify(result)
        finally:
            db.close()

    @app.post("/api/nodes/<node_id>/correct")
    def api_correct(node_id):
        body = request.get_json(silent=True) or {}
        try:
            new_upstream = body["new_upstream"]
            if not isinstance(new_upstream, list) or not all(
                isinstance(x, str) for x in new_upstream
            ):
                raise ValueError("new_upstream 必须为字符串 id 的数组")
        except (KeyError, ValueError) as exc:
            return jsonify({"error": str(exc)}), 400
        db = conn()
        try:
            try:
                result = services.correct_upstream(
                    db, node_id, new_upstream,
                    body.get("operator", "质控员"),
                    body.get("reason", ""),
                )
            except TraceError as exc:
                return jsonify({"error": str(exc)}), 400
            except sqlite3.OperationalError:
                return jsonify({"error": "数据库正忙，请稍后重试"}), 503
            return jsonify(result)
        finally:
            db.close()

    @app.get("/api/events")
    def api_events():
        db = conn()
        try:
            return jsonify(services.list_events(db))
        finally:
            db.close()

    return app


def _validate_payload(payload) -> None:
    if not isinstance(payload, dict):
        raise ValueError("根节点必须是对象")
    batches = payload.get("powder_batches", [])
    nodes = payload.get("prep_nodes", [])
    pairs = payload.get("report_pairs", [])
    if not isinstance(batches, list) or not isinstance(nodes, list) or not isinstance(pairs, list):
        raise ValueError("powder_batches / prep_nodes / report_pairs 必须为数组")

    batch_ids, node_ids = set(), set()
    for b in batches:
        if b.get("id") in batch_ids:
            raise ValueError(f"制粉批 id 重复：{b.get('id')}")
        if not isinstance(b.get("core_ids"), list) or not b["core_ids"]:
            raise ValueError(f"制粉批 {b.get('id')} 的 core_ids 必须为非空数组")
        batch_ids.add(b["id"])
    for n in nodes:
        if n.get("id") in node_ids:
            raise ValueError(f"节点 id 重复：{n.get('id')}")
        if n.get("kind") not in ("split", "mix"):
            raise ValueError(f"节点 {n.get('id')} 的 kind 必须为 split/mix")
        if not isinstance(n.get("upstream"), list) or not n["upstream"]:
            raise ValueError(f"节点 {n.get('id')} 的 upstream 必须为非空数组")
        node_ids.add(n["id"])
    known = batch_ids | node_ids
    for n in nodes:
        unknown = [u for u in n["upstream"] if u not in known]
        if unknown:
            raise ValueError(f"节点 {n['id']} 上游不存在：{', '.join(unknown)}")
        if len(set(n["upstream"])) != len(n["upstream"]):
            raise ValueError(f"节点 {n['id']} 上游重复")
    for p in pairs:
        for key in ("id", "sample_a", "sample_b"):
            if not p.get(key):
                raise ValueError(f"报告对缺少字段 {key}")
        for key in ("sample_a", "sample_b"):
            if p[key] not in known:
                raise ValueError(f"报告对 {p['id']} 的 {key} 不存在：{p[key]}")
    pair_ids = [p["id"] for p in pairs]
    if len(set(pair_ids)) != len(pair_ids):
        raise ValueError("报告对 id 重复")

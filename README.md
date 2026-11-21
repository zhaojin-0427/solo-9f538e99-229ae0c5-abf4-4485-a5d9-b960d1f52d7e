# 岩芯制样核查台（Flask + SQLite）

核查两份复测报告是否使用了同一批粉样。网页上传 JSON 数据集后，展示制粉批、
分装/混样的上游来源、待核查报告对的双侧追溯结果与自动分类，支持质控员确认、
来源更正（断链/成环拒绝、留痕）与确认自动撤回。

## 一、数据模型（JSON 上传格式）

```json
{
  "dataset_id": "demo-2026-11",
  "powder_batches": [
    { "id": "PB-001", "core_ids": ["CORE-A1"], "note": "可选备注" }
  ],
  "prep_nodes": [
    { "id": "SP-01", "kind": "split", "upstream": ["PB-001"], "note": "分装" },
    { "id": "MX-01", "kind": "mix",   "upstream": ["PB-001", "PB-002"] }
  ],
  "report_pairs": [
    {
      "id": "RP-01",
      "report_a": "RPT-1001",   "report_b": "RPT-1001-R",
      "sample_a": "SP-01",      "sample_b": "MX-01"
    }
  ]
}
```

- `powder_batches`：制粉批，`core_ids` 为该批所用的岩芯原样号列表。
- `prep_nodes`：分装（`split`）/混样（`mix`），`upstream` 为上游制粉批或其它节点的 id，
  整体必须构成有向无环图（DAG），上传时校验断链、重复与成环。
- `report_pairs`：待核查的两份报告及其样号，样号可指向制粉批或制样节点。
- 样例见 `examples/sample_dataset.json`。

## 二、分类规则（自动）

追溯时重复路径只计一次（按集合去重）。

| 分类 | 条件 |
|---|---|
| 同粉复测 `same_powder` | 两侧都只追溯到**同一个岩芯原样**，且共用**唯一制粉批** |
| 独立制样 `independent` | 两侧各只用**一个**制粉批，且两个批号**不同** |
| 待核 `pending` | 其余一切情况（多批混样、多原样共磨、追溯失败等） |

只有前两类可由质控员「按所见来源确认」。

## 三、确认、撤回与更正规则

1. **确认留快照**：确认时记录两侧的原样集合与制粉批集合快照。
2. **来源更正校验**：新上游为空、含不存在 id（断链）、自环或形成环路一律拒绝，
   库内原记录与既有确认保持不变；成功的更正写入 `correction` 审计表。
3. **自动撤回**：更正生效后，受影响报告对中**任一侧**的原样集合或制粉批集合
   发生变化，即撤回该对确认；即使更正后分类不变也撤回。
4. **来源未变则继续有效**：更正不涉及的报告对，确认保留且不标记失效。
5. **改回原来源也要重新确认**：撤回只删除确认，恢复来源不会自动恢复确认。
6. **旧页面拦截**：每次成功更正数据版本 +1；页面隐藏域携带打开时的版本号，
   版本不一致的确认请求被拒绝并提示刷新（API 返回 409）。

## 四、Docker Compose 启动

```bash
docker compose up --build
# 浏览器打开 http://localhost:8000
```

SQLite 数据位于命名卷 `prepcheck-data:/app/data`，容器重建不丢数据。
生产环境请通过环境变量 `SECRET_KEY` 覆盖默认密钥，`DB_PATH` 可改数据库路径。

## 五、本地开发（无 Docker）

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python wsgi.py            # http://127.0.0.1:8000
```

## 六、自测

```bash
pip install pytest
pytest -q
```

覆盖：追溯去重与多级追溯、三分类、确认快照、断链/自环/成环拒绝、
更正撤回（含分类不变仍撤回、来源未变保留、改回原来源需重新确认）、
旧页面 409 拦截、JSON 上传校验、页面容错渲染。

## 七、HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 核查单页（清单、追溯、确认、更正、审计） |
| POST | `/upload` | multipart 上传 JSON 数据集（整库换版） |
| POST | `/pairs/<id>/confirm` | 表单确认（带 `data_version`） |
| POST | `/nodes/<id>/correct` | 表单更正上游（`new_upstream` 为 JSON 数组字符串） |
| GET | `/api/pairs` | 清单 + 报告对追溯/分类/确认状态 |
| POST | `/api/pairs/<id>/confirm` | JSON 确认，过期返回 409 |
| POST | `/api/nodes/<id>/correct` | JSON 更正，断链/成环返回 400 |
| GET | `/api/events` | 更正与确认/撤回审计 |

-- 制样核查 SQLite 结构
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS meta (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    data_version  INTEGER NOT NULL DEFAULT 1,
    dataset_id    TEXT
);
INSERT OR IGNORE INTO meta (id, data_version) VALUES (1, 1);

-- 制粉批：一批粉对应若干岩芯原样
CREATE TABLE IF NOT EXISTS powder_batch (
    id          TEXT PRIMARY KEY,
    core_ids    TEXT NOT NULL,          -- JSON 数组：岩芯原样号
    note        TEXT
);

-- 分装/混样节点，上游为制粉批或其它节点（DAG）
CREATE TABLE IF NOT EXISTS prep_node (
    id          TEXT PRIMARY KEY,
    kind        TEXT NOT NULL,          -- split（分装） / mix（混样）
    upstream    TEXT NOT NULL,          -- JSON 数组：上游 id 列表
    note        TEXT
);

-- 待核查报告对
CREATE TABLE IF NOT EXISTS report_pair (
    id            TEXT PRIMARY KEY,
    report_a      TEXT,
    report_b      TEXT,
    sample_a      TEXT NOT NULL,        -- 样号（指向粉批或制样节点）
    sample_b      TEXT NOT NULL
);

-- 质控确认
CREATE TABLE IF NOT EXISTS confirmation (
    pair_id      TEXT PRIMARY KEY REFERENCES report_pair(id),
    kind         TEXT NOT NULL,         -- same_powder / independent
    confirmed_by TEXT NOT NULL,
    confirmed_at TEXT NOT NULL,
    -- 确认时刻两侧的追溯快照（JSON），用于更正后判定是否撤回
    snapshot     TEXT NOT NULL
);

-- 确认撤回审计
CREATE TABLE IF NOT EXISTS confirmation_event (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    pair_id     TEXT NOT NULL,
    action      TEXT NOT NULL,          -- confirm / revoke
    kind        TEXT,
    actor       TEXT,
    reason      TEXT,
    created_at  TEXT NOT NULL
);

-- 来源更正审计（保留原记录）
CREATE TABLE IF NOT EXISTS correction (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id       TEXT NOT NULL,
    old_upstream  TEXT NOT NULL,        -- JSON 数组
    new_upstream  TEXT NOT NULL,        -- JSON 数组
    reason        TEXT,
    operator      TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    data_version  INTEGER NOT NULL
);

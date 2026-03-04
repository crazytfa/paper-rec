-- 在 Supabase 控制台 SQL Editor 中运行（一次性）
-- 位置：supabase.com → 你的项目 → SQL Editor → New query

-- ① 已推送论文记录表（去重用）
CREATE TABLE IF NOT EXISTS sent_papers (
    id              BIGSERIAL PRIMARY KEY,
    paper_id        VARCHAR(32)  NOT NULL,
    title           VARCHAR(500),
    doi             VARCHAR(200),
    topic_id        VARCHAR(50),           -- 对应 config.yaml 中的主题 id
    relevance_score FLOAT DEFAULT 0,
    sent_at         TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_id  ON sent_papers (paper_id);
CREATE        INDEX IF NOT EXISTS idx_sent_at   ON sent_papers (sent_at);
CREATE        INDEX IF NOT EXISTS idx_topic_id  ON sent_papers (topic_id);

-- ② 主题配置表（供飞书机器人动态修改主题时使用，可选）
CREATE TABLE IF NOT EXISTS user_topics (
    id          BIGSERIAL PRIMARY KEY,
    topic_id    VARCHAR(50) NOT NULL,
    name        VARCHAR(100),
    config_json JSONB,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_user_topic_id ON user_topics (topic_id);

-- 验证建表成功
SELECT table_name, pg_size_pretty(pg_total_relation_size(quote_ident(table_name)))
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;

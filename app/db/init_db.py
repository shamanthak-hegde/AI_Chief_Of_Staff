from app.db.session import get_conn


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS people (
    id SERIAL PRIMARY KEY,
    handle VARCHAR(120) UNIQUE NOT NULL,
    display_name VARCHAR(200),
    email VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    platform VARCHAR(50) NOT NULL,
    external_id VARCHAR(200),
    ts TIMESTAMPTZ NOT NULL,
    sender_person_id INTEGER REFERENCES people(id),
    channel_id VARCHAR(200),
    thread_id VARCHAR(200),
    subject VARCHAR(500),
    text TEXT,
    raw_json JSONB
);

CREATE TABLE IF NOT EXISTS message_recipients (
    message_id INTEGER REFERENCES messages(id) ON DELETE CASCADE,
    recipient_person_id INTEGER REFERENCES people(id),
    kind VARCHAR(50) NOT NULL,
    PRIMARY KEY (message_id, recipient_person_id)
);

CREATE TABLE IF NOT EXISTS turns (
    id SERIAL PRIMARY KEY,
    platform VARCHAR(50) NOT NULL,
    channel_id VARCHAR(200),
    thread_id VARCHAR(200),
    sender_person_id INTEGER REFERENCES people(id),
    start_ts TIMESTAMPTZ NOT NULL,
    end_ts TIMESTAMPTZ NOT NULL,
    text TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS turn_messages (
    turn_id INTEGER REFERENCES turns(id) ON DELETE CASCADE,
    message_id INTEGER REFERENCES messages(id) ON DELETE CASCADE,
    PRIMARY KEY (turn_id, message_id)
);

CREATE TABLE IF NOT EXISTS topics (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS truth_items (
    id SERIAL PRIMARY KEY,
    type VARCHAR(100) NOT NULL,
    title VARCHAR(500) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    current_version_id INTEGER
);

CREATE TABLE IF NOT EXISTS truth_item_embeddings (
    truth_item_id INTEGER PRIMARY KEY REFERENCES truth_items(id) ON DELETE CASCADE,
    embedding JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS knowledge_prs (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT now(),
    source_turn_id INTEGER REFERENCES turns(id),
    status VARCHAR(50) DEFAULT 'needs_review',
    extracted_json JSONB,
    model VARCHAR(100),
    title VARCHAR(300)
);

CREATE TABLE IF NOT EXISTS truth_versions (
    id SERIAL PRIMARY KEY,
    truth_item_id INTEGER REFERENCES truth_items(id) ON DELETE CASCADE,
    version_num INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    summary TEXT NOT NULL,
    sources_json JSONB,
    confidence DOUBLE PRECISION,
    merged_from_pr_id INTEGER REFERENCES knowledge_prs(id),
    UNIQUE (truth_item_id, version_num)
);

ALTER TABLE truth_items
    ADD CONSTRAINT truth_items_current_version_fk
    FOREIGN KEY (current_version_id)
    REFERENCES truth_versions(id);

CREATE TABLE IF NOT EXISTS pr_changes (
    id SERIAL PRIMARY KEY,
    pr_id INTEGER REFERENCES knowledge_prs(id) ON DELETE CASCADE,
    truth_item_id INTEGER REFERENCES truth_items(id),
    previous_version_id INTEGER REFERENCES truth_versions(id),
    proposed_summary TEXT NOT NULL,
    diff_summary TEXT,
    similarity DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS pr_conflicts (
    id SERIAL PRIMARY KEY,
    pr_id INTEGER REFERENCES knowledge_prs(id) ON DELETE CASCADE,
    truth_item_id INTEGER REFERENCES truth_items(id),
    conflict_type VARCHAR(100) NOT NULL,
    existing_claim TEXT,
    new_claim TEXT,
    resolution_hint TEXT
);

CREATE TABLE IF NOT EXISTS pr_stakeholders (
    id SERIAL PRIMARY KEY,
    pr_id INTEGER REFERENCES knowledge_prs(id) ON DELETE CASCADE,
    person_id INTEGER REFERENCES people(id),
    score DOUBLE PRECISION NOT NULL,
    reason TEXT NOT NULL,
    mode VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS comm_edges (
    src_person_id INTEGER REFERENCES people(id),
    dst_person_id INTEGER REFERENCES people(id),
    weight DOUBLE PRECISION NOT NULL,
    last_ts TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (src_person_id, dst_person_id)
);
"""


def init_db() -> None:
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    init_db()

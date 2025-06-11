CREATE TABLE IF NOT EXISTS chats (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    activated BOOLEAN DEFAULT FALSE,
    can_read_messages BOOLEAN DEFAULT FALSE,
    can_restrict_members BOOLEAN DEFAULT FALSE,
    is_bot_in BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS chat_admins (
    chat_id BIGINT REFERENCES chats(id) ON DELETE CASCADE,
    user_id BIGINT,
    activated BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (chat_id, user_id)
);

CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    username TEXT,
    full_name TEXT
);

-- Промпты / правила
CREATE TABLE IF NOT EXISTS rules (
    id BIGSERIAL PRIMARY KEY,
    chat_id BIGINT REFERENCES chats(id) ON DELETE CASCADE,
    rule_text TEXT NOT NULL,
    explanation_text TEXT,
    type TEXT CHECK (type IN ('BAN', 'NOTIFY', 'OBSERVE')) NOT NULL,
    activated BOOLEAN DEFAULT TRUE,
    is_silent BOOLEAN DEFAULT FALSE
);

-- Сообщения нарушителей
CREATE TABLE IF NOT EXISTS violator_messages (
    id BIGSERIAL PRIMARY KEY,
    violator_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
    text TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    post_id BIGINT
);

-- Нарушения правил (факт совпадения правила и сообщения)
CREATE TABLE IF NOT EXISTS rule_violations (
    id BIGSERIAL PRIMARY KEY,
    rule_id BIGINT REFERENCES rules(id) ON DELETE CASCADE,
    violator_msg_id BIGINT REFERENCES violator_messages(id) ON DELETE CASCADE,
    detected_at TIMESTAMP DEFAULT NOW()
);

-- Решения модераторов по нарушениям (история)
CREATE TABLE IF NOT EXISTS rule_violation_decision (
    id BIGSERIAL PRIMARY KEY,
    rule_violation_id BIGINT REFERENCES rule_violations(id) ON DELETE CASCADE,
    moderator_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
    timestamp TIMESTAMP DEFAULT NOW(),
    decision TEXT CHECK (decision IN ('BAN', 'UNBAN')) NOT NULL
);

-- Модераторы чатов
CREATE TABLE IF NOT EXISTS chat_moderators (
    chat_id BIGINT REFERENCES chats(id) ON DELETE CASCADE,
    user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
    activated BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (chat_id, user_id)
);

-- Настройки уведомлений модераторов
CREATE TABLE IF NOT EXISTS rule_violation_notification_policies (
    moderator_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
    policy TEXT CHECK (
        policy IN (
            'NOTIFY_BAN',
            'NOT_NOTIFY_BAN',
            'NOTIFY_NOTIFICATION',
            'NOT_NOTIFY_NOTIFICATION'
        )
    ),
    PRIMARY KEY (moderator_id, policy)
);

CREATE TABLE IF NOT EXISTS moderator_rule_last_seen (
    moderator_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
    rule_id BIGINT REFERENCES rules(id) ON DELETE CASCADE,
    last_seen_timestamp TIMESTAMP,
    PRIMARY KEY (moderator_id, rule_id)
);

CREATE TABLE IF NOT EXISTS message_images (
    id UUID PRIMARY KEY,
    image_data BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS message_audios (
    id UUID PRIMARY KEY,
    audio_data BYTEA NOT NULL
); 
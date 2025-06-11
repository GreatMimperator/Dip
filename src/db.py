import asyncpg
import os
from typing import List, Dict, Optional, Tuple
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Константы для типов правил
RULE_TYPE_BAN = 'BAN'
RULE_TYPE_NOTIFY = 'NOTIFY'
RULE_TYPE_OBSERVE = 'OBSERVE'

# Константы для решений модераторов
DECISION_BAN = 'BAN'
DECISION_WARN = 'WARN'

class Database:
    def __init__(self, config):
        self.config = config
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            host=self.config.postgres.host,
            port=self.config.postgres.port,
            user=self.config.postgres.user,
            password=self.config.postgres.password,
            database=self.config.postgres.db,
        )

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def add_chat(
        self, chat_id: int, title: str,
        activated: bool = True,
        can_read_messages: bool = False,
        can_restrict_members: bool = False,
        is_bot_in: bool = True
    ) -> None:
        """Добавляет или обновляет чат с расширенными параметрами."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO chats (id, title, activated, can_read_messages, can_restrict_members, is_bot_in) '
                'VALUES ($1, $2, $3, $4, $5, $6) '
                'ON CONFLICT (id) DO UPDATE '
                'SET title = $2, activated = $3, can_read_messages = $4, can_restrict_members = $5, is_bot_in = $6',
                chat_id, title, activated, can_read_messages, can_restrict_members, is_bot_in
            )

    async def update_chat_status(self, chat_id: int, activated: bool) -> None:
        """Обновляет статус чата."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'UPDATE chats SET activated = $2 WHERE id = $1',
                chat_id, activated
            )

    async def get_chat(self, chat_id: int) -> Dict:
        """Возвращает информацию о чате."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT id, title, activated, can_read_messages '
                'FROM chats '
                'WHERE id = $1',
                chat_id
            )
            if not row:
                return None
            return {
                'id': row['id'],
                'title': row['title'],
                'activated': row['activated'],
                'can_read_messages': row['can_read_messages']
            }

    async def get_active_chats(self) -> List[Dict]:
        """Возвращает список активных чатов."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT id, title, activated '
                'FROM chats '
                'WHERE activated = TRUE '
                'ORDER BY title'
            )
            return [{
                'id': r['id'],
                'title': r['title'],
                'activated': r['activated']
            } for r in rows]

    async def get_chat_stats(self, chat_id: int) -> Dict:
        """Возвращает статистику чата."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT '
                'COUNT(DISTINCT r.id) as rules_count, '
                'COUNT(DISTINCT cm.user_id) as moderators_count, '
                'COUNT(DISTINCT rv.id) as violations_count, '
                'COUNT(DISTINCT vm.violator_id) as violators_count '
                'FROM chats c '
                'LEFT JOIN rules r ON c.id = r.chat_id AND r.activated = TRUE '
                'LEFT JOIN chat_moderators cm ON c.id = cm.chat_id AND cm.activated = TRUE '
                'LEFT JOIN rule_violations rv ON r.id = rv.rule_id '
                'LEFT JOIN violator_messages vm ON rv.violator_msg_id = vm.id '
                'WHERE c.id = $1 '
                'GROUP BY c.id',
                chat_id
            )
            if not row:
                return None
            return {
                'rules_count': row['rules_count'],
                'moderators_count': row['moderators_count'],
                'violations_count': row['violations_count'],
                'violators_count': row['violators_count']
            }

    async def add_admin(self, chat_id: int, user_id: int, activated: bool = True) -> None:
        """Добавляет администратора в чат или обновляет его статус."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO chat_admins (chat_id, user_id, activated) '
                'VALUES ($1, $2, $3) '
                'ON CONFLICT (chat_id, user_id) DO UPDATE SET activated = $3',
                chat_id, user_id, activated
            )

    async def get_admin_chats_for_user(self, user_id):
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT c.id, c.title FROM chats c '
                'JOIN chat_admins a ON c.id = a.chat_id '
                'WHERE a.user_id = $1 AND c.is_bot_in = TRUE',
                user_id
            )
            return [{'id': r['id'], 'title': r['title']} for r in rows]

    async def remove_admin_from_all_chats(self, user_id):
        async with self.pool.acquire() as conn:
            await conn.execute(
                'DELETE FROM chat_admins WHERE user_id = $1',
                user_id
            )

    async def get_all_active_chats(self):
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT id, title FROM chats WHERE activated = TRUE'
            )
            return [{'id': r['id'], 'title': r['title']} for r in rows]

    async def update_admin_status(self, chat_id: int, user_id: int, activated: bool) -> None:
        """Обновляет статус администратора в чате."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'UPDATE chat_admins SET activated = $3 WHERE chat_id = $1 AND user_id = $2',
                chat_id, user_id, activated
            )

    async def create_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS chats (
                    id BIGINT PRIMARY KEY,
                    title TEXT,
                    activated BOOLEAN DEFAULT TRUE,
                    can_read_messages BOOLEAN DEFAULT FALSE,
                    can_restrict_members BOOLEAN DEFAULT FALSE,
                    is_bot_in BOOLEAN DEFAULT FALSE
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS chat_admins (
                    chat_id BIGINT REFERENCES chats(id),
                    user_id BIGINT,
                    PRIMARY KEY (chat_id, user_id)
                )
            ''')

    async def add_or_update_user(self, user_id: int, username: str, full_name: str) -> None:
        """Добавляет или обновляет пользователя в таблице users."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO users (user_id, username, full_name) VALUES ($1, $2, $3) '
                'ON CONFLICT (user_id) DO UPDATE SET username = $2, full_name = $3',
                user_id, username, full_name
            )

    async def get_all_users(self, offset: int, limit: int) -> List[Dict]:
        """Возвращает список всех активных администраторов с пейджингом."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT DISTINCT u.user_id, u.username, u.full_name '
                'FROM users u '
                'JOIN chat_admins ca ON u.user_id = ca.user_id '
                'WHERE ca.activated = TRUE '
                'ORDER BY u.user_id '
                'LIMIT $1 OFFSET $2',
                limit, offset
            )
            return [{'user_id': r['user_id'], 'username': r['username'], 'full_name': r['full_name']} for r in rows]

    async def get_users_count(self) -> int:
        """Возвращает общее количество активных администраторов."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT COUNT(DISTINCT u.user_id) '
                'FROM users u '
                'JOIN chat_admins ca ON u.user_id = ca.user_id '
                'WHERE ca.activated = TRUE'
            )

    async def user_exists(self, user_id: int) -> bool:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT 1 FROM users WHERE user_id = $1', user_id)
            return row is not None

    async def get_all_chats(self):
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT id, title, activated FROM chats')
            return [{'id': r['id'], 'title': r['title'], 'activated': r['activated']} for r in rows]

    async def add_moderator(self, chat_id, user_id, activated=True):
        """Добавляет модератора в чат."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO chat_moderators (chat_id, user_id, activated) VALUES ($1, $2, $3) '
                'ON CONFLICT (chat_id, user_id) DO UPDATE SET activated = $3',
                chat_id, user_id, activated
            )

    async def get_moderator_chats_for_user(self, user_id):
        """Возвращает чаты, где пользователь активный админ (может назначать модераторов)."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT c.id, c.title FROM chats c '
                'JOIN chat_admins a ON c.id = a.chat_id '
                'WHERE a.user_id = $1 AND a.activated = TRUE AND c.activated = TRUE',
                user_id
            )
            return [{'id': r['id'], 'title': r['title']} for r in rows]

    async def get_all_moderators(self, offset: int, limit: int):
        """Возвращает список всех активных модераторов с пейджингом."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT DISTINCT u.user_id, u.username, u.full_name '
                'FROM users u '
                'JOIN chat_moderators cm ON u.user_id = cm.user_id '
                'WHERE cm.activated = TRUE '
                'ORDER BY u.user_id '
                'LIMIT $1 OFFSET $2',
                limit, offset
            )
            return [{'user_id': r['user_id'], 'username': r['username'], 'full_name': r['full_name']} for r in rows]

    async def update_moderator_status(self, chat_id, user_id, is_active):
        """Обновляет статус активации модератора."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'UPDATE chat_moderators SET activated = $3 WHERE chat_id = $1 AND user_id = $2',
                chat_id, user_id, is_active
            )

    async def user_is_admin_in_chat(self, user_id, chat_id):
        """Проверяет, что пользователь активный админ в чате."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT 1 FROM chat_admins WHERE chat_id = $1 AND user_id = $2 AND activated = TRUE',
                chat_id, user_id
            )
            return row is not None

    async def get_user_moderator_chats(self, user_id):
        """Возвращает чаты, где пользователь является активным модератором."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT c.id, c.title FROM chats c '
                'JOIN chat_moderators m ON c.id = m.chat_id '
                'WHERE m.user_id = $1 AND m.activated = TRUE AND c.activated = TRUE',
                user_id
            )
            return [{'id': r['id'], 'title': r['title']} for r in rows]

    async def get_moderators_count(self) -> int:
        """Возвращает общее количество активных модераторов."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT COUNT(DISTINCT u.user_id) '
                'FROM users u '
                'JOIN chat_moderators cm ON u.user_id = cm.user_id '
                'WHERE cm.activated = TRUE'
            )

    async def add_rule(self, chat_id: int, rule_text: str, explanation_text: str, rule_type: str, is_silent: bool = None) -> int:
        """Добавляет новое правило в базу данных."""
        async with self.pool.acquire() as conn:
            # Получаем следующий доступный ID
            next_id = await conn.fetchval('SELECT COALESCE(MAX(id), 0) + 1 FROM rules')
            row = await conn.fetchrow(
                'INSERT INTO rules (id, chat_id, rule_text, explanation_text, type, is_silent) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id',
                next_id, chat_id, rule_text, explanation_text, rule_type, is_silent
            )
            return row['id']

    async def get_rules_for_chat(self, chat_id: int, offset: int, limit: int) -> List[Dict]:
        """Возвращает список правил для чата с пейджингом."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT r.id, r.rule_text, r.explanation_text, r.type, r.activated, '
                'COUNT(rv.id) as violation_count '
                'FROM rules r '
                'LEFT JOIN rule_violations rv ON r.id = rv.rule_id '
                'WHERE r.chat_id = $1 AND r.activated = TRUE '
                'GROUP BY r.id '
                'ORDER BY '
                'CASE r.type '
                '    WHEN \'BAN\' THEN 1 '
                '    WHEN \'NOTIFY\' THEN 2 '
                '    WHEN \'OBSERVE\' THEN 3 '
                'END, '
                'r.id '
                'LIMIT $2 OFFSET $3',
                chat_id, limit, offset
            )
            return [{
                'id': r['id'],
                'rule_text': r['rule_text'],
                'explanation_text': r['explanation_text'],
                'type': r['type'],
                'activated': r['activated'],
                'violation_count': r['violation_count']
            } for r in rows]

    async def get_rules_count_for_chat(self, chat_id: int) -> int:
        """Возвращает количество активных правил в чате."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT COUNT(*) FROM rules WHERE chat_id = $1 AND activated = TRUE',
                chat_id
            )

    async def get_rule_details(self, rule_id: int) -> Dict:
        """Возвращает детальную информацию о правиле."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT r.id, r.chat_id, r.rule_text, r.explanation_text, r.type, r.activated, '
                'c.title as chat_title, '
                'COUNT(rv.id) as violation_count '
                'FROM rules r '
                'JOIN chats c ON r.chat_id = c.id '
                'LEFT JOIN rule_violations rv ON r.id = rv.rule_id '
                'WHERE r.id = $1 '
                'GROUP BY r.id, c.title',
                rule_id
            )
            if not row:
                return None
            return {
                'id': row['id'],
                'chat_id': row['chat_id'],
                'chat_title': row['chat_title'],
                'rule_text': row['rule_text'],
                'explanation_text': row['explanation_text'],
                'type': row['type'],
                'activated': row['activated'],
                'violation_count': row['violation_count']
            }

    async def update_rule_status(self, rule_id: int, activated: bool) -> None:
        """Обновляет статус активации правила."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'UPDATE rules SET activated = $2 WHERE id = $1',
                rule_id, activated
            )

    async def get_rule_violations(self, rule_id: int, offset: int, limit: int) -> List[Dict]:
        """Возвращает список нарушений правила с пейджингом."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT rv.id, rv.detected_at, '
                'vm.text as message_text, vm.timestamp as message_time, '
                'u.username, u.full_name, '
                'rvd.decision, rvd.timestamp as decision_time, '
                'm.username as moderator_username, m.full_name as moderator_name '
                'FROM rule_violations rv '
                'JOIN violator_messages vm ON rv.violator_msg_id = vm.id '
                'JOIN users u ON vm.violator_id = u.user_id '
                'LEFT JOIN rule_violation_decision rvd ON rv.id = rvd.rule_violation_id '
                'LEFT JOIN users m ON rvd.moderator_id = m.user_id '
                'WHERE rv.rule_id = $1 '
                'ORDER BY rv.detected_at DESC '
                'LIMIT $2 OFFSET $3',
                rule_id, limit, offset
            )
            return [{
                'id': r['id'],
                'detected_at': r['detected_at'],
                'message_text': r['message_text'],
                'message_time': r['message_time'],
                'username': r['username'],
                'full_name': r['full_name'],
                'decision': r['decision'],
                'decision_time': r['decision_time'],
                'moderator_username': r['moderator_username'],
                'moderator_name': r['moderator_name']
            } for r in rows]

    async def get_rule_violations_count(self, rule_id: int) -> int:
        """Возвращает количество нарушений правила."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT COUNT(*) FROM rule_violations WHERE rule_id = $1',
                rule_id
            )

    async def search_violations(self, search_term: str, offset: int, limit: int) -> List[Dict]:
        """Поиск нарушений по тегу или ID."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT rv.id, rv.detected_at, '
                'vm.text as message_text, vm.timestamp as message_time, '
                'u.username, u.full_name, '
                'r.rule_text, r.type, '
                'c.title as chat_title, '
                'rvd.decision, rvd.timestamp as decision_time, '
                'm.username as moderator_username, m.full_name as moderator_name '
                'FROM rule_violations rv '
                'JOIN violator_messages vm ON rv.violator_msg_id = vm.id '
                'JOIN users u ON vm.violator_id = u.user_id '
                'JOIN rules r ON rv.rule_id = r.id '
                'JOIN chats c ON r.chat_id = c.id '
                'LEFT JOIN rule_violation_decision rvd ON rv.id = rvd.rule_violation_id '
                'LEFT JOIN users m ON rvd.moderator_id = m.user_id '
                'WHERE u.username ILIKE $1 OR u.full_name ILIKE $1 OR vm.text ILIKE $1 '
                'ORDER BY rv.detected_at DESC '
                'LIMIT $2 OFFSET $3',
                f'%{search_term}%', limit, offset
            )
            return [{
                'id': r['id'],
                'detected_at': r['detected_at'],
                'message_text': r['message_text'],
                'message_time': r['message_time'],
                'username': r['username'],
                'full_name': r['full_name'],
                'rule_text': r['rule_text'],
                'rule_type': r['type'],
                'chat_title': r['chat_title'],
                'decision': r['decision'],
                'decision_time': r['decision_time'],
                'moderator_username': r['moderator_username'],
                'moderator_name': r['moderator_name']
            } for r in rows]

    async def get_search_violations_count(self, search_term: str) -> int:
        """Возвращает количество найденных нарушений."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT COUNT(*) '
                'FROM rule_violations rv '
                'JOIN violator_messages vm ON rv.violator_msg_id = vm.id '
                'JOIN users u ON vm.violator_id = u.user_id '
                'WHERE u.username ILIKE $1 OR u.full_name ILIKE $1 OR vm.text ILIKE $1',
                f'%{search_term}%'
            )

    async def add_decision(self, rule_violation_id: int, moderator_id: int, decision: str) -> int:
        """Добавляет решение модератора по нарушению."""
        query = """
            INSERT INTO rule_violation_decision 
            (rule_violation_id, moderator_id, decision)
            VALUES ($1, $2, $3)
            RETURNING id
        """
        row = await self.pool.fetchrow(query, rule_violation_id, moderator_id, decision)
        return row['id']

    async def update_decision(self, decision_id: int, decision: str) -> None:
        """Обновляет решение модератора."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'UPDATE rule_violation_decision '
                'SET decision = $2 '
                'WHERE id = $1',
                decision_id, decision
            )

    async def get_decision(self, decision_id: int) -> Dict:
        """Возвращает информацию о решении модератора."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT d.id, d.rule_violation_id, d.moderator_id, d.decision, d.timestamp, '
                'm.username as moderator_username, m.full_name as moderator_name, '
                'rv.detected_at, '
                'r.rule_text, r.type as rule_type, '
                'c.title as chat_title '
                'FROM rule_violation_decision d '
                'JOIN users m ON d.moderator_id = m.user_id '
                'JOIN rule_violations rv ON d.rule_violation_id = rv.id '
                'JOIN rules r ON rv.rule_id = r.id '
                'JOIN chats c ON r.chat_id = c.id '
                'WHERE d.id = $1',
                decision_id
            )
            if not row:
                return None
            return {
                'id': row['id'],
                'rule_violation_id': row['rule_violation_id'],
                'moderator_id': row['moderator_id'],
                'moderator_username': row['moderator_username'],
                'moderator_name': row['moderator_name'],
                'decision': row['decision'],
                'timestamp': row['timestamp'],
                'detected_at': row['detected_at'],
                'rule_text': row['rule_text'],
                'rule_type': row['rule_type'],
                'chat_title': row['chat_title']
            }

    async def get_chat_decisions(self, chat_id: int, offset: int, limit: int, moderator_id: Optional[int] = None) -> List[Dict]:
        """Получает решения по нарушениям для чата."""
        query = """
            SELECT 
                rvd.id,
                rvd.rule_violation_id,
                rvd.moderator_id,
                rvd.timestamp,
                rvd.decision,
                u.username as moderator_username,
                u.full_name as moderator_full_name,
                rv.rule_id,
                r.rule_text,
                vm.violator_id,
                vm.text as message_text,
                vm.timestamp as message_timestamp,
                vu.username as violator_username,
                vu.full_name as violator_full_name
            FROM rule_violation_decision rvd
            JOIN users u ON rvd.moderator_id = u.user_id
            JOIN rule_violations rv ON rvd.rule_violation_id = rv.id
            JOIN rules r ON rv.rule_id = r.id
            JOIN violator_messages vm ON rv.violator_msg_id = vm.id
            JOIN users vu ON vm.violator_id = vu.user_id
            WHERE r.chat_id = $1
        """
        params = [chat_id]
        
        if moderator_id:
            query += " AND rvd.moderator_id = $2"
            params.append(moderator_id)
            
        query += " ORDER BY rvd.timestamp DESC LIMIT $%d OFFSET $%d" % (len(params) + 1, len(params) + 2)
        params.extend([limit, offset])
        
        rows = await self.pool.fetch(query, *params)
        return [dict(row) for row in rows]

    async def get_chat_decisions_count(self, chat_id: int) -> int:
        """Возвращает количество решений модераторов в чате."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT COUNT(*) '
                'FROM rule_violation_decision d '
                'JOIN rule_violations rv ON d.rule_violation_id = rv.id '
                'JOIN rules r ON rv.rule_id = r.id '
                'WHERE r.chat_id = $1',
                chat_id
            )

    async def add_violator_message(self, violator_id: int, text: str, timestamp: datetime) -> int:
        """Добавляет сообщение нарушителя."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'INSERT INTO violator_messages (violator_id, text, timestamp) '
                'VALUES ($1, $2, $3) RETURNING id',
                violator_id, text, timestamp
            )

    async def add_rule_violation(self, rule_id: int, violator_msg_id: int, detected_at: datetime) -> int:
        """Добавляет нарушение правила."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'INSERT INTO rule_violations (rule_id, violator_msg_id, detected_at) '
                'VALUES ($1, $2, $3) RETURNING id',
                rule_id, violator_msg_id, detected_at
            )

    async def get_violator_messages(self, chat_id: int, offset: int, limit: int) -> List[Dict]:
        """Возвращает список сообщений нарушителей с пейджингом."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT vm.id, vm.text, vm.timestamp, '
                'u.username, u.full_name, '
                'COUNT(rv.id) as violation_count '
                'FROM violator_messages vm '
                'JOIN users u ON vm.violator_id = u.user_id '
                'LEFT JOIN rule_violations rv ON vm.id = rv.violator_msg_id '
                'WHERE vm.chat_id = $1 '
                'GROUP BY vm.id, u.username, u.full_name '
                'ORDER BY vm.timestamp DESC '
                'LIMIT $2 OFFSET $3',
                chat_id, limit, offset
            )
            return [{
                'id': r['id'],
                'text': r['text'],
                'timestamp': r['timestamp'],
                'username': r['username'],
                'full_name': r['full_name'],
                'violation_count': r['violation_count']
            } for r in rows]

    async def get_violator_messages_count(self, chat_id: int) -> int:
        """Возвращает количество сообщений нарушителей в чате."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT COUNT(*) FROM violator_messages WHERE chat_id = $1',
                chat_id
            )

    async def get_violator_message_details(self, message_id: int) -> Dict:
        """Возвращает детальную информацию о сообщении нарушителя."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT vm.id, vm.text, vm.timestamp, '
                'u.username, u.full_name, '
                'c.title as chat_title, '
                'COUNT(rv.id) as violation_count '
                'FROM violator_messages vm '
                'JOIN users u ON vm.violator_id = u.user_id '
                'JOIN chats c ON vm.chat_id = c.id '
                'LEFT JOIN rule_violations rv ON vm.id = rv.violator_msg_id '
                'WHERE vm.id = $1 '
                'GROUP BY vm.id, u.username, u.full_name, c.title',
                message_id
            )
            if not row:
                return None
            return {
                'id': row['id'],
                'text': row['text'],
                'timestamp': row['timestamp'],
                'username': row['username'],
                'full_name': row['full_name'],
                'chat_title': row['chat_title'],
                'violation_count': row['violation_count']
            }

    async def add_notification_policy(self, chat_id: int, rule_type: str, notify_moderators: bool, notify_admins: bool) -> int:
        """Добавляет политику уведомлений для типа правил."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'INSERT INTO notification_policies (chat_id, rule_type, notify_moderators, notify_admins) '
                'VALUES ($1, $2, $3, $4) RETURNING id',
                chat_id, rule_type, notify_moderators, notify_admins
            )

    async def update_notification_policy(self, policy_id: int, notify_moderators: bool, notify_admins: bool) -> None:
        """Обновляет политику уведомлений."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'UPDATE notification_policies '
                'SET notify_moderators = $2, notify_admins = $3 '
                'WHERE id = $1',
                policy_id, notify_moderators, notify_admins
            )

    async def get_notification_policies(self, chat_id: int) -> List[Dict]:
        """Возвращает список политик уведомлений для чата."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT id, rule_type, notify_moderators, notify_admins '
                'FROM notification_policies '
                'WHERE chat_id = $1 '
                'ORDER BY '
                'CASE rule_type '
                '    WHEN \'BAN\' THEN 1 '
                '    WHEN \'NOTIFY\' THEN 2 '
                '    WHEN \'OBSERVE\' THEN 3 '
                'END',
                chat_id
            )
            return [{
                'id': r['id'],
                'rule_type': r['rule_type'],
                'notify_moderators': r['notify_moderators'],
                'notify_admins': r['notify_admins']
            } for r in rows]

    async def get_notification_policy(self, chat_id: int, rule_type: str) -> Dict:
        """Возвращает политику уведомлений для типа правил."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT id, rule_type, notify_moderators, notify_admins '
                'FROM notification_policies '
                'WHERE chat_id = $1 AND rule_type = $2',
                chat_id, rule_type
            )
            if not row:
                return None
            return {
                'id': row['id'],
                'rule_type': row['rule_type'],
                'notify_moderators': row['notify_moderators'],
                'notify_admins': row['notify_admins']
            }

    async def add_chat_moderator(self, chat_id: int, user_id: int) -> int:
        """Добавляет модератора в чат."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'INSERT INTO chat_moderators (chat_id, user_id) VALUES ($1, $2) RETURNING id',
                chat_id, user_id
            )

    async def update_chat_moderator_status(self, chat_id: int, user_id: int, activated: bool) -> None:
        """Обновляет статус модератора в чате."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'UPDATE chat_moderators SET activated = $3 WHERE chat_id = $1 AND user_id = $2',
                chat_id, user_id, activated
            )

    async def get_chat_moderators(self, chat_id: int) -> List[Dict]:
        """Возвращает список модераторов чата."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT cm.user_id, cm.activated, '
                'u.username, u.full_name '
                'FROM chat_moderators cm '
                'JOIN users u ON cm.user_id = u.user_id '
                'WHERE cm.chat_id = $1 '
                'ORDER BY u.username',
                chat_id
            )
            return [{
                'user_id': r['user_id'],
                'activated': r['activated'],
                'username': r['username'],
                'full_name': r['full_name']
            } for r in rows]

    async def get_user_moderated_chats(self, user_id: int) -> List[Dict]:
        """Возвращает список чатов, где пользователь является модератором."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT c.id, c.title, c.activated, '
                'cm.activated as moderator_activated '
                'FROM chats c '
                'JOIN chat_moderators cm ON c.id = cm.chat_id '
                'WHERE cm.user_id = $1 AND c.activated = TRUE '
                'ORDER BY c.title',
                user_id
            )
            return [{
                'id': r['id'],
                'title': r['title'],
                'activated': r['activated'],
                'moderator_activated': r['moderator_activated']
            } for r in rows]

    async def is_chat_moderator(self, chat_id: int, user_id: int) -> bool:
        """Проверяет, является ли пользователь активным модератором чата."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT EXISTS(SELECT 1 FROM chat_moderators '
                'WHERE chat_id = $1 AND user_id = $2 AND activated = TRUE)',
                chat_id, user_id
            )

    async def add_user(self, user_id: int, username: str, full_name: str) -> None:
        """Добавляет пользователя."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO users (user_id, username, full_name) '
                'VALUES ($1, $2, $3) '
                'ON CONFLICT (user_id) DO UPDATE '
                'SET username = $2, full_name = $3',
                user_id, username, full_name
            )

    async def get_user(self, user_id: int) -> Dict:
        """Возвращает информацию о пользователе."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT user_id, username, full_name '
                'FROM users '
                'WHERE user_id = $1',
                user_id
            )
            if not row:
                return None
            return {
                'user_id': row['user_id'],
                'username': row['username'],
                'full_name': row['full_name']
            }

    async def get_user_violations(self, user_id: int, offset: int, limit: int) -> List[Dict]:
        """Возвращает список нарушений пользователя с пейджингом."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT rv.id, rv.detected_at, '
                'r.rule_text, r.type as rule_type, '
                'c.title as chat_title, '
                'vm.text as message_text, vm.timestamp as message_time, '
                'rvd.decision, rvd.timestamp as decision_time, '
                'm.username as moderator_username, m.full_name as moderator_name '
                'FROM rule_violations rv '
                'JOIN rules r ON rv.rule_id = r.id '
                'JOIN chats c ON r.chat_id = c.id '
                'JOIN violator_messages vm ON rv.violator_msg_id = vm.id '
                'LEFT JOIN rule_violation_decision rvd ON rv.id = rvd.rule_violation_id '
                'LEFT JOIN users m ON rvd.moderator_id = m.user_id '
                'WHERE vm.violator_id = $1 '
                'ORDER BY rv.detected_at DESC '
                'LIMIT $2 OFFSET $3',
                user_id, limit, offset
            )
            return [{
                'id': r['id'],
                'detected_at': r['detected_at'],
                'rule_text': r['rule_text'],
                'rule_type': r['rule_type'],
                'chat_title': r['chat_title'],
                'message_text': r['message_text'],
                'message_time': r['message_time'],
                'decision': r['decision'],
                'decision_time': r['decision_time'],
                'moderator_username': r['moderator_username'],
                'moderator_name': r['moderator_name']
            } for r in rows]

    async def get_user_violations_count(self, user_id: int) -> int:
        """Возвращает количество нарушений пользователя."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT COUNT(*) '
                'FROM rule_violations rv '
                'JOIN violator_messages vm ON rv.violator_msg_id = vm.id '
                'WHERE vm.violator_id = $1',
                user_id
            )

    async def get_chat_admins(self, chat_id: int) -> List[Dict]:
        """Возвращает список администраторов чата."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT ca.chat_id, ca.user_id, ca.activated, u.username, u.full_name '
                'FROM chat_admins ca '
                'JOIN users u ON ca.user_id = u.user_id '
                'WHERE ca.chat_id = $1 '
                'ORDER BY u.username',
                chat_id
            )
            return [{
                'chat_id': r['chat_id'],
                'user_id': r['user_id'],
                'activated': r['activated'],
                'username': r['username'],
                'full_name': r['full_name']
            } for r in rows]

    async def get_user_admin_chats(self, user_id: int) -> List[Dict]:
        """Возвращает список чатов, где пользователь является администратором."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT c.id, c.title, c.activated, '
                'ca.activated as admin_activated '
                'FROM chats c '
                'JOIN chat_admins ca ON c.id = ca.chat_id '
                'WHERE ca.user_id = $1 AND c.activated = TRUE '
                'ORDER BY c.title',
                user_id
            )
            return [{
                'id': r['id'],
                'title': r['title'],
                'activated': r['activated'],
                'admin_activated': r['admin_activated']
            } for r in rows]

    async def is_chat_admin(self, chat_id: int, user_id: int) -> bool:
        """Проверяет, является ли пользователь активным администратором чата."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT EXISTS(SELECT 1 FROM chat_admins '
                'WHERE chat_id = $1 AND user_id = $2 AND activated = TRUE)',
                chat_id, user_id
            )

    async def update_bot_rights(self, chat_id: int, can_read_messages: bool, can_restrict_members: bool, is_bot_in: bool) -> None:
        """Обновляет права бота в чате."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'UPDATE chats '
                'SET can_read_messages = $2, can_restrict_members = $3, is_bot_in = $4 '
                'WHERE id = $1',
                chat_id, can_read_messages, can_restrict_members, is_bot_in
            )

    async def get_bot_rights(self, chat_id: int) -> Dict:
        """Возвращает права бота в чате."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT can_read_messages, can_restrict_members, is_bot_in '
                'FROM chats '
                'WHERE id = $1',
                chat_id
            )
            if not row:
                return None
            return {
                'can_read_messages': row['can_read_messages'],
                'can_restrict_members': row['can_restrict_members'],
                'is_bot_in': row['is_bot_in']
            }

    async def add_sysadmin(self, user_id: int) -> None:
        """Добавляет системного администратора."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO sysadmins (user_id) VALUES ($1) '
                'ON CONFLICT (user_id) DO NOTHING',
                user_id
            )

    async def remove_sysadmin(self, user_id: int) -> None:
        """Удаляет системного администратора."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'DELETE FROM sysadmins WHERE user_id = $1',
                user_id
            )

    async def is_sysadmin(self, user_id: int) -> bool:
        """Проверяет, является ли пользователь системным администратором."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT EXISTS(SELECT 1 FROM sysadmins WHERE user_id = $1)',
                user_id
            )

    async def get_sysadmins(self) -> List[Dict]:
        """Возвращает список системных администраторов."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT s.user_id, u.username, u.full_name '
                'FROM sysadmins s '
                'JOIN users u ON s.user_id = u.user_id '
                'ORDER BY u.username'
            )
            return [{
                'user_id': r['user_id'],
                'username': r['username'],
                'full_name': r['full_name']
            } for r in rows]

    async def add_to_queue(self, chat_id: int, user_id: int, message_id: int, rule_id: int, detected_at: datetime) -> int:
        """Добавляет нарушение в очередь на модерацию."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'INSERT INTO moderation_queue (chat_id, user_id, message_id, rule_id, detected_at) '
                'VALUES ($1, $2, $3, $4, $5) RETURNING id',
                chat_id, user_id, message_id, rule_id, detected_at
            )

    async def get_queue_item(self, queue_id: int) -> Dict:
        """Возвращает элемент очереди."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT mq.id, mq.chat_id, mq.user_id, mq.message_id, mq.rule_id, mq.detected_at, '
                'c.title as chat_title, '
                'u.username, u.full_name, '
                'r.rule_text, r.type as rule_type '
                'FROM moderation_queue mq '
                'JOIN chats c ON mq.chat_id = c.id '
                'JOIN users u ON mq.user_id = u.user_id '
                'JOIN rules r ON mq.rule_id = r.id '
                'WHERE mq.id = $1',
                queue_id
            )
            if not row:
                return None
            return {
                'id': row['id'],
                'chat_id': row['chat_id'],
                'chat_title': row['chat_title'],
                'user_id': row['user_id'],
                'username': row['username'],
                'full_name': row['full_name'],
                'message_id': row['message_id'],
                'rule_id': row['rule_id'],
                'rule_text': row['rule_text'],
                'rule_type': row['rule_type'],
                'detected_at': row['detected_at']
            }

    async def get_queue_items(self, chat_id: int, offset: int, limit: int) -> List[Dict]:
        """Возвращает список элементов очереди с пейджингом."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT mq.id, mq.chat_id, mq.user_id, mq.message_id, mq.rule_id, mq.detected_at, '
                'c.title as chat_title, '
                'u.username, u.full_name, '
                'r.rule_text, r.type as rule_type '
                'FROM moderation_queue mq '
                'JOIN chats c ON mq.chat_id = c.id '
                'JOIN users u ON mq.user_id = u.user_id '
                'JOIN rules r ON mq.rule_id = r.id '
                'WHERE mq.chat_id = $1 '
                'ORDER BY mq.detected_at DESC '
                'LIMIT $2 OFFSET $3',
                chat_id, limit, offset
            )
            return [{
                'id': r['id'],
                'chat_id': r['chat_id'],
                'chat_title': r['chat_title'],
                'user_id': r['user_id'],
                'username': r['username'],
                'full_name': r['full_name'],
                'message_id': r['message_id'],
                'rule_id': r['rule_id'],
                'rule_text': r['rule_text'],
                'rule_type': r['rule_type'],
                'detected_at': r['detected_at']
            } for r in rows]

    async def get_queue_items_count(self, chat_id: int) -> int:
        """Возвращает количество элементов в очереди."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT COUNT(*) FROM moderation_queue WHERE chat_id = $1',
                chat_id
            )

    async def remove_from_queue(self, queue_id: int) -> None:
        """Удаляет элемент из очереди."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'DELETE FROM moderation_queue WHERE id = $1',
                queue_id
            )

    async def add_notification(self, user_id: int, chat_id: int, message: str, created_at: datetime) -> int:
        """Добавляет уведомление для пользователя."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'INSERT INTO notifications (user_id, chat_id, message, created_at) '
                'VALUES ($1, $2, $3, $4) RETURNING id',
                user_id, chat_id, message, created_at
            )

    async def get_user_notifications(self, user_id: int, offset: int, limit: int) -> List[Dict]:
        """Возвращает список уведомлений пользователя с пейджингом."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT n.id, n.chat_id, n.message, n.created_at, n.read_at, '
                'c.title as chat_title '
                'FROM notifications n '
                'JOIN chats c ON n.chat_id = c.id '
                'WHERE n.user_id = $1 '
                'ORDER BY n.created_at DESC '
                'LIMIT $2 OFFSET $3',
                user_id, limit, offset
            )
            return [{
                'id': r['id'],
                'chat_id': r['chat_id'],
                'chat_title': r['chat_title'],
                'message': r['message'],
                'created_at': r['created_at'],
                'read_at': r['read_at']
            } for r in rows]

    async def get_user_notifications_count(self, user_id: int) -> int:
        """Возвращает количество уведомлений пользователя."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT COUNT(*) FROM notifications WHERE user_id = $1',
                user_id
            )

    async def mark_notification_as_read(self, notification_id: int) -> None:
        """Отмечает уведомление как прочитанное."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'UPDATE notifications SET read_at = NOW() WHERE id = $1',
                notification_id
            )

    async def mark_all_notifications_as_read(self, user_id: int) -> None:
        """Отмечает все уведомления пользователя как прочитанные."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'UPDATE notifications SET read_at = NOW() WHERE user_id = $1 AND read_at IS NULL',
                user_id
            )

    async def delete_notification(self, notification_id: int) -> None:
        """Удаляет уведомление."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'DELETE FROM notifications WHERE id = $1',
                notification_id
            )

    async def delete_all_notifications(self, user_id: int) -> None:
        """Удаляет все уведомления пользователя."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'DELETE FROM notifications WHERE user_id = $1',
                user_id
            )

    async def get_settings(self, chat_id: int) -> Dict:
        """Возвращает настройки чата."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT page_size '
                'FROM settings '
                'WHERE chat_id = $1',
                chat_id
            )
            if not row:
                return None
            return {
                'page_size': row['page_size']
            }

    async def update_settings(self, chat_id: int, page_size: int) -> None:
        """Обновляет настройки чата."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO settings (chat_id, page_size) '
                'VALUES ($1, $2) '
                'ON CONFLICT (chat_id) DO UPDATE '
                'SET page_size = $2',
                chat_id, page_size
            )

    async def add_log(self, chat_id: int, user_id: int, action: str, details: str, created_at: datetime) -> int:
        """Добавляет запись в лог."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'INSERT INTO logs (chat_id, user_id, action, details, created_at) '
                'VALUES ($1, $2, $3, $4, $5) RETURNING id',
                chat_id, user_id, action, details, created_at
            )

    async def get_chat_logs(self, chat_id: int, offset: int, limit: int) -> List[Dict]:
        """Возвращает список записей лога чата с пейджингом."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT l.id, l.user_id, l.action, l.details, l.created_at, '
                'u.username, u.full_name '
                'FROM logs l '
                'JOIN users u ON l.user_id = u.user_id '
                'WHERE l.chat_id = $1 '
                'ORDER BY l.created_at DESC '
                'LIMIT $2 OFFSET $3',
                chat_id, limit, offset
            )
            return [{
                'id': r['id'],
                'user_id': r['user_id'],
                'username': r['username'],
                'full_name': r['full_name'],
                'action': r['action'],
                'details': r['details'],
                'created_at': r['created_at']
            } for r in rows]

    async def get_chat_logs_count(self, chat_id: int) -> int:
        """Возвращает количество записей в логе чата."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT COUNT(*) FROM logs WHERE chat_id = $1',
                chat_id
            )

    async def get_user_logs(self, user_id: int, offset: int, limit: int) -> List[Dict]:
        """Возвращает список записей лога пользователя с пейджингом."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT l.id, l.chat_id, l.action, l.details, l.created_at, '
                'c.title as chat_title '
                'FROM logs l '
                'JOIN chats c ON l.chat_id = c.id '
                'WHERE l.user_id = $1 '
                'ORDER BY l.created_at DESC '
                'LIMIT $2 OFFSET $3',
                user_id, limit, offset
            )
            return [{
                'id': r['id'],
                'chat_id': r['chat_id'],
                'chat_title': r['chat_title'],
                'action': r['action'],
                'details': r['details'],
                'created_at': r['created_at']
            } for r in rows]

    async def get_user_logs_count(self, user_id: int) -> int:
        """Возвращает количество записей в логе пользователя."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT COUNT(*) FROM logs WHERE user_id = $1',
                user_id
            )

    async def add_tag(self, name: str) -> int:
        """Добавляет тег."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'INSERT INTO tags (name) VALUES ($1) RETURNING id',
                name
            )

    async def add_rule_tag(self, rule_id: int, tag_id: int) -> None:
        """Добавляет тег к правилу."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO rule_tags (rule_id, tag_id) VALUES ($1, $2) '
                'ON CONFLICT (rule_id, tag_id) DO NOTHING',
                rule_id, tag_id
            )

    async def remove_rule_tag(self, rule_id: int, tag_id: int) -> None:
        """Удаляет тег у правила."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'DELETE FROM rule_tags WHERE rule_id = $1 AND tag_id = $2',
                rule_id, tag_id
            )

    async def get_rule_tags(self, rule_id: int) -> List[Dict]:
        """Возвращает список тегов правила."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT t.id, t.name '
                'FROM tags t '
                'JOIN rule_tags rt ON t.id = rt.tag_id '
                'WHERE rt.rule_id = $1 '
                'ORDER BY t.name',
                rule_id
            )
            return [{
                'id': r['id'],
                'name': r['name']
            } for r in rows]

    async def get_tag_rules(self, tag_id: int) -> List[Dict]:
        """Возвращает список правил с тегом."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT r.id, r.rule_text, r.explanation_text, r.type, r.activated, '
                'c.title as chat_title '
                'FROM rules r '
                'JOIN rule_tags rt ON r.id = rt.rule_id '
                'JOIN chats c ON r.chat_id = c.id '
                'WHERE rt.tag_id = $1 AND r.activated = TRUE '
                'ORDER BY c.title, r.type, r.id',
                tag_id
            )
            return [{
                'id': r['id'],
                'rule_text': r['rule_text'],
                'explanation_text': r['explanation_text'],
                'type': r['type'],
                'activated': r['activated'],
                'chat_title': r['chat_title']
            } for r in rows]

    async def get_all_tags(self) -> List[Dict]:
        """Возвращает список всех тегов."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT t.id, t.name, COUNT(rt.rule_id) as rules_count '
                'FROM tags t '
                'LEFT JOIN rule_tags rt ON t.id = rt.tag_id '
                'GROUP BY t.id '
                'ORDER BY t.name'
            )
            return [{
                'id': r['id'],
                'name': r['name'],
                'rules_count': r['rules_count']
            } for r in rows]

    async def search_tags(self, search_term: str) -> List[Dict]:
        """Поиск тегов по названию."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT t.id, t.name, COUNT(rt.rule_id) as rules_count '
                'FROM tags t '
                'LEFT JOIN rule_tags rt ON t.id = rt.tag_id '
                'WHERE t.name ILIKE $1 '
                'GROUP BY t.id '
                'ORDER BY t.name',
                f'%{search_term}%'
            )
            return [{
                'id': r['id'],
                'name': r['name'],
                'rules_count': r['rules_count']
            } for r in rows]

    async def add_template(self, chat_id: int, name: str, text: str) -> int:
        """Добавляет шаблон."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'INSERT INTO templates (chat_id, name, text) '
                'VALUES ($1, $2, $3) RETURNING id',
                chat_id, name, text
            )

    async def update_template(self, template_id: int, name: str, text: str) -> None:
        """Обновляет шаблон."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'UPDATE templates SET name = $2, text = $3 WHERE id = $1',
                template_id, name, text
            )

    async def delete_template(self, template_id: int) -> None:
        """Удаляет шаблон."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'DELETE FROM templates WHERE id = $1',
                template_id
            )

    async def get_template(self, template_id: int) -> Dict:
        """Возвращает информацию о шаблоне."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT t.id, t.chat_id, t.name, t.text, '
                'c.title as chat_title '
                'FROM templates t '
                'JOIN chats c ON t.chat_id = c.id '
                'WHERE t.id = $1',
                template_id
            )
            if not row:
                return None
            return {
                'id': row['id'],
                'chat_id': row['chat_id'],
                'chat_title': row['chat_title'],
                'name': row['name'],
                'text': row['text']
            }

    async def get_chat_templates(self, chat_id: int) -> List[Dict]:
        """Возвращает список шаблонов чата."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT id, name, text '
                'FROM templates '
                'WHERE chat_id = $1 '
                'ORDER BY name',
                chat_id
            )
            return [{
                'id': r['id'],
                'name': r['name'],
                'text': r['text']
            } for r in rows]

    async def search_templates(self, chat_id: int, search_term: str) -> List[Dict]:
        """Поиск шаблонов по названию или тексту."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT id, name, text '
                'FROM templates '
                'WHERE chat_id = $1 AND (name ILIKE $2 OR text ILIKE $2) '
                'ORDER BY name',
                chat_id, f'%{search_term}%'
            )
            return [{
                'id': r['id'],
                'name': r['name'],
                'text': r['text']
            } for r in rows]

    async def add_prompt(self, chat_id: int, name: str, text: str, prompt_type: str) -> int:
        """Добавляет промпт."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'INSERT INTO prompts (chat_id, name, text, type) '
                'VALUES ($1, $2, $3, $4) RETURNING id',
                chat_id, name, text, prompt_type
            )

    async def update_prompt(self, prompt_id: int, name: str, text: str, prompt_type: str) -> None:
        """Обновляет промпт."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'UPDATE prompts SET name = $2, text = $3, type = $4 WHERE id = $1',
                prompt_id, name, text, prompt_type
            )

    async def delete_prompt(self, prompt_id: int) -> None:
        """Удаляет промпт."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'DELETE FROM prompts WHERE id = $1',
                prompt_id
            )

    async def get_prompt(self, prompt_id: int) -> Dict:
        """Возвращает информацию о промпте."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT p.id, p.chat_id, p.name, p.text, p.type, '
                'c.title as chat_title '
                'FROM prompts p '
                'JOIN chats c ON p.chat_id = c.id '
                'WHERE p.id = $1',
                prompt_id
            )
            if not row:
                return None
            return {
                'id': row['id'],
                'chat_id': row['chat_id'],
                'chat_title': row['chat_title'],
                'name': row['name'],
                'text': row['text'],
                'type': row['type']
            }

    async def get_chat_prompts(self, chat_id: int, prompt_type: str = None) -> List[Dict]:
        """Возвращает список промптов чата."""
        async with self.pool.acquire() as conn:
            if prompt_type:
                rows = await conn.fetch(
                    'SELECT id, name, text, type '
                    'FROM prompts '
                    'WHERE chat_id = $1 AND type = $2 '
                    'ORDER BY name',
                    chat_id, prompt_type
                )
            else:
                rows = await conn.fetch(
                    'SELECT id, name, text, type '
                    'FROM prompts '
                    'WHERE chat_id = $1 '
                    'ORDER BY type, name',
                    chat_id
                )
            return [{
                'id': r['id'],
                'name': r['name'],
                'text': r['text'],
                'type': r['type']
            } for r in rows]

    async def search_prompts(self, chat_id: int, search_term: str, prompt_type: str = None) -> List[Dict]:
        """Поиск промптов по названию или тексту."""
        async with self.pool.acquire() as conn:
            if prompt_type:
                rows = await conn.fetch(
                    'SELECT id, name, text, type '
                    'FROM prompts '
                    'WHERE chat_id = $1 AND type = $2 AND (name ILIKE $3 OR text ILIKE $3) '
                    'ORDER BY name',
                    chat_id, prompt_type, f'%{search_term}%'
                )
            else:
                rows = await conn.fetch(
                    'SELECT id, name, text, type '
                    'FROM prompts '
                    'WHERE chat_id = $1 AND (name ILIKE $2 OR text ILIKE $2) '
                    'ORDER BY type, name',
                    chat_id, f'%{search_term}%'
                )
            return [{
                'id': r['id'],
                'name': r['name'],
                'text': r['text'],
                'type': r['type']
            } for r in rows]

    # --- Rule Violations ---
    async def get_rule_violation(self, violation_id: int) -> Dict:
        """Получает информацию о нарушении правила."""
        query = """
            SELECT 
                rv.id,
                rv.rule_id,
                rv.violator_msg_id,
                rv.detected_at,
                r.rule_text,
                r.type as rule_type,
                r.chat_id,
                vm.violator_id,
                vm.text as message_text,
                vm.timestamp as message_timestamp,
                u.username as violator_username,
                u.full_name as violator_full_name,
                c.title as chat_title
            FROM rule_violations rv
            JOIN rules r ON rv.rule_id = r.id
            JOIN violator_messages vm ON rv.violator_msg_id = vm.id
            JOIN users u ON vm.violator_id = u.user_id
            JOIN chats c ON r.chat_id = c.id
            WHERE rv.id = $1
        """
        row = await self.pool.fetchrow(query, violation_id)
        if not row:
            return None
        return dict(row)

    async def get_chat_violations(self, chat_id: int, status: str = None, offset: int = 0, limit: int = 20) -> List[Dict]:
        """Получить список нарушений в чате с фильтрацией по статусу и пагинацией."""
        async with self.pool.acquire() as conn:
            if status:
                rows = await conn.fetch(
                    'SELECT rv.id, rv.violator_msg_id, rv.rule_id, rv.detected_at, rv.status, '
                    'u.username as violator_username, u.full_name as violator_name, '
                    'r.rule_text, r.type as rule_type '
                    'FROM rule_violations rv '
                    'JOIN rules r ON rv.rule_id = r.id '
                    'JOIN violator_messages vm ON rv.violator_msg_id = vm.id '
                    'JOIN users u ON vm.violator_id = u.user_id '
                    'WHERE r.chat_id = $1 AND rv.status = $2 '
                    'ORDER BY rv.detected_at DESC LIMIT $3 OFFSET $4',
                    chat_id, status, limit, offset
                )
            else:
                rows = await conn.fetch(
                    'SELECT rv.id, rv.violator_msg_id, rv.rule_id, rv.detected_at, rv.status, '
                    'u.username as violator_username, u.full_name as violator_name, '
                    'r.rule_text, r.type as rule_type '
                    'FROM rule_violations rv '
                    'JOIN rules r ON rv.rule_id = r.id '
                    'JOIN violator_messages vm ON rv.violator_msg_id = vm.id '
                    'JOIN users u ON vm.violator_id = u.user_id '
                    'WHERE r.chat_id = $1 '
                    'ORDER BY rv.detected_at DESC LIMIT $2 OFFSET $3',
                    chat_id, limit, offset
                )
            return [dict(r) for r in rows]

    async def get_user_violations(self, user_id: int, offset: int = 0, limit: int = 20) -> List[Dict]:
        """Получить список нарушений пользователя с пагинацией."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT rv.id, rv.violator_msg_id, rv.rule_id, rv.detected_at, rv.status, '
                'r.rule_text, r.type as rule_type, c.title as chat_title '
                'FROM rule_violations rv '
                'JOIN rules r ON rv.rule_id = r.id '
                'JOIN chats c ON r.chat_id = c.id '
                'JOIN violator_messages vm ON rv.violator_msg_id = vm.id '
                'WHERE vm.violator_id = $1 '
                'ORDER BY rv.detected_at DESC LIMIT $2 OFFSET $3',
                user_id, limit, offset
            )
            return [dict(r) for r in rows]

    async def update_rule_violation_status(self, violation_id: int, status: str) -> None:
        """Обновить статус нарушения."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'UPDATE rule_violations SET status = $2 WHERE id = $1',
                violation_id, status
            )

    async def delete_rule_violation(self, violation_id: int) -> None:
        """Удалить нарушение."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'DELETE FROM rule_violations WHERE id = $1',
                violation_id
            )

    # --- Violator Messages ---
    async def get_violator_message(self, message_id: int) -> Dict:
        """Возвращает информацию о сообщении нарушителя."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                '''
                SELECT vm.id, vm.text, vm.timestamp, vm.post_id,
                       u.username as violator_username,
                       u.full_name as violator_name,
                       rv.id as violation_id,
                       r.chat_id,
                       c.title as chat_title
                FROM violator_messages vm
                JOIN users u ON vm.violator_id = u.user_id
                JOIN rule_violations rv ON rv.violator_msg_id = vm.id
                JOIN rules r ON rv.rule_id = r.id
                JOIN chats c ON r.chat_id = c.id
                WHERE vm.id = $1
                ''',
                message_id
            )
            if not row:
                return None
            return {
                'id': row['id'],
                'text': row['text'],
                'timestamp': row['timestamp'],
                'post_id': row['post_id'],
                'violator_username': row['violator_username'],
                'violator_name': row['violator_name'],
                'violation_id': row['violation_id'],
                'chat_id': row['chat_id'],
                'chat_title': row['chat_title']
            }

    async def get_chat_violator_messages(self, chat_id: int, offset: int = 0, limit: int = 20) -> List[Dict]:
        """Получить список сообщений нарушителей в чате с пагинацией."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT vm.id, vm.violator_id, u.username as violator_username, u.full_name as violator_name, '
                'vm.text, vm.timestamp '
                'FROM violator_messages vm '
                'JOIN users u ON vm.violator_id = u.user_id '
                'WHERE vm.chat_id = $1 '
                'ORDER BY vm.timestamp DESC LIMIT $2 OFFSET $3',
                chat_id, limit, offset
            )
            return [dict(r) for r in rows]

    async def get_user_violator_messages(self, user_id: int, offset: int = 0, limit: int = 20) -> List[Dict]:
        """Получить список сообщений нарушителя с пагинацией."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT vm.id, vm.violator_id, vm.text, vm.timestamp, vm.chat_id, c.title as chat_title '
                'FROM violator_messages vm '
                'JOIN chats c ON vm.chat_id = c.id '
                'WHERE vm.violator_id = $1 '
                'ORDER BY vm.timestamp DESC LIMIT $2 OFFSET $3',
                user_id, limit, offset
            )
            return [dict(r) for r in rows]

    async def delete_violator_message(self, message_id: int) -> None:
        """Удалить сообщение нарушителя."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'DELETE FROM violator_messages WHERE id = $1',
                message_id
            )

    async def get_notification_policies_for_moderator(self, moderator_id: int) -> List[Dict]:
        """Возвращает список политик уведомлений для модератора."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT policy FROM rule_violation_notification_policies WHERE moderator_id = $1',
                moderator_id
            )
            return [{'policy': r['policy'], 'enabled': True} for r in rows]

    async def add_notification_policy(self, moderator_id: int, policy: str) -> None:
        """Включает политику уведомлений для модератора."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO rule_violation_notification_policies (moderator_id, policy) VALUES ($1, $2) ON CONFLICT DO NOTHING',
                moderator_id, policy
            )

    async def remove_notification_policy(self, moderator_id: int, policy: str) -> None:
        """Отключает политику уведомлений для модератора."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                'DELETE FROM rule_violation_notification_policies WHERE moderator_id = $1 AND policy = $2',
                moderator_id, policy
            )

    async def get_notification_policy_status(self, moderator_id: int, policy_type: str) -> bool:
        """Возвращает True, если политика типа (BAN/NOTIFICATION) включена (NOTIFY_*), иначе False. Если записи нет — True."""
        notify_policy = f'NOTIFY_{policy_type}'
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT 1 FROM rule_violation_notification_policies WHERE moderator_id = $1 AND (policy = $2 OR policy = $3)',
                moderator_id, notify_policy, f'NOT_NOTIFY_{policy_type}'
            )
            if row is None:
                return True  # По умолчанию включено
            # Проверяем, какая именно политика стоит
            row_notify = await conn.fetchrow(
                'SELECT 1 FROM rule_violation_notification_policies WHERE moderator_id = $1 AND policy = $2',
                moderator_id, notify_policy
            )
            return bool(row_notify)

    async def set_notification_policy_status(self, moderator_id: int, policy_type: str, enabled: bool) -> None:
        """Включает или выключает политику типа (BAN/NOTIFICATION) для модератора."""
        notify_policy = f'NOTIFY_{policy_type}'
        not_notify_policy = f'NOT_NOTIFY_{policy_type}'
        async with self.pool.acquire() as conn:
            # Удаляем обе политики
            await conn.execute(
                'DELETE FROM rule_violation_notification_policies WHERE moderator_id = $1 AND (policy = $2 OR policy = $3)',
                moderator_id, notify_policy, not_notify_policy
            )
            # Вставляем только нужную
            policy = notify_policy if enabled else not_notify_policy
            await conn.execute(
                'INSERT INTO rule_violation_notification_policies (moderator_id, policy) VALUES ($1, $2)',
                moderator_id, policy
            )

    async def get_new_violations_count(self, rule_id: int, since: datetime) -> int:
        """Возвращает количество новых нарушений по правилу с момента since."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT COUNT(*) FROM rule_violations WHERE rule_id = $1 AND detected_at > $2',
                rule_id, since
            )

    async def get_new_violations_per_user(self, rule_id: int, since: datetime) -> List[Dict]:
        """Возвращает список новых нарушений правила с момента since."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                '''
                SELECT rv.id, rv.violator_msg_id, rv.detected_at,
                       vm.text as message_text,
                       u.username as violator_username,
                       u.full_name as violator_name,
                       c.id as chat_id,
                       c.title as chat_title
                FROM rule_violations rv
                JOIN violator_messages vm ON rv.violator_msg_id = vm.id
                JOIN users u ON vm.violator_id = u.user_id
                JOIN rules r ON rv.rule_id = r.id
                JOIN chats c ON r.chat_id = c.id
                WHERE rv.rule_id = $1 AND rv.detected_at > $2
                ORDER BY rv.detected_at DESC
                ''',
                rule_id, since
            )
            return [{
                'id': r['id'],
                'violator_msg_id': r['violator_msg_id'],
                'detected_at': r['detected_at'],
                'message_text': r['message_text'],
                'violator_username': r['violator_username'],
                'violator_name': r['violator_name'],
                'chat_id': r['chat_id'],
                'chat_title': r['chat_title']
            } for r in rows]

    async def update_rule(self, rule_id: int, rule_text: str, explanation_text: str, rule_type: str) -> None:
        """Обновляет правило."""
        query = """
            UPDATE rules 
            SET rule_text = $1, explanation_text = $2, type = $3
            WHERE id = $4
        """
        await self.pool.execute(query, rule_text, explanation_text, rule_type, rule_id)

    async def get_last_seen(self, moderator_id: int, rule_id: int) -> Optional[datetime]:
        """Возвращает время последнего просмотра правила модератором."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT last_seen_timestamp FROM moderator_rule_last_seen WHERE moderator_id = $1 AND rule_id = $2',
                moderator_id, rule_id
            )
            return row['last_seen_timestamp'] if row else None

    async def set_last_seen(self, moderator_id: int, rule_id: int, timestamp: datetime) -> None:
        """Устанавливает время последнего просмотра правила модератором.
        Если записи нет - создает новую, если есть - обновляет существующую."""
        async with self.pool.acquire() as conn:
            # Проверяем существование записи
            exists = await conn.fetchval(
                'SELECT 1 FROM moderator_rule_last_seen WHERE moderator_id = $1 AND rule_id = $2',
                moderator_id, rule_id
            )
            
            if exists:
                # Обновляем существующую запись
                await conn.execute(
                    'UPDATE moderator_rule_last_seen SET last_seen_timestamp = $3 WHERE moderator_id = $1 AND rule_id = $2',
                    moderator_id, rule_id, timestamp
                )
            else:
                # Создаем новую запись
                await conn.execute(
                    'INSERT INTO moderator_rule_last_seen (moderator_id, rule_id, last_seen_timestamp) VALUES ($1, $2, $3)',
                    moderator_id, rule_id, timestamp
                )

    async def store_image(self, image_data: bytes) -> str:
        """Stores an image in the database and returns its UUID."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'INSERT INTO message_images (id, image_data) VALUES (gen_random_uuid(), $1) RETURNING id',
                image_data
            )

    async def store_audio(self, audio_data: bytes) -> str:
        """Stores an audio file in the database and returns its UUID."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'INSERT INTO message_audios (id, audio_data) VALUES (gen_random_uuid(), $1) RETURNING id',
                audio_data
            )

    async def get_image(self, image_id: str) -> bytes:
        """Retrieves an image from the database by its UUID."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT image_data FROM message_images WHERE id = $1',
                image_id
            )

    async def get_audio(self, audio_id: str) -> bytes:
        """Retrieves an audio file from the database by its UUID."""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                'SELECT audio_data FROM message_audios WHERE id = $1',
                audio_id
            )
  
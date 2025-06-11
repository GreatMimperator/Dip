import asyncio
import json
import aio_pika
import logging
import yaml
import os
import asyncpg
from typing import Dict, Any
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RuleDecider:
    def __init__(self):
        self.config = self.load_config()
        self.db = None
        self.rabbitmq = None
        self.bot = None

    def load_config(self) -> Dict[str, Any]:
        """Загрузка конфигурации из config.yaml"""
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config.yaml')
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise

    async def init_db(self):
        """Инициализация подключения к базе данных"""
        try:
            self.db = await asyncpg.create_pool(
                host=self.config['postgres']['host'],
                port=self.config['postgres']['port'],
                user=self.config['postgres']['user'],
                password=self.config['postgres']['password'],
                database=self.config['postgres']['db']
            )
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    async def init_rabbitmq(self):
        """Инициализация подключения к RabbitMQ"""
        try:
            self.rabbitmq = await aio_pika.connect_robust(
                host=self.config['queue']['host'],
                port=self.config['queue']['port'],
                login=self.config['queue']['username'],
                password=self.config['queue']['password'],
                virtualhost=self.config['queue']['vhost']
            )
            logger.info("RabbitMQ connection established")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    async def init_bot(self):
        """Инициализация бота"""
        try:
            from aiogram import Bot
            self.bot = Bot(token=self.config['telegram']['bot_token'])
            logger.info("Bot initialized")
        except Exception as e:
            logger.error(f"Failed to initialize bot: {e}")
            raise

    async def get_rule_info(self, rule_id: int) -> Dict[str, Any]:
        """Получение информации о правиле"""
        try:
            async with self.db.acquire() as conn:
                rule = await conn.fetchrow(
                    """
                    SELECT r.*, c.id as chat_id, c.title as chat_title
                    FROM rules r
                    JOIN chats c ON r.chat_id = c.id
                    WHERE r.id = $1
                    """,
                    rule_id
                )
                if not rule:
                    logger.error(f"Rule {rule_id} not found")
                    return None
                return dict(rule)
        except Exception as e:
            logger.error(f"Failed to get rule info: {e}")
            return None

    async def get_moderators_for_chat(self, chat_id: int) -> list:
        """Получение списка модераторов для чата"""
        try:
            async with self.db.acquire() as conn:
                moderators = await conn.fetch(
                    """
                    SELECT DISTINCT u.user_id, u.username, u.full_name
                    FROM users u
                    JOIN chat_moderators cm ON u.user_id = cm.user_id
                    WHERE cm.chat_id = $1 AND cm.activated = TRUE
                    """,
                    chat_id
                )
                return [dict(m) for m in moderators]
        except Exception as e:
            logger.error(f"Failed to get moderators: {e}")
            return []

    async def get_notification_policy(self, user_id: int) -> bool:
        """Получение политики уведомлений модератора"""
        try:
            async with self.db.acquire() as conn:
                policy = await conn.fetchval(
                    """
                    SELECT policy 
                    FROM rule_violation_notification_policies 
                    WHERE moderator_id = $1
                    """,
                    user_id
                )
                # Если политика не найдена, считаем что уведомления включены
                return policy is None or policy in ('NOTIFY_BAN', 'NOTIFY_NOTIFICATION')
        except Exception as e:
            logger.error(f"Failed to get notification policy: {e}")
            return True

    async def process_rule_match(self, message: aio_pika.IncomingMessage):
        """Обработка совпадения правила"""
        try:
            async with message.process():
                data = json.loads(message.body.decode())
                message_id = data['message_id']
                rule_id = data['rule_id']
                
                logger.info(f"Processing rule match: message_id={message_id}, rule_id={rule_id}")
                
                # Получаем информацию о правиле
                rule = await self.get_rule_info(rule_id)
                if not rule:
                    logger.error(f"Rule {rule_id} not found")
                    return
                
                # Получаем список модераторов
                moderators = await self.get_moderators_for_chat(rule['chat_id'])
                if not moderators:
                    logger.warning(f"No moderators found for chat {rule['chat_id']}")
                    return
                
                logger.info(f"Found {len(moderators)} moderators for chat {rule['chat_id']}")
                
                # Уведомляем модераторов
                for moderator in moderators:
                    try:
                        # Проверяем настройки уведомлений модератора
                        should_notify = await self.get_notification_policy(moderator['user_id'])
                        logger.info(f"Moderator {moderator['user_id']} notification policy: {should_notify}")
                        
                        if not should_notify:
                            logger.info(f"Skipping notification for moderator {moderator['user_id']} due to notification policy")
                            continue
                        
                        # Отправляем информацию о правиле и кнопки действий
                        keyboard = []
                        if rule['type'] == 'NOTIFY':
                            keyboard.append([InlineKeyboardButton(text="Забанить", callback_data=f"violation_action:{message_id}:BAN")])
                        else:
                            keyboard.append([InlineKeyboardButton(text="Разбанить", callback_data=f"violation_action:{message_id}:UNBAN")])
                        
                        markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
                        
                        # Пересылаем сообщение
                        try:
                            await self.bot.forward_message(
                                from_chat_id=rule['chat_id'],
                                chat_id=moderator['user_id'],
                                message_id=message_id
                            )
                            logger.info(f"Successfully forwarded message {message_id} to moderator {moderator['user_id']}")
                        except Exception as e:
                            logger.error(f"Failed to forward message to moderator {moderator['user_id']}: {e}")
                        
                        # Отправляем информацию о правиле
                        await self.bot.send_message(
                            moderator['user_id'],
                            f"⚠️ Нарушение правила в чате {rule['chat_title']}:\n\n"
                            f"Правило: {rule['rule_text']}\n"
                            f"Тип: {rule['type']}",
                            reply_markup=markup
                        )
                        logger.info(f"Successfully sent notification to moderator {moderator['user_id']}")
                        
                        # Если правило типа BAN, удаляем сообщение и баним пользователя
                        if rule['type'] == 'BAN':
                            try:
                                # Удаляем сообщение
                                await self.bot.delete_message(
                                    chat_id=rule['chat_id'],
                                    message_id=message_id
                                )
                                
                                # Баним пользователя
                                await self.bot.ban_chat_member(
                                    chat_id=rule['chat_id'],
                                    user_id=data['user_id']
                                )
                                
                                logger.info(f"User {data['user_id']} banned and message {message_id} deleted in chat {rule['chat_id']}")
                            except Exception as e:
                                logger.error(f"Failed to ban user or delete message: {e}")
                        
                    except Exception as e:
                        logger.error(f"Failed to send notification to moderator {moderator['user_id']}: {e}")
                
        except Exception as e:
            logger.error(f"Failed to process rule match: {e}")

    async def start(self):
        """Запуск сервиса"""
        try:
            # Инициализация подключений
            await self.init_db()
            await self.init_rabbitmq()
            await self.init_bot()
            
            # Создание канала
            channel = await self.rabbitmq.channel()
            
            # Объявление очереди
            queue = await channel.declare_queue(
                "message-rule-match",
                durable=True
            )
            
            # Начало прослушивания очереди
            await queue.consume(self.process_rule_match)
            
            logger.info("Service started successfully")
            
            # Бесконечный цикл для поддержания работы сервиса
            while True:
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Service failed: {e}")
            raise
        finally:
            if self.db:
                await self.db.close()
            if self.rabbitmq:
                await self.rabbitmq.close()
            if self.bot:
                await self.bot.session.close()

if __name__ == "__main__":
    decider = RuleDecider()
    asyncio.run(decider.start()) 
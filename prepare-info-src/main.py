import asyncio
import json
import aio_pika
from typing import Dict, Any, Set
import logging
import yaml
import os
from collections import defaultdict

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class InfoPreparator:
    def __init__(self):
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.config = self.load_config()
        
        # Хранилище для собранной информации
        self.message_data = defaultdict(lambda: {
            'images': None,
            'transcribed_audio': None,
            'text': None,
            'message_id': None,
            'has_video': False,
            'has_photo': False,
            'has_audio': False,
            'image_uuids': [],
            'audio_uuids': []
        })
        
        # Множества для отслеживания подтверждений
        self.pending_confirmations = defaultdict(set)

    def load_config(self) -> Dict[str, Any]:
        """Загрузка конфигурации из файла."""
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.yaml')
        logger.info(f"Loading configuration from {config_path}")
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info("Configuration loaded successfully")
            return config
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            raise

    async def init_rabbitmq(self):
        """Инициализация подключения к RabbitMQ."""
        logger.info("Connecting to RabbitMQ...")
        queue_config = self.config['queue']
        self.rabbitmq_connection = await aio_pika.connect_robust(
            f"amqp://{queue_config['username']}:{queue_config['password']}@{queue_config['host']}:{queue_config['port']}/{queue_config['vhost']}"
        )
        self.rabbitmq_channel = await self.rabbitmq_connection.channel()
        logger.info("Connected to RabbitMQ successfully")

    async def process_images_message(self, message: aio_pika.IncomingMessage):
        """Обработка сообщения из очереди multimedia.images."""
        try:
            body = message.body.decode()
            data = json.loads(body)
            message_id = data.get('message_id')
            
            logger.info(f"Received images message for message_id: {message_id}")
            
            # Сохраняем данные
            self.message_data[message_id].update({
                'images': data,
                'message_id': message_id,
                'has_video': data.get('has_video', False),
                'has_photo': data.get('has_photo', False),
                'has_audio': data.get('has_audio', False),
                'image_uuids': data.get('image_uuids', []),
                'audio_uuids': data.get('audio_uuids', [])
            })
            
            # Добавляем в ожидающие подтверждения
            self.pending_confirmations[message_id].add('images')
            
            # Проверяем, можно ли отправить собранную информацию
            await self.check_and_send_info(message_id)
            
        except Exception as e:
            logger.error(f"Error processing images message: {str(e)}")
            raise

    async def process_transcribed_audio_message(self, message: aio_pika.IncomingMessage):
        """Обработка сообщения из очереди prompt.transcribed-audio."""
        try:
            body = message.body.decode()
            data = json.loads(body)
            message_id = data.get('message_id')
            
            logger.info(f"Received transcribed audio message for message_id: {message_id}")
            
            # Сохраняем данные
            self.message_data[message_id].update({
                'transcribed_audio': data,
                'message_id': message_id,
                'has_video': data.get('has_video', False),
                'has_photo': data.get('has_photo', False),
                'has_audio': data.get('has_audio', False),
                'image_uuids': data.get('image_uuids', []),
                'audio_uuids': data.get('audio_uuids', [])
            })
            
            # Добавляем в ожидающие подтверждения
            self.pending_confirmations[message_id].add('transcribed_audio')
            
            # Проверяем, можно ли отправить собранную информацию
            await self.check_and_send_info(message_id)
            
        except Exception as e:
            logger.error(f"Error processing transcribed audio message: {str(e)}")
            raise

    async def process_text_message(self, message: aio_pika.IncomingMessage):
        """Обработка сообщения из очереди multimedia.text."""
        try:
            body = message.body.decode()
            data = json.loads(body)
            message_id = data.get('message_id')
            
            logger.info(f"Received text message for message_id: {message_id}")
            
            # Сохраняем данные
            self.message_data[message_id].update({
                'text': data,
                'message_id': message_id,
                'has_video': data.get('has_video', False),
                'has_photo': data.get('has_photo', False),
                'has_audio': data.get('has_audio', False),
                'image_uuids': data.get('image_uuids', []),
                'audio_uuids': data.get('audio_uuids', [])
            })
            
            # Добавляем в ожидающие подтверждения
            self.pending_confirmations[message_id].add('text')
            
            # Проверяем, можно ли отправить собранную информацию
            await self.check_and_send_info(message_id)
            
        except Exception as e:
            logger.error(f"Error processing text message: {str(e)}")
            raise

    async def check_and_send_info(self, message_id: int):
        """Проверяет, собраны ли все данные для сообщения, и отправляет их если да."""
        data = self.message_data[message_id]
        print("Данные: ", data)
        pending = self.pending_confirmations[message_id]
        
        # Проверяем, что у нас есть все необходимые данные
        required_data = set()
        if data['has_video']:
            required_data.add('images')
            required_data.add('transcribed_audio')
        if data['has_photo']:
            required_data.add('images')
        if data['has_audio']:
            required_data.add('transcribed_audio')
        if data['text']:
            required_data.add('text')
            
        logger.info(f"Message {message_id} - Required data: {required_data}, Pending: {pending}")
        
        # Если все необходимые данные собраны
        if required_data.issubset(pending):
            logger.info(f"All required data collected for message {message_id}, sending to prompt.ready_info")
            
            # Формируем итоговое сообщение
            result_message = {
                'message_id': message_id,
                'chat_id': data[next(iter(required_data))]['chat_id'],
                'has_video': data['has_video'],
                'has_photo': data['has_photo'],
                'has_audio': data['has_audio'],
                'image_uuids': data['image_uuids'],
                'audio_uuids': data['audio_uuids'],
                'transcribed_text': data['transcribed_audio']['transcribed_text'] if data['transcribed_audio'] else None,
                'text': data['text']['text'] if data['text'] else None
            }
            print("Отправляем в очередь: ", result_message)
            
            # Отправляем в очередь
            await self.rabbitmq_channel.default_exchange.publish(
                aio_pika.Message(body=json.dumps(result_message).encode()),
                routing_key="prompt.ready_info"
            )
            
            # Очищаем данные с проверкой на существование
            try:
                del self.message_data[message_id]
                logger.info(f"Removed message data for {message_id}")
            except KeyError:
                logger.warning(f"Message data for {message_id} was already removed")
                
            try:
                del self.pending_confirmations[message_id]
                logger.info(f"Removed pending confirmations for {message_id}")
            except KeyError:
                logger.warning(f"Pending confirmations for {message_id} were already removed")
            
            logger.info(f"Successfully sent and cleaned up data for message {message_id}")

    async def start(self):
        """Запуск сервиса подготовки информации."""
        try:
            # Инициализация RabbitMQ
            await self.init_rabbitmq()

            # Создаем очереди
            images_queue = await self.rabbitmq_channel.declare_queue(
                "multimedia.images",
                durable=True
            )
            
            transcribed_audio_queue = await self.rabbitmq_channel.declare_queue(
                "prompt.transcribed-audio",
                durable=True
            )
            
            text_queue = await self.rabbitmq_channel.declare_queue(
                "multimedia.text",
                durable=True
            )
            
            ready_info_queue = await self.rabbitmq_channel.declare_queue(
                "prompt.ready_info",
                durable=True
            )
            
            logger.info("All queues declared successfully")

            # Начинаем прослушивание очередей
            logger.info("Starting to listen to queues...")
            await images_queue.consume(self.process_images_message, no_ack=True)
            await transcribed_audio_queue.consume(self.process_transcribed_audio_message, no_ack=True)
            await text_queue.consume(self.process_text_message, no_ack=True)

            # Держим соединение активным
            await asyncio.Future()

        except Exception as e:
            logger.error(f"Error in info preparator service: {str(e)}")
            raise
        finally:
            if self.rabbitmq_connection:
                await self.rabbitmq_connection.close()

if __name__ == "__main__":
    preparator = InfoPreparator()
    asyncio.run(preparator.start()) 
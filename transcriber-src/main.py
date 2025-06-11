import asyncio
import json
import aio_pika
import gigaam
from typing import Dict, Any
import logging
import asyncpg
from uuid import UUID
import yaml
import os

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AudioTranscriber:
    def __init__(self):
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.model = None
        self.db_pool = None
        self.config = self.load_config()

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

    async def init_db(self):
        """Инициализация подключения к базе данных."""
        logger.info("Connecting to database...")
        db_config = self.config['postgres']
        self.db_pool = await asyncpg.create_pool(
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['db'],
            host=db_config['host'],
            port=db_config['port']
        )
        logger.info("Connected to database successfully")

    async def get_audio_data(self, audio_uuid: str) -> bytes:
        """Получение аудио данных из базы данных по UUID."""
        logger.info(f"Retrieving audio data for UUID: {audio_uuid}")
        try:
            async with self.db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT audio_data FROM message_audios WHERE id = $1",
                    UUID(audio_uuid)
                )
                if not row:
                    logger.error(f"No audio found for UUID: {audio_uuid}")
                    raise ValueError(f"Audio not found for UUID: {audio_uuid}")
                
                audio_data = row['audio_data']
                logger.info(f"Retrieved audio data, size: {len(audio_data)} bytes")
                return audio_data
        except Exception as e:
            logger.error(f"Error retrieving audio data: {str(e)}")
            raise

    async def init_model(self):
        """Инициализация модели для транскрибации."""
        logger.info("Initializing gigamodel...")
        self.model = gigaam.load_model("ctc")
        logger.info("Model initialized successfully")

    async def init_rabbitmq(self):
        """Инициализация подключения к RabbitMQ."""
        logger.info("Connecting to RabbitMQ...")
        queue_config = self.config['queue']
        self.rabbitmq_connection = await aio_pika.connect_robust(
            f"amqp://{queue_config['username']}:{queue_config['password']}@{queue_config['host']}:{queue_config['port']}/{queue_config['vhost']}"
        )
        self.rabbitmq_channel = await self.rabbitmq_connection.channel()
        logger.info("Connected to RabbitMQ successfully")

    async def transcribe_audio(self, audio_data: bytes) -> str:
        """Транскрибация аудио с помощью модели."""
        logger.info("Starting audio transcription...")
        try:
            # Здесь будет код для транскрибации аудио
            # Пока возвращаем заглушку
            transcribed_text = "Transcribed text will be here"
            logger.info("Audio transcription completed")
            return transcribed_text
        except Exception as e:
            logger.error(f"Error during transcription: {str(e)}")
            raise

    async def process_message(self, message: aio_pika.IncomingMessage):
        """Обработка сообщения из очереди."""
        async with message.process():
            try:
                # Получаем данные из сообщения
                body = message.body.decode()
                data = json.loads(body)
                logger.info(f"Received message: {data}")

                # Получаем UUID аудио
                audio_uuid = data.get('audio_uuids', [None])[0]
                if not audio_uuid:
                    logger.warning("No audio UUID in message")
                    return

                # Получаем аудио данные из базы данных
                audio_data = await self.get_audio_data(audio_uuid)

                # Транскрибируем аудио
                transcribed_text = await self.transcribe_audio(audio_data)

                # Формируем сообщение для отправки
                result_message = {
                    'message_id': data.get('message_id'),
                    'chat_id': data.get('chat_id'),
                    'transcribed_text': transcribed_text,
                    'has_video': data.get('has_video', False),
                    'has_photo': data.get('has_photo', False),
                    'has_audio': data.get('has_audio', False),
                    'image_uuids': data.get('image_uuids', []),
                    'audio_uuids': data.get('audio_uuids', [])
                }
                print("Ответ: ", result_message)

                # Отправляем результат в очередь
                await self.rabbitmq_channel.default_exchange.publish(
                    aio_pika.Message(body=json.dumps(result_message).encode()),
                    routing_key="prompt.transcribed-audio"
                )
                logger.info(f"Sent transcription result to prompt.transcribed-audio queue")

            except Exception as e:
                logger.error(f"Error processing message: {str(e)}")
                raise

    async def start(self):
        """Запуск сервиса транскрибации."""
        try:
            # Инициализация модели, RabbitMQ и базы данных
            await self.init_model()
            await self.init_rabbitmq()
            await self.init_db()

            # Создаем очереди для прослушивания и отправки
            input_queue = await self.rabbitmq_channel.declare_queue(
                "multimedia.audio",
                durable=True
            )
            
            output_queue = await self.rabbitmq_channel.declare_queue(
                "prompt.transcribed-audio",
                durable=True
            )
            logger.info("Queues declared successfully")

            # Начинаем прослушивание очереди
            logger.info("Starting to listen to multimedia.audio queue...")
            await input_queue.consume(self.process_message)

            # Держим соединение активным
            await asyncio.Future()

        except Exception as e:
            logger.error(f"Error in transcriber service: {str(e)}")
            raise
        finally:
            if self.rabbitmq_connection:
                await self.rabbitmq_connection.close()
            if self.db_pool:
                await self.db_pool.close()

if __name__ == "__main__":
    transcriber = AudioTranscriber()
    asyncio.run(transcriber.start()) 
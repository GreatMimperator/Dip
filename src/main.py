import asyncio
import logging

from config import Config
from bot import TelegramBot
from db import Database


async def main():
    # Настройка логирования
    logging.basicConfig(level=logging.INFO)
    
    # Загрузка конфигурации
    config = Config.from_yaml('config.yaml')
    
    # Инициализация базы данных
    db = Database(config)
    await db.connect()
    
    # Инициализация бота
    bot = TelegramBot(config, db)
    await bot.connect_rabbitmq()
    
    try:
        # Запуск бота
        await bot.start()
    finally:
        # Закрытие соединений
        await bot.close_rabbitmq()
        await db.close()


if __name__ == '__main__':
    asyncio.run(main()) 
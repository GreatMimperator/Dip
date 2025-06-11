from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from enum import Enum, auto
from typing import List, Dict, Union, Optional, Set
from datetime import datetime, timezone
import aio_pika
import uuid
import subprocess
import tempfile
import os
import json

from config import Config
from db import Database


class UserRole(Enum):
    """Роли пользователей в системе"""
    ANONYMOUS = auto()  # Неавторизованный пользователь
    MODERATOR = auto()  # Модератор в одном или нескольких чатах
    ADMIN = auto()      # Администратор в одном или нескольких чатах
    SYSADMIN = auto()   # Системный администратор


class BotStates(StatesGroup):
    """Состояния бота для FSM"""
    waiting_for_contact = State()  # Общее состояние для ожидания контакта
    waiting_for_channel_selection = State()
    waiting_for_deactivate_channel_selection = State()
    show_all_admins = State()
    show_all_channels = State()
    waiting_for_moderator_channel_selection = State()
    show_all_moderators = State()
    waiting_for_log_channel_selection = State()
    waiting_for_violation_selection = State()
    waiting_for_decision_action = State()
    waiting_for_notification_policy = State()
    # Новые состояния для создания промпта
    waiting_for_prompt_text = State()
    waiting_for_prompt_type = State()
    waiting_for_prompt_reason = State()
    waiting_for_prompt_explanation = State()
    
    # Состояния для редактирования правил
    waiting_for_rule_type_edit = State()
    waiting_for_rule_text_edit = State()
    waiting_for_rule_explanation_edit = State()
    waiting_for_violation_type = State()


class SysadminMenuButton(Enum):
    CHANNEL_ACTIVATION = "Активация / деактивация каналов"
    ADMIN_ACTIVATION = "Активация / деактивация админов"
    ADMIN_LIST = "Общий список админов"
    CHANNEL_LIST = "Общий список каналов"


class AdminMenuButton(Enum):
    MODERATOR_MANAGEMENT = "Назначение / деактивация модераторов"
    MODERATOR_LIST = "Общий список модераторов"
    BAN_LOGS = "Логи банов/разбанов"
    PROMPT_MANAGEMENT = "Управление промптами"


class ModeratorMenuButton(Enum):
    MY_CHATS = "Мои чаты"
    BAN_LOGS = "Логи банов/разбанов"
    NOTIFICATION_POLICIES = "Мои политики уведомлений"
    RECENT_VIOLATIONS = "Последние нарушения"


SYSADMIN_MENU_BUTTONS = [
    [SysadminMenuButton.CHANNEL_ACTIVATION.value],
    [SysadminMenuButton.ADMIN_ACTIVATION.value],
    [SysadminMenuButton.ADMIN_LIST.value],
    [SysadminMenuButton.CHANNEL_LIST.value]
]

SYSADMIN_MENU = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text=btn) for btn in row] for row in SYSADMIN_MENU_BUTTONS],
    resize_keyboard=True
)

# Меню для админов
ADMIN_MENU_BUTTONS = [
    [AdminMenuButton.MODERATOR_MANAGEMENT.value],
    [AdminMenuButton.MODERATOR_LIST.value],
    [AdminMenuButton.BAN_LOGS.value],
    [AdminMenuButton.PROMPT_MANAGEMENT.value]
]

ADMIN_MENU = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text=btn) for btn in row] for row in ADMIN_MENU_BUTTONS],
    resize_keyboard=True
)

# Меню для модераторов
MODERATOR_MENU_BUTTONS = [
    [ModeratorMenuButton.MY_CHATS.value],
    [ModeratorMenuButton.BAN_LOGS.value],
    [ModeratorMenuButton.NOTIFICATION_POLICIES.value],
    [ModeratorMenuButton.RECENT_VIOLATIONS.value]
]

MODERATOR_MENU = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text=btn) for btn in row] for row in MODERATOR_MENU_BUTTONS],
    resize_keyboard=True
)


class TelegramBot:
    def __init__(self, config: Config, db: Database):
        self.config = config
        self.db = db
        self.bot = Bot(token=config.telegram.bot_token)
        self.dp = Dispatcher(storage=MemoryStorage())
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        
        # Регистрируем хендлеры
        self._register_handlers()
    
    async def connect_rabbitmq(self):
        """Establishes connection to RabbitMQ."""
        self.rabbitmq_connection = await aio_pika.connect_robust(
            host=self.config.queue.host,
            port=self.config.queue.port,
            login=self.config.queue.username,
            password=self.config.queue.password,
            virtualhost=self.config.queue.vhost
        )
        self.rabbitmq_channel = await self.rabbitmq_connection.channel()
        await self.rabbitmq_channel.declare_queue("multimedia.images", durable=True)
        await self.rabbitmq_channel.declare_queue("multimedia.audio", durable=True)
        await self.rabbitmq_channel.declare_queue("multimedia.text", durable=True)

    async def close_rabbitmq(self):
        """Closes RabbitMQ connection."""
        if self.rabbitmq_channel:
            await self.rabbitmq_channel.close()
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()

    async def extract_video_frame(self, video_path: str) -> bytes:
        """Извлекает центральный кадр из видео с помощью ffmpeg."""
        print(f"[DEBUG] Starting frame extraction with ffmpeg from {video_path}")
        try:
            # Получаем длительность видео
            duration_cmd = [
                'ffprobe', 
                '-v', 'error',
                '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                video_path
            ]
            print(f"[DEBUG] Running ffprobe command: {' '.join(duration_cmd)}")
            duration = float(subprocess.check_output(duration_cmd).decode().strip())
            print(f"[DEBUG] Video duration: {duration} seconds")
            
            # Вычисляем время для центрального кадра
            middle_time = duration / 2
            print(f"[DEBUG] Extracting frame at {middle_time} seconds")
            
            # Извлекаем кадр
            frame_path = f"{video_path}_frame.jpg"
            extract_cmd = [
                'ffmpeg',
                '-ss', str(middle_time),
                '-i', video_path,
                '-vframes', '1',
                '-q:v', '2',
                '-y',
                frame_path
            ]
            print(f"[DEBUG] Running ffmpeg command: {' '.join(extract_cmd)}")
            subprocess.run(extract_cmd, check=True, capture_output=True)
            
            # Читаем полученный кадр
            print(f"[DEBUG] Reading extracted frame from {frame_path}")
            with open(frame_path, 'rb') as f:
                frame_data = f.read()
            print(f"[DEBUG] Frame extracted successfully, size: {len(frame_data)} bytes")
            
            # Удаляем временный файл
            os.unlink(frame_path)
            print(f"[DEBUG] Temporary frame file deleted")
            
            return frame_data
        except subprocess.CalledProcessError as e:
            print(f"[DEBUG] Error during frame extraction: {e.stderr.decode()}")
            raise Exception(f"Failed to extract frame: {e.stderr.decode()}")
        except Exception as e:
            print(f"[DEBUG] Unexpected error during frame extraction: {str(e)}")
            raise

    async def extract_video_audio(self, video_file: str) -> bytes:
        """Extracts audio from video using ffmpeg."""
        print(f"[DEBUG] Starting audio extraction with ffmpeg from {video_file}")
        try:
            # Extract audio to temporary file
            audio_path = f"{video_file}_audio.mp3"
            print(f"[DEBUG] Will save audio to {audio_path}")
            
            extract_cmd = [
                'ffmpeg',
                '-i', video_file,
                '-vn',  # No video
                '-acodec', 'libmp3lame',
                '-q:a', '2',
                '-y',
                audio_path
            ]
            print(f"[DEBUG] Running ffmpeg command: {' '.join(extract_cmd)}")
            subprocess.run(extract_cmd, check=True, capture_output=True)
            print(f"[DEBUG] Audio extraction completed")
            
            # Read the audio file
            print(f"[DEBUG] Reading extracted audio from {audio_path}")
            with open(audio_path, 'rb') as f:
                audio_data = f.read()
            print(f"[DEBUG] Audio read successfully, size: {len(audio_data)} bytes")
            
            # Clean up
            os.unlink(audio_path)
            print(f"[DEBUG] Temporary audio file deleted")
            
            return audio_data
        except subprocess.CalledProcessError as e:
            print(f"[DEBUG] Error during audio extraction: {e.stderr.decode()}")
            raise Exception(f"Failed to extract audio: {e.stderr.decode()}")
        except Exception as e:
            print(f"[DEBUG] Unexpected error during audio extraction: {str(e)}")
            raise
    
    async def get_user_role(self, user_id: int) -> UserRole:
        """
        Определяет роль пользователя в системе.
        
        Args:
            user_id: ID пользователя в Telegram
            
        Returns:
            UserRole: Роль пользователя
        """
        # Проверяем, является ли пользователь системным администратором
        if user_id in self.config.admin.sysadmin_ids:
            return UserRole.SYSADMIN
            
        # Проверяем, является ли пользователь администратором в каких-либо чатах
        admin_chats = await self.db.get_moderator_chats_for_user(user_id)
        if admin_chats:
            return UserRole.ADMIN
            
        # Проверяем, является ли пользователь модератором в каких-либо чатах
        moderator_chats = await self.db.get_user_moderator_chats(user_id)
        if moderator_chats:
            return UserRole.MODERATOR
            
        # Если ни одна из ролей не подходит, считаем пользователя анонимным
        return UserRole.ANONYMOUS
    
    def _register_handlers(self):
        """Регистрация всех хендлеров в правильном порядке"""

        # Add message handler for monitoring messages - moved to the top to ensure it's registered first
        self.dp.message.register(self.handle_message_monitoring, F.chat.type.in_({'group', 'supergroup'}))

        # 1. F.contact в конкретных состояниях FSM
        self.dp.message.register(self.handle_contact, BotStates.waiting_for_contact, F.contact)

        # 2. CallbackQuery-хендлеры с состояниями
        self.dp.callback_query.register(self.handle_channel_page, F.data.startswith("page:"), BotStates.waiting_for_channel_selection)
        self.dp.callback_query.register(self.handle_channel_select, F.data.startswith("select_channel:"), BotStates.waiting_for_channel_selection)
        self.dp.callback_query.register(self.handle_deactivate_channel_page, F.data.startswith("deact_page:"), BotStates.waiting_for_deactivate_channel_selection)
        self.dp.callback_query.register(self.handle_deactivate_channel_select, F.data.startswith("deact_channel:"), BotStates.waiting_for_deactivate_channel_selection)
        self.dp.callback_query.register(self.handle_admins_page, F.data.startswith("admins_page:"), BotStates.show_all_admins)
        self.dp.callback_query.register(self.handle_channels_page, F.data.startswith("channels_page:"), BotStates.show_all_channels)
        self.dp.callback_query.register(self.handle_moderator_channel_page, F.data.startswith("mod_page:"), BotStates.waiting_for_moderator_channel_selection)
        self.dp.callback_query.register(self.handle_moderator_channel_select, F.data.startswith("toggle_moderator:"), BotStates.waiting_for_moderator_channel_selection)
        self.dp.callback_query.register(self.handle_moderators_page, F.data.startswith("moderators_page:"), BotStates.show_all_moderators)
        self.dp.callback_query.register(self.handle_log_channel_select, F.data.startswith("log_channel:"), BotStates.waiting_for_log_channel_selection)
        self.dp.callback_query.register(self.handle_log_violation_select, F.data.startswith("log_violation:"), BotStates.waiting_for_violation_selection)
        self.dp.callback_query.register(self.handle_change_decision, F.data.startswith("change_decision:"), BotStates.waiting_for_decision_action)
        self.dp.callback_query.register(self.handle_toggle_notification_policy, F.data.startswith("toggle_policy:"), BotStates.waiting_for_notification_policy)
        self.dp.callback_query.register(self.handle_chat_selection_for_prompt, F.data.startswith("select_chat_for_prompt:"))
        self.dp.callback_query.register(self.handle_prompt_type, F.data.startswith("prompt_type:"), BotStates.waiting_for_prompt_type)
        self.dp.callback_query.register(self.handle_prompt_reason, F.data.startswith("prompt_silent:"), BotStates.waiting_for_prompt_reason)
        self.dp.callback_query.register(self.handle_prompt_explanation, BotStates.waiting_for_prompt_explanation)

        # 3. CallbackQuery-хендлеры без состояний
        self.dp.callback_query.register(self.handle_activate_channel_cmd, F.data == "activate_channel_cmd")
        self.dp.callback_query.register(self.handle_noop, F.data == "noop")
        self.dp.callback_query.register(self.handle_toggle_channel, F.data.startswith("toggle_channel:"))
        self.dp.callback_query.register(self.handle_toggle_admin, F.data.startswith("toggle_admin:"))
        self.dp.callback_query.register(self.handle_add_prompt, F.data == "add_prompt")

        # 4. Message-хендлеры с состояниями
        self.dp.message.register(self.handle_prompt_text, BotStates.waiting_for_prompt_text)
        self.dp.message.register(self.handle_prompt_explanation, BotStates.waiting_for_prompt_explanation)
        # Обработчики текстовых сообщений для редактирования правил
        self.dp.message.register(self.handle_rule_text_edit, BotStates.waiting_for_rule_text_edit)
        self.dp.message.register(self.handle_rule_explanation_edit, BotStates.waiting_for_rule_explanation_edit)

        # 5. Базовые команды
        self.dp.message.register(self.cmd_start, Command(commands=["start"]))

        # 6. Основное меню (Любой текст, если не было мэтча)
        self.dp.message.register(self.handle_main_menu, F.text & ~F.chat.type.in_({'group', 'supergroup'}))

        # 7. Обработчики событий для обновления БД
        self.dp.my_chat_member.register(self.handle_my_chat_member)
        self.dp.chat_member.register(self.handle_chat_member)

        # 8. Debug-хендлеры (в самом конце)
        self.dp.message.register(self.debug_log_message)
        self.dp.message.register(self.debug_contact, F.contact)
        self.dp.message.register(self.debug_any)

        # Новые хендлеры для работы с правилами
        self.dp.callback_query.register(self.handle_list_prompts, F.data == "list_prompts")
        self.dp.callback_query.register(self.handle_rules_page, F.data.startswith("rules_page:"))
        self.dp.callback_query.register(self.handle_view_rule, F.data.startswith("view_rule:"))
        self.dp.callback_query.register(self.handle_delete_rule, F.data.startswith("delete_rule:"))
        self.dp.callback_query.register(self.handle_edit_rule, F.data.startswith("edit_rule:"))
        
        # Обработчики редактирования правил
        self.dp.callback_query.register(self.handle_edit_rule_type, F.data.startswith("edit_rule_type:"))
        self.dp.callback_query.register(self.handle_edit_rule_text, F.data.startswith("edit_rule_text:"))
        self.dp.callback_query.register(self.handle_edit_rule_explanation, F.data.startswith("edit_rule_explanation:"))
        self.dp.callback_query.register(self.handle_rule_type_edit, F.data.startswith("rule_type:"))
        
        # Обработчики для последних нарушений
        self.dp.callback_query.register(
            self.handle_violation_type_select,
            F.data.startswith("violation_type:"),
            BotStates.waiting_for_violation_type
        )
        
        self.dp.callback_query.register(
            self.handle_violation_action,
            F.data.startswith("violation_action:")
        )


    
    async def cmd_start(self, message: types.Message):
        user_id = message.from_user.id
        user_role = await self.get_user_role(user_id)
        if user_role == UserRole.SYSADMIN:
            await message.answer("Добро пожаловать! Выберите действие:", reply_markup=SYSADMIN_MENU)
            return
        if user_role == UserRole.ADMIN:
            await message.answer("Добро пожаловать! Выберите действие:", reply_markup=ADMIN_MENU)
            return
        if user_role == UserRole.MODERATOR:
            moderator_chats = await self.db.get_user_moderator_chats(user_id)
            await message.answer(
                "Добро пожаловать! Вы являетесь модератором в следующих чатах:\n" +
                "\n".join([f"• {chat['title']}" for chat in moderator_chats]),
                reply_markup=MODERATOR_MENU
            )
            return
            
        await message.answer("У вас нет прав для работы с этим ботом.")
    
    async def handle_main_menu(self, message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        user_role = await self.get_user_role(user_id)
        
        match user_role:
            case UserRole.SYSADMIN:
                await self._handle_sysadmin_menu(message, state)
            case UserRole.ADMIN:
                await self._handle_admin_menu(message, state)
            case UserRole.MODERATOR:
                await self._handle_moderator_menu(message, state)
            case _:
                await message.answer("У вас нет прав для работы с этим ботом.")

    async def _handle_sysadmin_menu(self, message: types.Message, state: FSMContext):
        """Обработчик меню системного администратора."""
        match message.text:
            case SysadminMenuButton.CHANNEL_ACTIVATION.value:
                await message.answer(
                    "Пожалуйста, перейдите в профиль администратора канала, который вы хотите активировать, "
                    "и выберите 'Поделиться контактом'. Затем перешлите контакт сюда.",
                    reply_markup=SYSADMIN_MENU
                )
                await state.set_state(BotStates.waiting_for_contact)
                await state.update_data(action_type='activate_channel')
            case SysadminMenuButton.ADMIN_ACTIVATION.value:
                await message.answer(
                    "Пожалуйста, перейдите в профиль администратора, которого хотите активировать или деактивировать, "
                    "и выберите 'Поделиться контактом'. Затем перешлите контакт сюда.",
                    reply_markup=SYSADMIN_MENU
                )
                await state.set_state(BotStates.waiting_for_contact)
                await state.update_data(action_type='deactivate_admin')
            case SysadminMenuButton.ADMIN_LIST.value:
                await state.set_state(BotStates.show_all_admins)
                await self._send_admins_page(message, 0, state)
            case SysadminMenuButton.CHANNEL_LIST.value:
                await state.set_state(BotStates.show_all_channels)
                await self._send_channels_page(message, 0, state)
            case _:
                await message.answer("Неизвестная команда", reply_markup=SYSADMIN_MENU)

    async def _handle_admin_menu(self, message: types.Message, state: FSMContext):
        """Обработчик меню администратора."""
        match message.text:
            case AdminMenuButton.MODERATOR_MANAGEMENT.value:
                await self.handle_moderator_menu(message, state)
            case AdminMenuButton.MODERATOR_LIST.value:
                await self.handle_show_all_moderators(message, state)
            case AdminMenuButton.BAN_LOGS.value:
                await self.handle_logs_entry(message, state)
            case AdminMenuButton.PROMPT_MANAGEMENT.value:
                await self.handle_prompt_management(message, state)
            case _:
                await message.answer("Неизвестная команда.", reply_markup=ADMIN_MENU)

    async def _handle_moderator_menu(self, message: types.Message, state: FSMContext):
        """Обработчик меню модератора."""
        user_id = message.from_user.id
        match message.text:
            case ModeratorMenuButton.MY_CHATS.value:
                moderator_chats = await self.db.get_user_moderator_chats(user_id)
                await message.answer(
                    "Вы являетесь модератором в следующих чатах:\n" +
                    "\n".join([f"• {chat['title']}" for chat in moderator_chats]),
                    reply_markup=MODERATOR_MENU
                )
            case ModeratorMenuButton.BAN_LOGS.value:
                await self.handle_logs_entry(message, state)
            case ModeratorMenuButton.NOTIFICATION_POLICIES.value:
                await self.handle_show_notification_policies(message, state)
            case ModeratorMenuButton.RECENT_VIOLATIONS.value:
                await self.handle_recent_violations_entry(message, state)
            case _:
                await message.answer("Неизвестная команда.", reply_markup=MODERATOR_MENU)

    async def handle_contact(self, message: types.Message, state: FSMContext):
        """
        Общий обработчик контактов. Определяет роль пользователя и вызывает соответствующий обработчик.
        """
        user_id = message.from_user.id
        user_role = await self.get_user_role(user_id)
        
        # Получаем данные о текущем действии из состояния
        state_data = await state.get_data()
        action_type = state_data.get('action_type')
        
        match user_role:
            case UserRole.SYSADMIN:
                match action_type:
                    case 'activate_channel':
                        await self.handle_contact_for_channel(message, state)
                    case 'deactivate_admin':
                        await self.handle_contact_for_deactivate_admin(message, state)
                    case _:
                        await message.answer("Неизвестное действие для системного администратора.")
                        await state.clear()
                        
            case UserRole.ADMIN:
                match action_type:
                    case 'moderator_management':
                        await self.handle_contact_for_moderator(message, state)
                    case _:
                        await message.answer("Неизвестное действие для администратора.")
                        await state.clear()
                        
            case _:
                await message.answer("У вас нет прав для выполнения этого действия.")
                await state.clear()
    
    async def handle_contact_for_channel(self, message: types.Message, state: FSMContext):
        contact = message.contact
        print(f"[LOG] Contact object: {contact}")
        print(f"[LOG] Contact attributes: {dir(contact)}")
        print(f"[LOG] Contact first_name: {contact.first_name}")
        print(f"[LOG] Contact last_name: {contact.last_name}")
        print(f"[LOG] Contact user_id: {contact.user_id}")
        
        admin_user_id = contact.user_id
        # Получаем данные пользователя из контакта
        username = contact.username if hasattr(contact, 'username') else None
        # Формируем full_name только из существующих полей
        full_name = contact.first_name
        if contact.last_name:
            full_name += f" {contact.last_name}"
        print(f"[LOG] Получен контакт: user_id={admin_user_id}, username={username}, full_name={full_name}")

        # Проверяем, есть ли пользователь уже в users
        user_exists = await self.db.user_exists(admin_user_id)
        if not user_exists:
            await self.db.add_or_update_user(admin_user_id, username, full_name)
            print(f"[LOG] Данные пользователя сохранены в БД")
        else:
            print(f"[LOG] Пользователь уже есть в users, не обновляем запись")

        await state.update_data(selected_admin_user_id=admin_user_id)
        print(f"[LOG] FSM: update_data selected_admin_user_id={admin_user_id}")
        my_chats = await self.db.get_admin_chats_for_user(admin_user_id)
        print(f"[LOG] Каналы пользователя {admin_user_id}: {my_chats}")
        if not my_chats:
            print("[LOG] Нет каналов для пользователя, показываем кнопку добавления.")
            markup = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="Добавить канал", callback_data="activate_channel_cmd")]
                ]
            )
            await message.answer(
                "Нет каналов, где этот пользователь является администратором.\n\n"
                "👉 Чтобы добавить канал, сделайте бота администратором нужного канала и затем нажмите кнопку ниже.",
                reply_markup=markup
            )
            await state.clear()
            print(f"[LOG] FSM: state cleared")
            return

        await state.update_data(channels=my_chats, page=0)
        print(f"[LOG] FSM: update_data channels/page")
        await self._send_channel_page(message, my_chats, 0, state)
        await state.set_state(BotStates.waiting_for_channel_selection)
        print(f"[LOG] FSM: set_state -> waiting_for_channel_selection")
    
    def _build_channel_menu(self, channels, page, page_size):
        start = page * page_size
        end = start + page_size
        page_channels = channels[start:end]
        keyboard = []
        for ch in page_channels:
            status = "активен" if ch.get('activated', True) else "неактивен"
            btn_text = f"{ch['title']} ({status})"
            keyboard.append([InlineKeyboardButton(text=btn_text, callback_data=f"toggle_channel:{ch['id']}")])
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"page:{page-1}"))
        nav.append(InlineKeyboardButton(text=f"{page+1}/{(len(channels)-1)//page_size+1}", callback_data="noop"))
        if end < len(channels):
            nav.append(InlineKeyboardButton("➡️", callback_data=f"page:{page+1}"))
        if nav:
            keyboard.append(nav)
        return InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    async def _send_channel_page(self, message_or_query, channels, page, state):
        page_size = self.config.ui.page_size
        markup = self._build_channel_menu(channels, page, page_size)
        text = "Выберите канал для активации:" if channels else "Нет доступных каналов."
        if isinstance(message_or_query, types.Message):
            await message_or_query.answer(text, reply_markup=markup)
        else:
            await message_or_query.message.edit_text(text, reply_markup=markup)
        await state.update_data(page=page)
    
    async def handle_channel_page(self, query: types.CallbackQuery, state: FSMContext):
        data = await state.get_data()
        channels = data.get("channels", [])
        page = int(query.data.split(":")[1])
        await self._send_channel_page(query, channels, page, state)
        await query.answer()
    
    async def handle_channel_select(self, query: types.CallbackQuery, state: FSMContext):
        channel_id = int(query.data.split(":")[1])
        data = await state.get_data()
        channels = data.get("channels", [])
        channel = next((c for c in channels if c["id"] == channel_id), None)
        selected_admin_user_id = data.get("selected_admin_user_id")
        print(f"[LOG] Выбран канал: {channel_id}, выбранный админ: {selected_admin_user_id}")
        if channel:
            # Проверяем, активирован ли уже канал
            async with self.db.pool.acquire() as conn:
                row = await conn.fetchrow('SELECT activated FROM chats WHERE id = $1', channel_id)
                if row and row['activated']:
                    await query.message.edit_text(f"Канал {channel['title']} (ID: {channel['id']}) уже активирован!")
                    await state.clear()
                    return
            print(f"[LOG] Активируем канал {channel_id}")
            await self.db.add_chat(
                channel_id,
                channel["title"],
                activated=True,
                is_bot_in=True
            )
            await query.message.edit_text(f"Канал {channel['title']} (ID: {channel['id']}) успешно активирован!")
        else:
            print(f"[LOG] Ошибка: канал {channel_id} не найден в списке.")
            await query.message.edit_text("Ошибка: канал не найден.")
        await state.clear()
    
    async def handle_contact_for_deactivate_admin(self, message: types.Message, state: FSMContext):
        print(f"[LOG] handle_contact_for_deactivate_admin: state={await state.get_state()}")
        contact = message.contact
        admin_user_id = contact.user_id
        print(f"[LOG] Получен контакт для управления: user_id={admin_user_id}")
        if not admin_user_id:
            await message.answer("Не удалось получить ID пользователя. Пользователь должен быть в Telegram.", reply_markup=SYSADMIN_MENU)
            await state.clear()
            print(f"[LOG] FSM: state cleared")
            return
        # Получаем все чаты, где бот есть
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch('SELECT id, title FROM chats WHERE is_bot_in = TRUE')
        # Для каждого чата проверяем, есть ли этот админ и его статус
        admins_status = []
        for row in rows:
            chat_id = row['id']
            title = row['title']
            admin_row = None
            async with self.db.pool.acquire() as conn:
                admin_row = await conn.fetchrow(
                    'SELECT activated FROM chat_admins WHERE chat_id = $1 AND user_id = $2',
                    chat_id, admin_user_id
                )
            admins_status.append({
                'chat_id': chat_id,
                'title': title,
                'active': bool(admin_row and admin_row['activated'])
            })
        await state.update_data(admin_user_id=admin_user_id, admins_status=admins_status, admin_page=0)
        await self._send_admin_page(message, admins_status, 0, state)
        await state.set_state(BotStates.waiting_for_contact)
        print(f"[LOG] FSM: set_state -> waiting_for_contact")

    def _build_admin_menu(self, admins_status, page, page_size):
        start = page * page_size
        end = start + page_size
        page_admins = admins_status[start:end]
        keyboard = []
        for adm in page_admins:
            status = "активен" if adm.get('active', False) else "неактивен"
            btn_text = f"{adm['title']} ({status})"
            keyboard.append([InlineKeyboardButton(text=btn_text, callback_data=f"toggle_admin:{adm['chat_id']}")])
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"admin_page:{page-1}"))
        nav.append(InlineKeyboardButton(text=f"{page+1}/{(len(admins_status)-1)//page_size+1}", callback_data="noop"))
        if end < len(admins_status):
            nav.append(InlineKeyboardButton("➡️", callback_data=f"admin_page:{page+1}"))
        if nav:
            keyboard.append(nav)
        return InlineKeyboardMarkup(inline_keyboard=keyboard)

    async def _send_admin_page(self, message_or_query, admins_status, page, state):
        page_size = self.config.ui.page_size
        markup = self._build_admin_menu(admins_status, page, page_size)
        text = "Выберите чат для активации/деактивации админа:" if admins_status else "Нет доступных чатов."
        if isinstance(message_or_query, types.Message):
            await message_or_query.answer(text, reply_markup=markup)
        else:
            await message_or_query.message.edit_text(text, reply_markup=markup)
        await state.update_data(admin_page=page)

    async def handle_toggle_admin(self, query: types.CallbackQuery, state: FSMContext):
        data = await state.get_data()
        admin_user_id = data.get("admin_user_id")
        admins_status = data.get("admins_status", [])
        page = data.get("admin_page", 0)
        chat_id = int(query.data.split(":")[1])
        # Найти текущий статус
        current = next((a for a in admins_status if a['chat_id'] == chat_id), None)
        if not current:
            await query.answer("Чат не найден")
            return
        if current['active']:
            # Деактивировать (удалить из chat_admins)
            async with self.db.pool.acquire() as conn:
                await conn.execute('DELETE FROM chat_admins WHERE chat_id = $1 AND user_id = $2', chat_id, admin_user_id)
            current['active'] = False
        else:
            # Активировать (добавить в chat_admins)
            await self.db.add_admin(chat_id, admin_user_id)
            current['active'] = True
        await self._send_admin_page(query, admins_status, page, state)
        await query.answer()

    async def handle_deactivate_channel_page(self, message_or_query, state: FSMContext):
        data = await state.get_data()
        chats = data.get("deact_chats", [])
        page = data.get("deact_page", 0)
        page_size = self.config.ui.page_size
        start = page * page_size
        end = start + page_size
        page_chats = chats[start:end]
        keyboard = []
        for ch in page_chats:
            keyboard.append([InlineKeyboardButton(text=ch['title'], callback_data=f"deact_channel:{ch['id']}")])
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"deact_page:{page-1}"))
        nav.append(InlineKeyboardButton(text=f"{page+1}/{(len(chats)-1)//page_size+1}", callback_data="noop"))
        if end < len(chats):
            nav.append(InlineKeyboardButton("➡️", callback_data=f"deact_page:{page+1}"))
        if nav:
            keyboard.append(nav)
        markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
        text = "Выберите канал для деактивации:" if chats else "Нет доступных каналов."
        if isinstance(message_or_query, types.Message):
            await message_or_query.answer(text, reply_markup=markup)
        else:
            await message_or_query.message.edit_text(text, reply_markup=markup)
        await state.update_data(deact_page=page)
    
    async def handle_deactivate_channel_select(self, query: types.CallbackQuery, state: FSMContext):
        channel_id = int(query.data.split(":")[1])
        await self.db.deactivate_chat(channel_id)
        await query.message.edit_text(f"Канал {channel_id} деактивирован.")
        await state.clear()
    
    async def handle_my_chat_member(self, event: types.ChatMemberUpdated):
        chat = event.chat
        new_status = event.new_chat_member.status
        old_status = event.old_chat_member.status
        print(f"[LOG] handle_my_chat_member: chat_id={chat.id}, title={chat.title}, old_status={old_status}, new_status={new_status}")
        print(f"[LOG] event.new_chat_member: {event.new_chat_member}")
        print(f"[LOG] can_read_all_group_messages: {getattr(event.new_chat_member, 'can_read_all_group_messages', None)}")
        print(f"[LOG] can_restrict_members: {getattr(event.new_chat_member, 'can_restrict_members', None)}")
        print(f"[LOG] is_anonymous: {getattr(event.new_chat_member, 'is_anonymous', None)}")
        print(f"[LOG] custom_title: {getattr(event.new_chat_member, 'custom_title', None)}")
        print(f"[LOG] privileges: {event.new_chat_member.__dict__ if hasattr(event.new_chat_member, '__dict__') else str(event.new_chat_member)}")
        can_read = getattr(event.new_chat_member, "can_read_all_group_messages", None)
        can_read_messages = (can_read is None) or (can_read is True)
        can_restrict = getattr(event.new_chat_member, "can_restrict_members", False)
        if new_status == "administrator" and old_status != "administrator":
            print(f"[LOG] Бот стал админом в чате {chat.id} ('{chat.title}')")
            for sysadmin_id in self.config.admin.sysadmin_ids:
                try:
                    await self.bot.send_message(
                        sysadmin_id,
                        f"Бот был добавлен администратором в чат '{chat.title}' (ID: {chat.id})"
                    )
                except Exception as e:
                    print(f"[LOG] Ошибка отправки уведомления сисадмину {sysadmin_id}: {e}")
            await self.db.add_chat(chat.id, chat.title or "", activated=True, can_read_messages=can_read_messages, can_restrict_members=can_restrict, is_bot_in=True)
            print(f"[LOG] Чат {chat.id} ('{chat.title}') добавлен/активирован в базе.")
            # Получаем и сохраняем всех админов чата
            try:
                admins = await self.bot.get_chat_administrators(chat.id)
                for admin in admins:
                    # Сохраняем данные пользователя
                    username = getattr(admin.user, 'username', None)
                    full_name = admin.user.first_name
                    if getattr(admin.user, 'last_name', None):
                        full_name += f" {admin.user.last_name}"
                    await self.db.add_or_update_user(admin.user.id, username, full_name)
                    # Добавляем админа как неактивного
                    await self.db.add_admin(chat.id, admin.user.id, activated=False)
                    print(f"[LOG] (auto) Добавлен админ user_id={admin.user.id} в chat_admins для чата {chat.id} (неактивный)")
            except Exception as e:
                print(f"[LOG] Ошибка при получении админов чата {chat.id}: {e}")
        elif new_status in ("administrator", "member"):
            await self.db.add_chat(chat.id, chat.title or "", activated=True, can_read_messages=can_read_messages, can_restrict_members=can_restrict, is_bot_in=True)
            print(f"[LOG] Чат {chat.id} ('{chat.title}') обновлён/активирован в базе.")
        elif new_status in ("left", "kicked"):
            print(f"[LOG] Бот удалён или потерял права в чате {chat.id} ('{chat.title}')")
            for sysadmin_id in self.config.admin.sysadmin_ids:
                try:
                    await self.bot.send_message(
                        sysadmin_id,
                        f"Бот был удалён или потерял права в чате '{chat.title}' (ID: {chat.id})"
                    )
                except Exception as e:
                    print(f"[LOG] Ошибка отправки уведомления сисадмину {sysadmin_id}: {e}")
            await self.db.add_chat(
                chat.id,
                chat.title or "",
                activated=False,
                can_read_messages=False,
                can_restrict_members=False,
                is_bot_in=False
            )
            print(f"[LOG] Чат {chat.id} ('{chat.title}') деактивирован и is_bot_in=False в базе.")

    async def handle_chat_member(self, event: types.ChatMemberUpdated):
        """Обработчик изменения прав участника чата."""
        chat_id = event.chat.id
        user_id = event.new_chat_member.user.id
        old_status = event.old_chat_member.status
        new_status = event.new_chat_member.status
        
        print(f"[LOG] handle_chat_member: chat_id={chat_id}, user_id={user_id}, old_status={old_status}, new_status={new_status}")
        
        # Проверяем, является ли пользователь модератором в этом чате
        async with self.db.pool.acquire() as conn:
            is_moderator = await conn.fetchval(
                'SELECT 1 FROM chat_moderators WHERE chat_id = $1 AND user_id = $2 AND activated = TRUE',
                chat_id, user_id
            )
            
            if is_moderator:
                # Если пользователь был модератором и потерял права
                if old_status in ['administrator', 'member'] and new_status not in ['administrator', 'member']:
                    print(f"[LOG] Модератор {user_id} потерял права в чате {chat_id}")
                    await self.db.update_moderator_status(chat_id, user_id, False)
                    return
                
                # Если пользователь был модератором и получил права админа
                if old_status not in ['administrator'] and new_status == 'administrator':
                    print(f"[LOG] Модератор {user_id} получил права админа в чате {chat_id}")
                    # Не деактивируем модератора, так как он может быть и админом, и модератором
                    return
                
                # Если пользователь был админом и потерял права админа, но остался участником
                if old_status == 'administrator' and new_status == 'member':
                    print(f"[LOG] Админ {user_id} стал обычным участником в чате {chat_id}")
                    # Не деактивируем модератора, так как он может быть модератором без прав админа
                    return
        
        # Проверяем, является ли пользователь админом в этом чате
        is_admin = await self.db.user_is_admin_in_chat(user_id, chat_id)
        if is_admin:
            # Если пользователь был админом и потерял права
            if old_status == 'administrator' and new_status != 'administrator':
                print(f"[LOG] Админ {user_id} потерял права в чате {chat_id}")
                await self.db.update_admin_status(chat_id, user_id, False)
                return
            
            # Если пользователь был админом и получил права обратно
            if old_status != 'administrator' and new_status == 'administrator':
                print(f"[LOG] Админ {user_id} получил права обратно в чате {chat_id}")
                await self.db.update_admin_status(chat_id, user_id, True)
                return

    async def handle_activate_channel_cmd(self, query: types.CallbackQuery, state: FSMContext):
        await self.cmd_activate_channel(query.message, state, user_id=query.from_user.id)
        await query.answer()

    async def handle_noop(self, query: types.CallbackQuery, state: FSMContext):
        await query.answer()

    async def handle_toggle_channel(self, query: types.CallbackQuery, state: FSMContext):
        channel_id = int(query.data.split(":")[1])
        # Получаем текущий статус
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT activated, title FROM chats WHERE id = $1', channel_id)
            if not row:
                await query.answer("Канал не найден")
                return
            new_status = not row['activated']
            await self.db.add_chat(channel_id, row['title'], activated=new_status, is_bot_in=True)
        # Обновляем клавиатуру
        data = await state.get_data()
        chats = await self.db.get_all_chats()
        page = data.get("page", 0)
        # Обновляем статус в списке
        for ch in chats:
            if ch['id'] == channel_id:
                ch['activated'] = new_status
        markup = self._build_channels_menu(chats, page, len(chats), self.config.ui.page_size)
        text = "Список всех каналов:" if chats else "Нет каналов."
        # Проверяем, изменился ли статус
        if row['activated'] == new_status:
            await query.answer("Статус канала не изменился.")
            return
        if isinstance(query, types.CallbackQuery):
            await query.message.edit_text(text, reply_markup=markup)
        await query.answer()

    async def _send_deactivate_channel_page(self, message_or_query, chats, page, state):
        page_size = self.config.ui.page_size
        start = page * page_size
        end = start + page_size
        page_chats = chats[start:end]
        keyboard = []
        for ch in page_chats:
            keyboard.append([InlineKeyboardButton(text=ch['title'], callback_data=f"deact_channel:{ch['id']}")])
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"deact_page:{page-1}"))
        nav.append(InlineKeyboardButton(text=f"{page+1}/{(len(chats)-1)//page_size+1}", callback_data="noop"))
        if end < len(chats):
            nav.append(InlineKeyboardButton("➡️", callback_data=f"deact_page:{page+1}"))
        if nav:
            keyboard.append(nav)
        markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
        text = "Выберите канал для деактивации:" if chats else "Нет доступных каналов."
        if isinstance(message_or_query, types.Message):
            await message_or_query.answer(text, reply_markup=markup)
        else:
            await message_or_query.message.edit_text(text, reply_markup=markup)
        await state.update_data(deact_page=page)

    async def debug_log_message(self, message: types.Message):
        # print(f"[DEBUG] RAW MESSAGE: {message.model_dump_json() if hasattr(message, 'model_dump_json') else str(message)}")
        pass

    async def start(self):
        """Запуск бота"""
        await self.dp.start_polling(self.bot)

    async def _send_admins_page(self, message_or_query, page, state):
        """Отправляет страницу со списком админов."""
        print(f"[LOG] _send_admins_page: page={page}")
        page_size = self.config.ui.page_size
        print(f"[LOG] page_size={page_size}")
        users = await self.db.get_all_users(page * page_size, page_size)
        print(f"[LOG] Получено пользователей: {len(users)}")
        total = await self.db.get_users_count()
        print(f"[LOG] Всего пользователей: {total}")
        markup = self._build_admins_menu(users, page, total, page_size)
        text = "Список всех администраторов:" if users else "Нет администраторов."
        if isinstance(message_or_query, types.Message):
            print(f"[LOG] Отправляем сообщение пользователю")
            await message_or_query.answer(text, reply_markup=markup)
        else:
            print(f"[LOG] Редактируем сообщение")
            await message_or_query.message.edit_text(text, reply_markup=markup)
        await state.update_data(admin_page=page)
        print(f"[LOG] FSM: update_data admin_page={page}")

    async def _send_channels_page(self, message_or_query, page, state):
        """Отправляет страницу со списком каналов."""
        print(f"[LOG] _send_channels_page: page={page}")
        page_size = self.config.ui.page_size
        print(f"[LOG] page_size={page_size}")
        chats = await self.db.get_all_chats()
        print(f"[LOG] Получено чатов: {len(chats)}")
        total = len(chats)
        markup = self._build_channels_menu(chats, page, total, page_size)
        text = "Список всех каналов:" if chats else "Нет каналов."
        if isinstance(message_or_query, types.Message):
            print(f"[LOG] Отправляем сообщение пользователю")
            await message_or_query.answer(text, reply_markup=markup)
        else:
            print(f"[LOG] Редактируем сообщение")
            await message_or_query.message.edit_text(text, reply_markup=markup)
        await state.update_data(channel_page=page)
        print(f"[LOG] FSM: update_data channel_page={page}")

    def _build_admins_menu(self, users, page, total, page_size):
        """Строит меню для списка админов."""
        keyboard = []
        for user in users:
            username = user['username'] or 'Нет username'
            full_name = user['full_name'] or 'Нет имени'
            btn_text = f"{full_name} (@{username})"
            keyboard.append([InlineKeyboardButton(text=btn_text, callback_data="noop")])
        
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"admins_page:{page-1}"))
        nav.append(InlineKeyboardButton(text=f"{page+1}/{(total-1)//page_size+1}", callback_data="noop"))
        if (page + 1) * page_size < total:
            nav.append(InlineKeyboardButton("➡️", callback_data=f"admins_page:{page+1}"))
        if nav:
            keyboard.append(nav)
        
        return InlineKeyboardMarkup(inline_keyboard=keyboard)

    def _build_channels_menu(self, chats, page, total, page_size):
        """Строит меню для списка каналов."""
        keyboard = []
        for chat in chats:
            status = "активен" if chat.get('activated', True) else "неактивен"
            btn_text = f"{chat['title']} ({status})"
            keyboard.append([InlineKeyboardButton(text=btn_text, callback_data=f"toggle_channel:{chat['id']}")])
        
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"channels_page:{page-1}"))
        nav.append(InlineKeyboardButton(text=f"{page+1}/{(total-1)//page_size+1}", callback_data="noop"))
        if (page + 1) * page_size < total:
            nav.append(InlineKeyboardButton("➡️", callback_data=f"channels_page:{page+1}"))
        if nav:
            keyboard.append(nav)
        
        return InlineKeyboardMarkup(inline_keyboard=keyboard)

    async def handle_admins_page(self, query: types.CallbackQuery, state: FSMContext):
        """Обработчик переключения страниц списка админов."""
        print(f"[LOG] handle_admins_page: user_id={query.from_user.id}, data={query.data}")
        page = int(query.data.split(":")[1])
        print(f"[LOG] Переключение на страницу {page}")
        await self._send_admins_page(query, page, state)
        await query.answer()

    async def handle_channels_page(self, query: types.CallbackQuery, state: FSMContext):
        """Обработчик переключения страниц списка каналов."""
        print(f"[LOG] handle_channels_page: user_id={query.from_user.id}, data={query.data}")
        page = int(query.data.split(":")[1])
        print(f"[LOG] Переключение на страницу {page}")
        await self._send_channels_page(query, page, state)
        await query.answer()

    async def handle_moderator_menu(self, message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        print(f"[LOG] handle_moderator_menu: user_id={user_id}")
        admin_chats = await self.db.get_moderator_chats_for_user(user_id)
        print(f"[LOG] admin_chats: {admin_chats}")
        if not admin_chats:
            await message.answer("У вас нет каналов, где вы являетесь активным админом.", reply_markup=ADMIN_MENU)
            print(f"[LOG] Нет доступных каналов для админа")
            return
        await message.answer(
            "Перешлите контакт пользователя, которого хотите назначить модератором (или снять).",
            reply_markup=ADMIN_MENU
        )
        print(f"[LOG] Инструкция по пересылке контакта модератора отправлена")
        print(f"[LOG] FSM перед set_state: {await state.get_state()}")
        await state.set_state(BotStates.waiting_for_contact)
        await state.update_data(action_type='moderator_management')
        print(f"[LOG] FSM после set_state: {await state.get_state()}")

    async def handle_contact_for_moderator(self, message: types.Message, state: FSMContext):
        print(f"[LOG] handle_contact_for_moderator: state={await state.get_state()} (Ожидаем: waiting_for_contact)")
        contact = message.contact
        moderator_user_id = contact.user_id
        print(f"[LOG] Получен контакт модератора: user_id={moderator_user_id}")
        username = contact.username if hasattr(contact, 'username') else None
        full_name = contact.first_name
        if contact.last_name:
            full_name += f" {contact.last_name}"
        user_exists = await self.db.user_exists(moderator_user_id)
        if not user_exists:
            await self.db.add_or_update_user(moderator_user_id, username, full_name)
            print(f"[LOG] Данные модератора сохранены в БД")
        else:
            print(f"[LOG] Модератор уже есть в users, не обновляем запись")
        user_id = message.from_user.id
        admin_chats = await self.db.get_moderator_chats_for_user(user_id)
        print(f"[LOG] admin_chats для админа: {admin_chats}")
        if not admin_chats:
            await message.answer("У вас нет каналов для назначения модератора.", reply_markup=ADMIN_MENU)
            print(f"[LOG] Нет доступных каналов для назначения модератора")
            return
        await state.update_data(selected_moderator_user_id=moderator_user_id, mod_channels=admin_chats, mod_page=0)
        print(f"[LOG] FSM: update_data selected_moderator_user_id={moderator_user_id}, mod_channels={admin_chats}, mod_page=0")
        await self._send_moderator_channel_page(message, admin_chats, 0, state)
        await state.set_state(BotStates.waiting_for_moderator_channel_selection)

    async def _send_moderator_channel_page(self, message_or_query, channels, page, state):
        print(f"[LOG] _send_moderator_channel_page: page={page}, channels={channels}")
        page_size = self.config.ui.page_size
        start = page * page_size
        end = start + page_size
        page_channels = channels[start:end]
        data = await state.get_data()
        moderator_user_id = data.get("selected_moderator_user_id")
        statuses = []
        for ch in page_channels:
            async with self.db.pool.acquire() as conn:
                row = await conn.fetchrow(
                    'SELECT activated FROM chat_moderators WHERE chat_id = $1 AND user_id = $2',
                    ch['id'], moderator_user_id
                )
            statuses.append({
                'chat_id': ch['id'],
                'title': ch['title'],
                'active': bool(row and row['activated'])
            })
        print(f"[LOG] Статусы модератора по каналам: {statuses}")
        keyboard = []
        for st in statuses:
            status = "активен" if st['active'] else "неактивен"
            btn_text = f"{st['title']} ({status})"
            keyboard.append([InlineKeyboardButton(text=btn_text, callback_data=f"toggle_moderator:{st['chat_id']}")])
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"mod_page:{page-1}"))
        nav.append(InlineKeyboardButton(text=f"{page+1}/{(len(channels)-1)//page_size+1}", callback_data="noop"))
        if end < len(channels):
            nav.append(InlineKeyboardButton("➡️", callback_data=f"mod_page:{page+1}"))
        if nav:
            keyboard.append(nav)
        markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
        text = "Выберите канал для назначения/деактивации модератора:" if statuses else "Нет доступных каналов."
        if isinstance(message_or_query, types.Message):
            await message_or_query.answer(text, reply_markup=markup)
            print(f"[LOG] Инструкция по выбору канала отправлена")
        else:
            await message_or_query.message.edit_text(text, reply_markup=markup)
            print(f"[LOG] Инструкция по выбору канала обновлена")
        await state.update_data(mod_page=page)
        print(f"[LOG] FSM: update_data mod_page={page}")

    async def handle_moderator_channel_page(self, query: types.CallbackQuery, state: FSMContext):
        data = await state.get_data()
        channels = data.get("mod_channels", [])
        page = int(query.data.split(":")[1])
        await self._send_moderator_channel_page(query, channels, page, state)
        await query.answer()

    async def handle_moderator_channel_select(self, query: types.CallbackQuery, state: FSMContext):
        data = await state.get_data()
        moderator_user_id = data.get("selected_moderator_user_id")
        channels = data.get("mod_channels", [])
        page = data.get("mod_page", 0)
        chat_id = int(query.data.split(":")[1])
        # Проверяем, что пользователь-инициатор админ в этом чате
        user_id = query.from_user.id
        is_admin = await self.db.user_is_admin_in_chat(user_id, chat_id)
        if not is_admin:
            await query.answer("Нет прав на управление этим каналом.")
            return
        # Получаем текущий статус
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT activated FROM chat_moderators WHERE chat_id = $1 AND user_id = $2', chat_id, moderator_user_id)
        current_active = bool(row and row['activated'])
        if current_active:
            # Деактивировать
            await self.db.update_moderator_status(chat_id, moderator_user_id, False)
        else:
            # Активировать
            await self.db.add_moderator(chat_id, moderator_user_id, activated=True)
        # Обновить страницу
        await self._send_moderator_channel_page(query, channels, page, state)
        await query.answer()

    async def handle_show_all_moderators(self, message: types.Message, state: FSMContext) -> None:
        """Показывает общий список модераторов."""
        await state.set_state(BotStates.show_all_moderators)
        await self._send_moderators_page(message, 0, state)

    async def _send_moderators_page(self, message_or_query, page, state):
        """Отправляет страницу со списком модераторов."""
        page_size = self.config.ui.page_size
        total = await self.db.get_moderators_count()
        users = await self.db.get_all_moderators(page * page_size, page_size)
        
        markup = self._build_moderators_menu(users, page, total, page_size)
        text = "Список модераторов:" if users else "Нет активных модераторов."
        
        if isinstance(message_or_query, types.Message):
            await message_or_query.answer(text, reply_markup=markup)
        else:
            await message_or_query.message.edit_text(text, reply_markup=markup)
        await state.update_data(page=page)

    def _build_moderators_menu(self, users, page, total, page_size):
        """Строит меню со списком модераторов."""
        keyboard = []
        for user in users:
            username = user['username'] or 'Нет username'
            full_name = user['full_name'] or 'Нет имени'
            btn_text = f"{full_name} (@{username})"
            keyboard.append([InlineKeyboardButton(text=btn_text, callback_data=f"moderator:{user['user_id']}")])
        
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"moderators_page:{page-1}"))
        nav.append(InlineKeyboardButton(text=f"{page+1}/{(total-1)//page_size+1}", callback_data="noop"))
        if (page + 1) * page_size < total:
            nav.append(InlineKeyboardButton("➡️", callback_data=f"moderators_page:{page+1}"))
        if nav:
            keyboard.append(nav)
        
        return InlineKeyboardMarkup(inline_keyboard=keyboard)

    async def handle_moderators_page(self, query: types.CallbackQuery, state: FSMContext):
        """Обработчик переключения страниц в списке модераторов."""
        page = int(query.data.split(":")[1])
        await self._send_moderators_page(query, page, state)
        await query.answer()

    async def debug_contact(self, message: types.Message):
        print("[DEBUG] debug_contact сработал!", message)

    async def debug_any(self, message: types.Message):
        print("[DEBUG] debug_any:", message.model_dump_json() if hasattr(message, 'model_dump_json') else str(message))

    def _build_log_channels_menu(self, channels):
        keyboard = [
            [InlineKeyboardButton(text=ch['title'], callback_data=f"log_channel:{ch['id']}")]
            for ch in channels
        ]
        return InlineKeyboardMarkup(inline_keyboard=keyboard)

    def _build_violations_menu(self, violations):
        """Создает меню с нарушениями."""
        keyboard = []
        for violation in violations:
            action_text = "Забанил" if violation['decision'] == 'BAN' else "Разбанил"
            moderator_name = violation.get('moderator_username') or violation.get('moderator_full_name') or "Неизвестный"
            violator_name = violation.get('violator_username') or violation.get('violator_full_name') or "Публичный тег"
            # Обрезаем текст сообщения до 30 символов и добавляем многоточие, если длиннее
            message_text = violation.get('message_text', '')[:30] + "..." if len(violation.get('message_text', '')) > 30 else violation.get('message_text', '')
            
            keyboard.append([
                InlineKeyboardButton(
                    text=f"{moderator_name} | {action_text} | {message_text} | {violator_name}",
                    callback_data=f"log_violation:{violation['id']}"
                )
            ])
        return InlineKeyboardMarkup(inline_keyboard=keyboard)

    def _build_decision_action_menu(self, decision, violation_id):
        opposite = 'unban' if decision == 'ban' else 'ban'
        btn_text = 'Разбанить' if decision == 'ban' else 'Забанить'
        return InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text=btn_text, callback_data=f"change_decision:{violation_id}:{opposite}")]]
        )

    async def handle_logs_entry(self, message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        # Получаем каналы, где пользователь модератор или админ
        channels = await self.db.get_user_moderator_chats(user_id)
        if not channels:
            channels = await self.db.get_moderator_chats_for_user(user_id)
        if not channels:
            await message.answer("У вас нет каналов для просмотра логов.")
            return
        await state.set_state(BotStates.waiting_for_log_channel_selection)
        await state.update_data(log_channels=channels)
        await message.answer("Выберите канал:", reply_markup=self._build_log_channels_menu(channels))

    async def handle_log_channel_select(self, query: types.CallbackQuery, state: FSMContext):
        channel_id = int(query.data.split(":")[1])
        user_id = query.from_user.id
        
        # Проверяем, является ли пользователь модератором
        is_moderator = await self.db.is_chat_moderator(channel_id, user_id)
        
        # Получаем последние 10 нарушений с решениями модератора
        violations = await self.db.get_chat_decisions(
            channel_id, 
            0, 
            10, 
            moderator_id=user_id if is_moderator else None
        )
        
        await state.set_state(BotStates.waiting_for_contact)
        await state.update_data(selected_log_channel=channel_id, log_violations=violations)
        
        if not violations:
            await query.message.edit_text(
                "Нет нарушений с решениями модератора в этом канале." if not is_moderator 
                else "У вас нет решений по нарушениям в этом канале."
            )
            return
            
        await query.message.edit_text("Выберите нарушение:", reply_markup=self._build_violations_menu(violations))
        await query.answer()

    async def handle_log_violation_select(self, query: types.CallbackQuery, state: FSMContext):
        violation_id = int(query.data.split(":")[1])
        decision = await self.db.get_decision(violation_id)
        if not decision:
            await query.message.edit_text("Детали решения не найдены.")
            return
        # Получаем сообщение нарушителя
        violation = await self.db.get_rule_violation(decision['rule_violation_id'])
        msg_text = violation['message_text']
        post_link = f"https://t.me/c/{str(violation['chat_id'])[4:]}/{violation['violator_msg_id']}" if str(violation['chat_id']).startswith('-100') else None
        text = f"<b>Сообщение нарушителя:</b> {msg_text}\n"
        if post_link:
            text += f"<a href='{post_link}'>Ссылка на пост</a>\n"
        text += f"<b>Решение:</b> {decision['decision']}"
        await state.set_state(BotStates.waiting_for_contact)
        await state.update_data(selected_violation=violation_id)
        await query.message.edit_text(text, reply_markup=self._build_decision_action_menu(decision['decision'], violation_id), parse_mode="HTML", disable_web_page_preview=True)
        await query.answer()

    async def handle_change_decision(self, query: types.CallbackQuery, state: FSMContext):
        parts = query.data.split(":")
        violation_id = int(parts[1])
        new_decision = parts[2]
        moderator_id = query.from_user.id
        # Обновляем решение
        await self.db.update_decision(violation_id, new_decision)
        await query.message.edit_text(f"Решение изменено на: {new_decision}")
        await state.clear()
        await query.answer()

    def _build_notification_policies_menu(self, policies):
        keyboard = []
        for p in policies:
            status = 'включено' if p['enabled'] else 'выключено'
            btn_text = f"{p['label']} ({status})"
            keyboard.append([InlineKeyboardButton(text=btn_text, callback_data=f"toggle_policy:{p['type']}")])
        print(f"[LOG] _build_notification_policies_menu: keyboard={keyboard}")
        return InlineKeyboardMarkup(inline_keyboard=keyboard)

    async def handle_show_notification_policies(self, message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        # Получаем статусы политик
        ban_enabled = await self.db.get_notification_policy_status(user_id, 'BAN')
        notif_enabled = await self.db.get_notification_policy_status(user_id, 'NOTIFICATION')
        policies = [
            {'type': 'BAN', 'label': 'Баны', 'enabled': ban_enabled},
            {'type': 'NOTIFICATION', 'label': 'Предупреждения', 'enabled': notif_enabled}
        ]
        await state.set_state(BotStates.waiting_for_notification_policy)
        await message.answer(
            "Ваши политики уведомлений:",
            reply_markup=self._build_notification_policies_menu(policies)
        )

    async def handle_toggle_notification_policy(self, query: types.CallbackQuery, state: FSMContext):
        user_id = query.from_user.id
        policy_type = query.data.split(":")[1]
        # Получаем текущий статус
        enabled = await self.db.get_notification_policy_status(user_id, policy_type)
        # Переключаем
        await self.db.set_notification_policy_status(user_id, policy_type, not enabled)
        # Обновляем меню
        ban_enabled = await self.db.get_notification_policy_status(user_id, 'BAN')
        notif_enabled = await self.db.get_notification_policy_status(user_id, 'NOTIFICATION')
        policies = [
            {'type': 'BAN', 'label': 'Баны', 'enabled': ban_enabled},
            {'type': 'NOTIFICATION', 'label': 'Предупреждения', 'enabled': notif_enabled}
        ]
        await query.message.edit_text(
            "Ваши политики уведомлений:",
            reply_markup=self._build_notification_policies_menu(policies)
        )
        await query.answer()

    async def handle_prompt_management(self, message: types.Message, state: FSMContext):
        """Обработчик меню управления промптами."""
        user_id = message.from_user.id
        # Получаем чаты, где пользователь является админом
        admin_chats = await self.db.get_moderator_chats_for_user(user_id)
        if not admin_chats:
            await message.answer("У вас нет каналов для управления промптами.")
            return

        # Создаем клавиатуру с чатами
        keyboard = []
        for chat in admin_chats:
            keyboard.append([InlineKeyboardButton(text=chat['title'], callback_data=f"select_chat_for_prompt:{chat['id']}")])
        
        markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
        await message.answer("Выберите чат для управления промптами:", reply_markup=markup)

    async def handle_chat_selection_for_prompt(self, query: types.CallbackQuery, state: FSMContext):
        """Обработка выбора чата для промпта."""
        chat_id = int(query.data.split(":")[1])
        await state.update_data(selected_chat_id=chat_id)
        
        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Добавить промпт", callback_data="add_prompt")],
                [InlineKeyboardButton(text="Список правил", callback_data="list_prompts")]
            ]
        )
        await query.message.edit_text("Выберите действие:", reply_markup=markup)
        await query.answer()

    async def handle_add_prompt(self, query: types.CallbackQuery, state: FSMContext):
        """Начало процесса добавления промпта."""
        await query.message.edit_text(
            "Пожалуйста, отправьте текст промпта. Это должен быть текст, который будет использоваться для определения нарушения."
        )
        await state.set_state(BotStates.waiting_for_prompt_text)
        await query.answer()

    async def handle_prompt_text(self, message: types.Message, state: FSMContext):
        """Обработка введенного текста промпта."""
        await state.update_data(prompt_text=message.text)
        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Бан", callback_data="prompt_type:BAN")],
                [InlineKeyboardButton(text="Предупреждение модерам", callback_data="prompt_type:NOTIFY")],
                [InlineKeyboardButton(text="Слежение", callback_data="prompt_type:OBSERVE")]
            ]
        )
        await message.answer("Выберите тип промпта:", reply_markup=markup)
        await state.set_state(BotStates.waiting_for_prompt_type)

    async def handle_prompt_type(self, query: types.CallbackQuery, state: FSMContext):
        """Обработка выбранного типа промпта."""
        prompt_type = query.data.split(":")[1]
        await state.update_data(prompt_type=prompt_type)
        
        if prompt_type == "OBSERVE":
            # Для типа "Слежение" не нужна причина
            await query.message.edit_text("Введите объяснение для промпта (или отправьте '-' если не нужно):")
            await state.set_state(BotStates.waiting_for_prompt_explanation)
        else:
            markup = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="Тихий (без сообщения в чат)", callback_data="prompt_silent:true")],
                    [InlineKeyboardButton(text="Обычный (с сообщением в чат)", callback_data="prompt_silent:false")]
                ]
            )
            await query.message.edit_text(
                "Выберите тип уведомления:",
                reply_markup=markup
            )
            await state.set_state(BotStates.waiting_for_prompt_reason)
        await query.answer()

    async def handle_prompt_reason(self, query: types.CallbackQuery, state: FSMContext):
        """Обработка выбранного типа уведомления."""
        is_silent = query.data.split(":")[1] == "true"
        await state.update_data(is_silent=is_silent)
        
        if is_silent:
            # Если выбран тихий режим, сразу сохраняем промпт без объяснения
            await state.update_data(explanation_text="")
            await self._save_prompt(query.message, state)
        else:
            # Если выбран обычный режим, запрашиваем объяснение
            await query.message.edit_text("Введите объяснение для промпта (или отправьте '-' если не нужно):")
            await state.set_state(BotStates.waiting_for_prompt_explanation)
        await query.answer()

    async def handle_prompt_explanation(self, message: types.Message, state: FSMContext):
        """Обработка введенного текста объяснения."""
        explanation = message.text
        if explanation == "-":
            explanation = ""
        await state.update_data(explanation_text=explanation)
        await self._save_prompt(message, state)

    async def _save_prompt(self, message: types.Message, state: FSMContext):
        """Сохранение промпта в базу данных."""
        data = await state.get_data()
        prompt_text = data.get("prompt_text")
        prompt_type = data.get("prompt_type")
        is_silent = data.get("is_silent", True if prompt_type == "OBSERVE" else None)
        chat_id = data.get("selected_chat_id")
        explanation_text = data.get("explanation_text", "")
        
        try:
            # Добавляем промпт в базу данных
            await self.db.add_rule(
                chat_id=chat_id,
                rule_text=prompt_text,
                explanation_text=explanation_text,
                rule_type=prompt_type,
                is_silent=is_silent
            )
            await message.answer("Промпт успешно добавлен!", reply_markup=ADMIN_MENU)
        except Exception as e:
            print(f"[ERROR] Ошибка при добавлении промпта: {e}")
            await message.answer("Произошла ошибка при добавлении промпта.", reply_markup=ADMIN_MENU)
        
        await state.clear()

    async def handle_list_prompts(self, query: types.CallbackQuery, state: FSMContext):
        """Обработчик кнопки 'Список правил'."""
        data = await state.get_data()
        chat_id = data.get("selected_chat_id")
        
        # Получаем первую страницу правил
        rules = await self.db.get_rules_for_chat(chat_id, 0, self.config.ui.page_size)
        total_rules = await self.db.get_rules_count_for_chat(chat_id)
        
        await self._send_rules_page(query, rules, 0, total_rules, state)
        await query.answer()

    async def _send_rules_page(self, message_or_query: Union[types.Message, types.CallbackQuery], rules: List[Dict], page: int, total: int, state: FSMContext):
        """Отправляет страницу со списком правил."""
        keyboard = []
        for rule in rules:
            rule_type = {
                'BAN': '🚫 Бан',
                'NOTIFY': '⚠️ Уведомление',
                'OBSERVE': '👀 Слежение'
            }.get(rule['type'], rule['type'])
            
            btn_text = f"{rule_type}: {rule['rule_text'][:30]}..."
            keyboard.append([InlineKeyboardButton(text=btn_text, callback_data=f"view_rule:{rule['id']}")])
        
        # Добавляем навигацию
        nav = []
        total_pages = (total + self.config.ui.page_size - 1) // self.config.ui.page_size
        
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"rules_page:{page-1}"))
        nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
        if page + 1 < total_pages:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"rules_page:{page+1}"))
        if nav:
            keyboard.append(nav)
        
        markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
        text = "Список правил:" if rules else "Нет активных правил."
        
        if isinstance(message_or_query, types.Message):
            await message_or_query.answer(text, reply_markup=markup)
        else:
            await message_or_query.message.edit_text(text, reply_markup=markup)
            
        await state.update_data(rules_page=page)

    async def handle_rules_page(self, query: types.CallbackQuery, state: FSMContext):
        """Обработчик пагинации списка правил."""
        data = await state.get_data()
        chat_id = data.get("selected_chat_id")
        page = int(query.data.split(":")[1])
        
        rules = await self.db.get_rules_for_chat(chat_id, page * self.config.ui.page_size, self.config.ui.page_size)
        total_rules = await self.db.get_rules_count_for_chat(chat_id)
        
        await self._send_rules_page(query, rules, page, total_rules, state)
        await query.answer()

    async def handle_view_rule(self, query: types.CallbackQuery, state: FSMContext):
        """Обработчик просмотра деталей правила."""
        rule_id = int(query.data.split(":")[1])
        rule = await self.db.get_rule_details(rule_id)
        
        if not rule:
            await query.answer("Правило не найдено")
            return
        
        rule_type = {
            'BAN': '🚫 Бан',
            'NOTIFY': '⚠️ Уведомление',
            'OBSERVE': '👀 Слежение'
        }.get(rule['type'], rule['type'])
        
        text = (
            f"Правило #{rule['id']}\n"
            f"Тип: {rule_type}\n"
            f"Текст: {rule['rule_text']}\n"
        )
        
        if rule['explanation_text']:
            text += f"Объяснение: {rule['explanation_text']}\n"
        
        text += f"Нарушений: {rule['violation_count']}"
        
        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="Удалить", callback_data=f"delete_rule:{rule_id}"),
                    InlineKeyboardButton(text="Изменить", callback_data=f"edit_rule:{rule_id}")
                ]
            ]
        )
        
        await query.message.answer(text, reply_markup=markup)
        await query.answer()

    async def handle_delete_rule(self, query: types.CallbackQuery, state: FSMContext):
        """Обработчик деактивации правила."""
        rule_id = int(query.data.split(":")[1])
        
        # Получаем информацию о правиле перед деактивацией
        rule = await self.db.get_rule_details(rule_id)
        if not rule:
            await query.answer("Правило не найдено")
            return
            
        # Деактивируем правило
        await self.db.update_rule_status(rule_id, False)
        
        # Отправляем подтверждение
        rule_type = {
            'BAN': '🚫 Бан',
            'NOTIFY': '⚠️ Уведомление',
            'OBSERVE': '👀 Слежение'
        }.get(rule['type'], rule['type'])
        
        await query.message.edit_text(
            f"Правило деактивировано:\n"
            f"Тип: {rule_type}\n"
            f"Текст: {rule['rule_text']}\n"
            f"Объяснение: {rule['explanation_text'] if rule['explanation_text'] else 'Нет'}"
        )
        
        # Возвращаемся к списку правил
        data = await state.get_data()
        chat_id = data.get("selected_chat_id")
        page = data.get("rules_page", 0)
        
        rules = await self.db.get_rules_for_chat(chat_id, page * self.config.ui.page_size, self.config.ui.page_size)
        total_rules = await self.db.get_rules_count_for_chat(chat_id)
        
        await self._send_rules_page(query, rules, page, total_rules, state)
        await query.answer("Правило деактивировано")

    async def handle_edit_rule(self, query: types.CallbackQuery, state: FSMContext):
        """Обработчик редактирования правила."""
        rule_id = int(query.data.split(":")[1])
        
        # Получаем информацию о правиле
        rule = await self.db.get_rule_details(rule_id)
        if not rule:
            await query.answer("Правило не найдено")
            return
            
        # Сохраняем ID правила в состоянии
        await state.update_data(editing_rule_id=rule_id)
        
        # Формируем текст сообщения
        rule_type = {
            'BAN': '🚫 Бан',
            'NOTIFY': '⚠️ Уведомление',
            'OBSERVE': '👀 Слежение'
        }.get(rule['type'], rule['type'])
        
        text = (
            f"Редактирование правила:\n\n"
            f"Тип: {rule_type}\n"
            f"Правило: {rule['rule_text']}\n"
            f"Объяснение: {rule['explanation_text'] if rule['explanation_text'] else 'Нет'}\n\n"
            f"Выберите, что хотите изменить:"
        )
        
        # Создаем клавиатуру с кнопками редактирования
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="◀️ Назад", callback_data=f"view_rule:{rule_id}"),
            ],
            [
                InlineKeyboardButton(text="📝 Тип", callback_data=f"edit_rule_type:{rule_id}"),
                InlineKeyboardButton(text="📝 Правило", callback_data=f"edit_rule_text:{rule_id}"),
            ],
            [
                InlineKeyboardButton(text="📝 Объяснение", callback_data=f"edit_rule_explanation:{rule_id}"),
            ]
        ])
        
        await query.message.edit_text(text, reply_markup=markup)
        await query.answer()

    async def handle_edit_rule_type(self, query: types.CallbackQuery, state: FSMContext):
        """Обработчик изменения типа правила."""
        rule_id = int(query.data.split(":")[1])
        rule = await self.db.get_rule_details(rule_id)
        if not rule:
            await query.answer("Правило не найдено")
            return
            
        await state.update_data(editing_rule_id=rule_id)
        await state.set_state(BotStates.waiting_for_rule_type_edit)
        
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🚫 Бан", callback_data="rule_type:BAN"),
                InlineKeyboardButton(text="⚠️ Уведомление", callback_data="rule_type:NOTIFY"),
            ],
            [
                InlineKeyboardButton(text="👀 Слежение", callback_data="rule_type:OBSERVE"),
            ],
            [
                InlineKeyboardButton(text="◀️ Назад", callback_data=f"edit_rule:{rule_id}"),
            ]
        ])
        
        await query.message.edit_text(
            f"Выберите новый тип для правила:\n\n"
            f"Текущий тип: {rule['type']}\n"
            f"Текст правила: {rule['rule_text']}",
            reply_markup=markup
        )
        await query.answer()

    async def handle_edit_rule_text(self, query: types.CallbackQuery, state: FSMContext):
        """Обработчик изменения текста правила."""
        rule_id = int(query.data.split(":")[1])
        rule = await self.db.get_rule_details(rule_id)
        if not rule:
            await query.answer("Правило не найдено")
            return
            
        await state.update_data(editing_rule_id=rule_id)
        await state.set_state(BotStates.waiting_for_rule_text_edit)
        
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="◀️ Назад", callback_data=f"edit_rule:{rule_id}"),
            ]
        ])
        
        await query.message.edit_text(
            f"Введите новый текст для правила:\n\n"
            f"Текущий текст: {rule['rule_text']}",
            reply_markup=markup
        )
        await query.answer()

    async def handle_edit_rule_explanation(self, query: types.CallbackQuery, state: FSMContext):
        """Обработчик изменения объяснения правила."""
        rule_id = int(query.data.split(":")[1])
        rule = await self.db.get_rule_details(rule_id)
        if not rule:
            await query.answer("Правило не найдено")
            return
            
        await state.update_data(editing_rule_id=rule_id)
        await state.set_state(BotStates.waiting_for_rule_explanation_edit)
        
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="◀️ Назад", callback_data=f"edit_rule:{rule_id}"),
            ]
        ])
        
        current_explanation = rule['explanation_text'] if rule['explanation_text'] else "Нет"
        await query.message.edit_text(
            f"Введите новое объяснение для правила:\n\n"
            f"Текущее объяснение: {current_explanation}",
            reply_markup=markup
        )
        await query.answer()

    async def handle_rule_type_edit(self, query: types.CallbackQuery, state: FSMContext):
        """Обработчик сохранения нового типа правила."""
        data = await state.get_data()
        rule_id = data.get("editing_rule_id")
        if not rule_id:
            await query.answer("Ошибка: правило не найдено")
            return
            
        new_type = query.data.split(":")[1]
        rule = await self.db.get_rule_details(rule_id)
        
        # Обновляем тип правила
        await self.db.update_rule(rule_id, rule['rule_text'], rule['explanation_text'], new_type)
        
        # Возвращаемся к редактированию
        await query.answer("Тип правила обновлен")
        await self.handle_edit_rule(query, state)

    async def handle_rule_text_edit(self, message: types.Message, state: FSMContext):
        """Обработчик сохранения нового текста правила."""
        data = await state.get_data()
        rule_id = data.get("editing_rule_id")
        if not rule_id:
            await message.answer("Ошибка: правило не найдено")
            return
            
        new_text = message.text
        rule = await self.db.get_rule_details(rule_id)
        
        # Обновляем текст правила
        await self.db.update_rule(rule_id, new_text, rule['explanation_text'], rule['type'])
        
        # Возвращаемся к редактированию
        await state.clear()
        
        # Отправляем сообщение об успешном обновлении с меню
        await message.answer("✅ Текст правила успешно обновлен", reply_markup=ADMIN_MENU)
        
        # Получаем информацию о правиле
        rule = await self.db.get_rule_details(rule_id)
        
        # Формируем текст сообщения
        rule_type = {
            'BAN': '🚫 Бан',
            'NOTIFY': '⚠️ Уведомление',
            'OBSERVE': '👀 Слежение'
        }.get(rule['type'], rule['type'])
        
        text = (
            f"Редактирование правила:\n\n"
            f"Тип: {rule_type}\n"
            f"Правило: {rule['rule_text']}\n"
            f"Объяснение: {rule['explanation_text'] if rule['explanation_text'] else 'Нет'}\n\n"
            f"Выберите, что хотите изменить:"
        )
        
        # Создаем клавиатуру с кнопками редактирования
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="◀️ Назад", callback_data=f"view_rule:{rule_id}"),
            ],
            [
                InlineKeyboardButton(text="📝 Тип", callback_data=f"edit_rule_type:{rule_id}"),
                InlineKeyboardButton(text="📝 Правило", callback_data=f"edit_rule_text:{rule_id}"),
            ],
            [
                InlineKeyboardButton(text="📝 Объяснение", callback_data=f"edit_rule_explanation:{rule_id}"),
            ]
        ])
        
        await message.answer(text, reply_markup=markup)

    async def handle_rule_explanation_edit(self, message: types.Message, state: FSMContext):
        """Обработчик сохранения нового объяснения правила."""
        data = await state.get_data()
        rule_id = data.get("editing_rule_id")
        if not rule_id:
            await message.answer("Ошибка: правило не найдено")
            return
            
        new_explanation = message.text
        rule = await self.db.get_rule_details(rule_id)
        
        # Обновляем объяснение правила
        await self.db.update_rule(rule_id, rule['rule_text'], new_explanation, rule['type'])
        
        # Возвращаемся к редактированию
        await state.clear()
        
        # Отправляем сообщение об успешном обновлении с меню
        await message.answer("✅ Объяснение правила успешно обновлено", reply_markup=ADMIN_MENU)
        
        # Получаем информацию о правиле
        rule = await self.db.get_rule_details(rule_id)
        
        # Формируем текст сообщения
        rule_type = {
            'BAN': '🚫 Бан',
            'NOTIFY': '⚠️ Уведомление',
            'OBSERVE': '👀 Слежение'
        }.get(rule['type'], rule['type'])
        
        text = (
            f"Редактирование правила:\n\n"
            f"Тип: {rule_type}\n"
            f"Правило: {rule['rule_text']}\n"
            f"Объяснение: {rule['explanation_text'] if rule['explanation_text'] else 'Нет'}\n\n"
            f"Выберите, что хотите изменить:"
        )
        
        # Создаем клавиатуру с кнопками редактирования
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="◀️ Назад", callback_data=f"view_rule:{rule_id}"),
            ],
            [
                InlineKeyboardButton(text="📝 Тип", callback_data=f"edit_rule_type:{rule_id}"),
                InlineKeyboardButton(text="📝 Правило", callback_data=f"edit_rule_text:{rule_id}"),
            ],
            [
                InlineKeyboardButton(text="📝 Объяснение", callback_data=f"edit_rule_explanation:{rule_id}"),
            ]
        ])
        
        await message.answer(text, reply_markup=markup)

    async def handle_recent_violations_entry(self, message: types.Message, state: FSMContext):
        """Обработчик входа в раздел последних нарушений."""
        keyboard = [
            [InlineKeyboardButton(text="Бан", callback_data="violation_type:BAN")],
            [InlineKeyboardButton(text="Предупреждение", callback_data="violation_type:NOTIFY")]
        ]
        
        # Добавляем кнопку "Наблюдение" только для администраторов
        user_role = await self.get_user_role(message.from_user.id)
        if user_role == UserRole.ADMIN:
            keyboard.append([InlineKeyboardButton(text="Наблюдение", callback_data="violation_type:OBSERVE")])
            
        markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
        await message.answer("Выберите тип нарушения:", reply_markup=markup)
        await state.set_state(BotStates.waiting_for_violation_type)

    async def handle_violation_type_select(self, query: types.CallbackQuery, state: FSMContext):
        """Обработчик выбора типа нарушения."""
        violation_type = query.data.split(":")[1]
        user_id = query.from_user.id
        
        # Получаем чаты, где пользователь является модератором
        moderator_chats = await self.db.get_user_moderator_chats(user_id)
        if not moderator_chats:
            await query.message.edit_text("У вас нет чатов для просмотра нарушений.")
            return
            
        violations_found = False
        # Получаем последние нарушения для каждого чата
        for chat in moderator_chats:
            # Получаем last_seen_timestamp для каждого правила
            rules = await self.db.get_rules_for_chat(chat['id'], 0, 100)  # Получаем все правила
            rules = [r for r in rules if r['type'] == violation_type]  # Фильтруем по типу
            
            for rule in rules:
                last_seen = await self.db.get_last_seen(user_id, rule['id'])
                if last_seen is None:
                    last_seen = datetime.min
                
                # Получаем новые нарушения
                violations = await self.db.get_new_violations_per_user(rule['id'], last_seen)
                if not violations:
                    continue
                
                violations_found = True
                # Отправляем сообщение нарушителя
                for violation in violations:
                    violator_msg = await self.db.get_violator_message(violation['violator_msg_id'])
                    if not violator_msg:
                        continue
                        
                    # Пересылаем сообщение нарушителя
                    try:
                        await query.message.answer(
                            "Сообщение нарушителя:"
                        )
                        await self.bot.forward_message(
                            query.from_user.id,
                            violator_msg['chat_id'],
                            violator_msg['post_id']
                        )
                    except Exception as e:
                        print(f"[DEBUG] Failed to forward message: {e}")
                        # Если не удалось переслать, отправляем текст
                        await query.message.answer(
                            f"Сообщение нарушителя:\n{violator_msg['text']}"
                        )
                    
                    # Отправляем информацию о правиле и кнопки действий
                    keyboard = []
                    if rule['type'] == 'NOTIFY':
                        keyboard.append([InlineKeyboardButton(text="Забанить", callback_data=f"violation_action:{violation['id']}:BAN")])
                    else:
                        keyboard.append([InlineKeyboardButton(text="Разбанить", callback_data=f"violation_action:{violation['id']}:UNBAN")])
                    
                    # Добавляем кнопку "Наблюдение" только для администраторов
                    user_role = await self.get_user_role(user_id)
                    if user_role == UserRole.ADMIN:
                        keyboard.append([InlineKeyboardButton(text="Наблюдение", callback_data=f"violation_action:{violation['id']}:WATCH")])
                    
                    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
                    await query.message.answer(
                        f"Тип правила: {rule['type']}\n"
                        f"Текст правила: {rule['rule_text']}",
                        reply_markup=markup
                    )
                    
                    # Обновляем last_seen_timestamp
                    await self.db.set_last_seen(user_id, rule['id'], violation['detected_at'])
        
        if not violations_found:
            await query.message.edit_text(f"Новых нарушений типа {violation_type} не найдено.")
                    
        await state.clear()
        await query.answer()

    async def handle_violation_action(self, query: types.CallbackQuery, state: FSMContext):
        """Обработчик действий с нарушением."""
        parts = query.data.split(":")
        violation_id = int(parts[1])
        action = parts[2]
        
        # Получаем информацию о нарушении
        violation = await self.db.get_rule_violation(violation_id)
        if not violation:
            await query.answer("Нарушение не найдено")
            return
            
        # Обновляем решение
        await self.db.update_decision(violation_id, action)
        
        # Добавляем запись в rule_violation_decision
        await self.db.add_decision(
            rule_violation_id=violation_id,
            moderator_id=query.from_user.id,
            decision=action
        )
        
        # Выполняем действие бана/разбана
        try:
            if action == 'BAN':
                await self.bot.ban_chat_member(violation['chat_id'], violation['violator_id'])
            elif action == 'UNBAN':
                await self.bot.unban_chat_member(violation['chat_id'], violation['violator_id'])
        except Exception as e:
            print(f"[DEBUG] Failed to {action.lower()} user: {e}")
            await query.answer(f"Не удалось {action.lower()} пользователя: {str(e)}")
            return
        
        # Создаем кнопку для обратного действия
        keyboard = []
        if action == 'BAN':
            keyboard.append([InlineKeyboardButton(text="Разбанить", callback_data=f"violation_action:{violation_id}:UNBAN")])
        else:
            keyboard.append([InlineKeyboardButton(text="Забанить", callback_data=f"violation_action:{violation_id}:BAN")])
        
        markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
        
        # Обновляем сообщение с кнопкой
        action_text = "Забанен" if action == 'BAN' else "Разбанен"
        await query.message.edit_text(
            f"Статус: {action_text}",
            reply_markup=markup
        )
        await query.answer()

    async def handle_message_monitoring(self, message: types.Message):
        """Обработчик мониторинга сообщений в чате."""
        print(f"[DEBUG] ====== Start processing message {message.message_id} ======")
        print(f"[DEBUG] Message received in chat {message.chat.id} ({message.chat.title})")
        print(f"[DEBUG] Message type: {message.content_type}")
        print(f"[DEBUG] Has video: {bool(message.video)}")
        print(f"[DEBUG] Has photo: {bool(message.photo)}")
        print(f"[DEBUG] Has audio: {bool(message.audio)}")
        print(f"[DEBUG] Has voice: {bool(message.voice)}")
        print(f"[DEBUG] Text: {message.text or message.caption or 'None'}")
        
        # Проверяем, что бот имеет права на чтение сообщений
        chat = await self.db.get_chat(message.chat.id)
        if not chat or not chat['can_read_messages']:
            print(f"[DEBUG] Skipping message - no read permissions or chat not found")
            return

        # Добавляем информацию о пользователе
        username = message.from_user.username
        full_name = message.from_user.full_name
        await self.db.add_or_update_user(message.from_user.id, username, full_name)
        print(f"[DEBUG] User info added/updated: id={message.from_user.id}, username={username}, full_name={full_name}")

        # Собираем информацию о медиа в сообщении
        media_info = {
            'message_id': message.message_id,
            'chat_id': message.chat.id,
            'user_id': message.from_user.id,
            'text': message.text or message.caption or '',
            'has_video': bool(message.video),
            'has_photo': bool(message.photo),
            'has_audio': bool(message.audio or message.voice),
            'image_uuids': [],
            'audio_uuids': []
        }
        print(f"[DEBUG] Initial media_info: {media_info}")

        # Обработка видео
        if message.video:
            print(f"[DEBUG] Processing video message")
            with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_file:
                video_path = temp_file.name
                try:
                    print(f"[DEBUG] Downloading video to {video_path}")
                    # Получаем файл и скачиваем его
                    file = await self.bot.get_file(message.video.file_id)
                    await self.bot.download_file(file.file_path, video_path)
                    
                    # Извлекаем кадр и аудио
                    frame_data = await self.extract_video_frame(video_path)
                    audio_data = await self.extract_video_audio(video_path)
                    
                    # Сохраняем в БД
                    frame_uuid = await self.db.store_image(frame_data)
                    audio_uuid = await self.db.store_audio(audio_data)
                    
                    media_info['image_uuids'].append(frame_uuid)
                    media_info['audio_uuids'].append(audio_uuid)
                finally:
                    if os.path.exists(video_path):
                        os.unlink(video_path)

        # Обработка фото
        if message.photo:
            for photo in message.photo:
                with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as temp_file:
                    temp_path = temp_file.name
                    try:
                        # Получаем файл и скачиваем его
                        file = await self.bot.get_file(photo.file_id)
                        await self.bot.download_file(file.file_path, temp_path)
                        # Читаем файл
                        with open(temp_path, 'rb') as f:
                            photo_data = f.read()
                        # Сохраняем в БД
                        photo_uuid = await self.db.store_image(photo_data)
                        media_info['image_uuids'].append(photo_uuid)
                    finally:
                        if os.path.exists(temp_path):
                            os.unlink(temp_path)

        # Обработка аудио/голосовых сообщений
        if message.audio or message.voice:
            audio = message.audio or message.voice
            with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False) as temp_file:
                temp_path = temp_file.name
                try:
                    # Получаем файл и скачиваем его
                    file = await self.bot.get_file(audio.file_id)
                    await self.bot.download_file(file.file_path, temp_path)
                    # Читаем файл
                    with open(temp_path, 'rb') as f:
                        audio_data = f.read()
                    # Сохраняем в БД
                    audio_uuid = await self.db.store_audio(audio_data)
                    media_info['audio_uuids'].append(audio_uuid)
                finally:
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)

        # Отправляем информацию в соответствующие очереди
        if media_info['image_uuids']:
            await self.rabbitmq_channel.default_exchange.publish(
                aio_pika.Message(body=json.dumps({
                    **media_info,
                    'image_uuids': [str(uuid) for uuid in media_info['image_uuids']],
                    'audio_uuids': [str(uuid) for uuid in media_info['audio_uuids']]
                }).encode()),
                routing_key="multimedia.images"
            )

        if media_info['audio_uuids']:
            await self.rabbitmq_channel.default_exchange.publish(
                aio_pika.Message(body=json.dumps({
                    **media_info,
                    'image_uuids': [str(uuid) for uuid in media_info['image_uuids']],
                    'audio_uuids': [str(uuid) for uuid in media_info['audio_uuids']]
                }).encode()),
                routing_key="multimedia.audio"
            )

        if media_info['text']:
            await self.rabbitmq_channel.default_exchange.publish(
                aio_pika.Message(body=json.dumps({
                    **media_info,
                    'image_uuids': [str(uuid) for uuid in media_info['image_uuids']],
                    'audio_uuids': [str(uuid) for uuid in media_info['audio_uuids']]
                }).encode()),
                routing_key="multimedia.text"
            )
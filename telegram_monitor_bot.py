import asyncio
import json
import logging
import sqlite3
import hashlib
import time
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict, field
from enum import Enum
import httpx
from bs4 import BeautifulSoup
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)

# Проверка наличия OpenAI
try:
    from openai import OpenAI

    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("⚠️ OpenAI не установлен. Установите: pip install openai")

# ============= КОНФИГУРАЦИЯ =============
TELEGRAM_TOKEN = "8595773105:AAGRN9C2P_rLBjBXWd5XqmPYxsVEiFzmEUI"  # Замени на свой токен
OPENAI_API_KEY = "sk-aitunnel-EvDg3cE6gN23lyotZlzhPH4jCkqESIuu"  # Опционально: для ИИ-персонализации

# Настройки мониторинга
CHECK_INTERVAL_MINUTES = 15
DATABASE_FILE = "monitor_bot.db"
SNAPSHOTS_DIR = Path("snapshots")
SNAPSHOTS_DIR.mkdir(exist_ok=True)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ============= МОДЕЛИ ДАННЫХ =============
class NotificationType(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    SUCCESS = "success"
    CRITICAL = "critical"


class UserPreferences:
    """Предпочтения пользователя"""

    def __init__(self, user_id: int):
        self.user_id = user_id
        self.notification_types = {
            NotificationType.INFO: True,
            NotificationType.WARNING: True,
            NotificationType.ERROR: True,
            NotificationType.SUCCESS: True,
            NotificationType.CRITICAL: True
        }
        self.min_priority = "medium"  # low, medium, high
        self.ai_personalization = True
        self.language = "ru"
        self.notification_frequency = "realtime"  # realtime, hourly, daily

    def to_dict(self):
        return {
            'user_id': self.user_id,
            'notification_types': json.dumps({k.value: v for k, v in self.notification_types.items()}),
            'min_priority': self.min_priority,
            'ai_personalization': self.ai_personalization,
            'language': self.language,
            'notification_frequency': self.notification_frequency
        }

    @classmethod
    def from_dict(cls, data: Dict):
        prefs = cls(data['user_id'])
        if data.get('notification_types'):
            types = json.loads(data['notification_types'])
            for k, v in types.items():
                prefs.notification_types[NotificationType(k)] = v
        prefs.min_priority = data.get('min_priority', 'medium')
        prefs.ai_personalization = data.get('ai_personalization', True)
        prefs.language = data.get('language', 'ru')
        prefs.notification_frequency = data.get('notification_frequency', 'realtime')
        return prefs


@dataclass
class MonitoredSite:
    """Мониторимый сайт"""
    url: str
    user_id: int
    created_at: datetime
    last_check: Optional[datetime] = None
    last_hash: Optional[str] = None
    is_active: bool = True
    check_interval: int = CHECK_INTERVAL_MINUTES
    custom_name: Optional[str] = None

    def to_dict(self):
        return {
            'url': self.url,
            'user_id': self.user_id,
            'created_at': self.created_at.isoformat(),
            'last_check': self.last_check.isoformat() if self.last_check else None,
            'last_hash': self.last_hash,
            'is_active': self.is_active,
            'check_interval': self.check_interval,
            'custom_name': self.custom_name
        }

    @classmethod
    def from_dict(cls, data: Dict):
        return cls(
            url=data['url'],
            user_id=data['user_id'],
            created_at=datetime.fromisoformat(data['created_at']),
            last_check=datetime.fromisoformat(data['last_check']) if data.get('last_check') else None,
            last_hash=data.get('last_hash'),
            is_active=data.get('is_active', True),
            check_interval=data.get('check_interval', CHECK_INTERVAL_MINUTES),
            custom_name=data.get('custom_name')
        )


@dataclass
class Notification:
    """Уведомление"""
    user_id: int
    site_url: str
    type: NotificationType
    title: str
    message: str
    priority: str  # low, medium, high
    changes: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    sent: bool = False
    recommendation: Optional[str] = None

    def to_dict(self):
        return {
            'user_id': self.user_id,
            'site_url': self.site_url,
            'type': self.type.value,
            'title': self.title,
            'message': self.message,
            'priority': self.priority,
            'changes': json.dumps(self.changes),
            'created_at': self.created_at.isoformat(),
            'sent': self.sent,
            'recommendation': self.recommendation
        }


# ============= БАЗА ДАННЫХ =============
class Database:
    """Работа с базой данных"""

    def __init__(self, db_file: str):
        self.db_file = db_file
        self.init_db()

    def get_connection(self):
        return sqlite3.connect(self.db_file)

    def init_db(self):
        """Инициализация базы данных"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Таблица пользователей
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    created_at TIMESTAMP,
                    preferences TEXT,
                    is_active INTEGER DEFAULT 1
                )
            """)

            # Таблица мониторимых сайтов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS monitored_sites (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT,
                    user_id INTEGER,
                    created_at TIMESTAMP,
                    last_check TIMESTAMP,
                    last_hash TEXT,
                    is_active INTEGER DEFAULT 1,
                    check_interval INTEGER DEFAULT 15,
                    custom_name TEXT,
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    UNIQUE(url, user_id)
                )
            """)

            # Таблица уведомлений
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS notifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    site_url TEXT,
                    type TEXT,
                    title TEXT,
                    message TEXT,
                    priority TEXT,
                    changes TEXT,
                    created_at TIMESTAMP,
                    sent INTEGER DEFAULT 0,
                    recommendation TEXT,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)

            # Таблица снимков
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    site_url TEXT,
                    user_id INTEGER,
                    hash TEXT,
                    content TEXT,
                    created_at TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)

            conn.commit()
            logger.info("✅ База данных инициализирована")

    def add_user(self, user_id: int, username: str, first_name: str, last_name: str = None):
        """Добавление пользователя"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO users (user_id, username, first_name, last_name, created_at, is_active)
                    VALUES (?, ?, ?, ?, ?, 1)
                """, (user_id, username, first_name, last_name, datetime.now()))
                conn.commit()
                logger.info(f"Пользователь {user_id} добавлен в базу")
        except Exception as e:
            logger.error(f"Ошибка добавления пользователя: {e}")

    def get_user_preferences(self, user_id: int) -> UserPreferences:
        """Получение предпочтений пользователя"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT preferences FROM users WHERE user_id = ?", (user_id,))
                row = cursor.fetchone()

                if row and row[0]:
                    try:
                        prefs_data = json.loads(row[0])
                        prefs_data['user_id'] = user_id
                        return UserPreferences.from_dict(prefs_data)
                    except:
                        pass
        except Exception as e:
            logger.error(f"Ошибка получения предпочтений: {e}")

        return UserPreferences(user_id)

    def save_user_preferences(self, prefs: UserPreferences):
        """Сохранение предпочтений пользователя"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE users SET preferences = ? WHERE user_id = ?
                """, (json.dumps(prefs.to_dict()), prefs.user_id))
                conn.commit()
        except Exception as e:
            logger.error(f"Ошибка сохранения предпочтений: {e}")

    def add_monitored_site(self, site: MonitoredSite):
        """Добавление сайта в мониторинг"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO monitored_sites 
                    (url, user_id, created_at, last_check, last_hash, is_active, check_interval, custom_name)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    site.url, site.user_id, site.created_at,
                    site.last_check, site.last_hash, 1 if site.is_active else 0,
                    site.check_interval, site.custom_name
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"Ошибка добавления сайта: {e}")

    def get_user_sites(self, user_id: int, active_only: bool = True) -> List[MonitoredSite]:
        """Получение сайтов пользователя"""
        sites = []
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                query = "SELECT url, user_id, created_at, last_check, last_hash, is_active, check_interval, custom_name FROM monitored_sites WHERE user_id = ?"
                params = [user_id]
                if active_only:
                    query += " AND is_active = 1"
                cursor.execute(query, params)

                for row in cursor.fetchall():
                    sites.append(MonitoredSite(
                        url=row[0],
                        user_id=row[1],
                        created_at=datetime.fromisoformat(row[2]) if isinstance(row[2], str) else row[2],
                        last_check=datetime.fromisoformat(row[3]) if row[3] and isinstance(row[3], str) else row[3],
                        last_hash=row[4],
                        is_active=bool(row[5]),
                        check_interval=row[6],
                        custom_name=row[7]
                    ))
        except Exception as e:
            logger.error(f"Ошибка получения сайтов: {e}")

        return sites

    def get_all_active_sites(self) -> List[MonitoredSite]:
        """Получение всех активных сайтов"""
        sites = []
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT url, user_id, created_at, last_check, last_hash, is_active, check_interval, custom_name 
                    FROM monitored_sites 
                    WHERE is_active = 1
                """)

                for row in cursor.fetchall():
                    sites.append(MonitoredSite(
                        url=row[0],
                        user_id=row[1],
                        created_at=datetime.fromisoformat(row[2]) if isinstance(row[2], str) else row[2],
                        last_check=datetime.fromisoformat(row[3]) if row[3] and isinstance(row[3], str) else row[3],
                        last_hash=row[4],
                        is_active=bool(row[5]),
                        check_interval=row[6],
                        custom_name=row[7]
                    ))
        except Exception as e:
            logger.error(f"Ошибка получения активных сайтов: {e}")

        return sites

    def remove_monitored_site(self, user_id: int, url: str):
        """Удаление сайта из мониторинга"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE monitored_sites SET is_active = 0 WHERE user_id = ? AND url = ?
                """, (user_id, url))
                conn.commit()
        except Exception as e:
            logger.error(f"Ошибка удаления сайта: {e}")

    def update_site_check(self, user_id: int, url: str, last_hash: str):
        """Обновление времени последней проверки"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE monitored_sites 
                    SET last_check = ?, last_hash = ?
                    WHERE user_id = ? AND url = ?
                """, (datetime.now(), last_hash, user_id, url))
                conn.commit()
        except Exception as e:
            logger.error(f"Ошибка обновления проверки: {e}")

    def add_notification(self, notification: Notification):
        """Добавление уведомления"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO notifications 
                    (user_id, site_url, type, title, message, priority, changes, created_at, sent, recommendation)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    notification.user_id, notification.site_url, notification.type.value,
                    notification.title, notification.message, notification.priority,
                    json.dumps(notification.changes), notification.created_at,
                    1 if notification.sent else 0, notification.recommendation
                ))
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка добавления уведомления: {e}")
            return None

    def save_snapshot(self, site_url: str, user_id: int, hash_value: str, content: str):
        """Сохранение снимка"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO snapshots (site_url, user_id, hash, content, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (site_url, user_id, hash_value, content[:10000], datetime.now()))
                conn.commit()
        except Exception as e:
            logger.error(f"Ошибка сохранения снимка: {e}")

    def get_last_snapshot(self, user_id: int, site_url: str) -> Optional[Tuple[str, str]]:
        """Получение последнего снимка"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT hash, content FROM snapshots 
                    WHERE user_id = ? AND site_url = ? 
                    ORDER BY created_at DESC LIMIT 1
                """, (user_id, site_url))
                row = cursor.fetchone()
                if row:
                    return row[0], row[1]
        except Exception as e:
            logger.error(f"Ошибка получения снимка: {e}")
        return None


# ============= ИИ-АНАЛИЗАТОР =============
class PersonalizationAI:
    """ИИ для персонализации уведомлений"""

    def __init__(self, api_key: str = None):
        self.enabled = bool(api_key) and OPENAI_AVAILABLE
        self.client = None

        if self.enabled:
            try:
                self.client = OpenAI(api_key=api_key)
                logger.info("✅ ИИ-персонализация включена")
            except Exception as e:
                logger.error(f"Ошибка инициализации OpenAI: {e}")
                self.enabled = False

    async def personalize_notification(self, user_id: int, site_url: str,
                                       changes: List[str], user_prefs: UserPreferences) -> Notification:
        """Персонализация уведомления"""

        # Создаем базовое уведомление
        priority = self._calculate_priority(changes)

        # Проверяем, нужно ли отправлять уведомление по приоритету
        if not self._should_send_by_priority(priority, user_prefs.min_priority):
            return None

        if self.enabled and user_prefs.ai_personalization:
            try:
                return await self._ai_personalize(user_id, site_url, changes, user_prefs, priority)
            except Exception as e:
                logger.error(f"Ошибка ИИ-персонализации: {e}")

        # Стандартное уведомление
        return self._create_standard_notification(site_url, changes, priority, user_id)

    def _calculate_priority(self, changes: List[str]) -> str:
        """Расчет приоритета изменений"""
        high_keywords = ['срочно', 'важно', 'критично', 'уязвимость', 'безопасность', 'критический']
        medium_keywords = ['новый', 'обновление', 'изменение', 'добавлено', 'опубликован']

        changes_text = ' '.join(changes).lower()

        for kw in high_keywords:
            if kw in changes_text:
                return 'high'

        for kw in medium_keywords:
            if kw in changes_text:
                return 'medium'

        return 'low'

    def _should_send_by_priority(self, priority: str, min_priority: str) -> bool:
        """Проверка, нужно ли отправлять уведомление по приоритету"""
        priority_order = {'low': 1, 'medium': 2, 'high': 3}
        min_order = priority_order.get(min_priority, 1)
        return priority_order.get(priority, 1) >= min_order

    async def _ai_personalize(self, user_id: int, site_url: str, changes: List[str],
                              user_prefs: UserPreferences, priority: str) -> Notification:
        """ИИ-персонализация уведомления"""

        changes_text = "\n".join([f"- {c}" for c in changes[:5]])

        prompt = f"""
        Создай персонализированное уведомление для пользователя о изменениях на сайте {site_url}.

        Изменения:
        {changes_text}

        Создай уведомление в формате JSON:
        {{
            "type": "info/warning/error/success/critical",
            "title": "Заголовок уведомления",
            "message": "Подробное сообщение",
            "recommendation": "Рекомендация для пользователя"
        }}

        Тип выбирай исходя из важности изменений.
        """

        response = self.client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "Ты помощник для персонализации уведомлений. Отвечай только JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=300
        )

        result = json.loads(response.choices[0].message.content)

        # Определяем тип уведомления
        type_map = {
            'info': NotificationType.INFO,
            'warning': NotificationType.WARNING,
            'error': NotificationType.ERROR,
            'success': NotificationType.SUCCESS,
            'critical': NotificationType.CRITICAL
        }

        return Notification(
            user_id=user_id,
            site_url=site_url,
            type=type_map.get(result.get('type', 'info'), NotificationType.INFO),
            title=result.get('title', 'Изменения на сайте'),
            message=result.get('message', ''),
            priority=priority,
            changes=changes,
            recommendation=result.get('recommendation')
        )

    def _create_standard_notification(self, site_url: str, changes: List[str],
                                      priority: str, user_id: int) -> Notification:
        """Создание стандартного уведомления"""

        # Определяем тип по приоритету
        type_map = {
            'high': NotificationType.CRITICAL,
            'medium': NotificationType.WARNING,
            'low': NotificationType.INFO
        }

        # Формируем сообщение
        message = f"Обнаружены изменения на сайте {site_url}\n\n"

        if changes:
            message += "Основные изменения:\n"
            for change in changes[:5]:
                message += f"• {change}\n"

        return Notification(
            user_id=user_id,
            site_url=site_url,
            type=type_map.get(priority, NotificationType.INFO),
            title=f"Изменения на {site_url}",
            message=message,
            priority=priority,
            changes=changes
        )


# ============= МОНИТОРИНГ САЙТОВ =============
class SiteMonitor:
    """Мониторинг сайтов"""

    def __init__(self, db: Database, ai: PersonalizationAI):
        self.db = db
        self.ai = ai

    async def check_site(self, site: MonitoredSite) -> Optional[List[str]]:
        """Проверка сайта на изменения"""
        try:
            # Загружаем страницу
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                response = await client.get(
                    site.url,
                    headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                )

                if response.status_code != 200:
                    logger.warning(f"Ошибка {response.status_code} при проверке {site.url}")
                    return None

                # Извлекаем значимый контент
                content = self.extract_content(response.text)
                current_hash = hashlib.sha256(content.encode()).hexdigest()

                # Получаем последний снимок
                last_hash, last_content = self.db.get_last_snapshot(site.user_id, site.url)

                # Сохраняем новый снимок
                self.db.save_snapshot(site.url, site.user_id, current_hash, content)
                self.db.update_site_check(site.user_id, site.url, current_hash)

                # Сравниваем с предыдущим
                if last_hash and last_hash != current_hash and last_content:
                    changes = self.detect_changes(last_content, content)
                    return changes

        except Exception as e:
            logger.error(f"Ошибка проверки {site.url}: {e}")

        return None

    def extract_content(self, html: str) -> str:
        """Извлечение значимого контента"""
        try:
            soup = BeautifulSoup(html, 'html.parser')

            # Удаляем скрипты и стили
            for tag in soup(['script', 'style', 'meta', 'link', 'noscript', 'iframe']):
                tag.decompose()

            # Извлекаем текст
            text = soup.get_text(separator=' ', strip=True)

            # Нормализуем пробелы
            text = ' '.join(text.split())

            return text[:10000]  # Ограничиваем размер
        except Exception as e:
            logger.error(f"Ошибка извлечения контента: {e}")
            return html[:10000]

    def detect_changes(self, old_content: str, new_content: str) -> List[str]:
        """Детектирование изменений"""
        changes = []

        try:
            # Простое сравнение
            if len(new_content) != len(old_content):
                changes.append(f"Изменен размер контента: {len(old_content)} → {len(new_content)} символов")

            # Ищем новые ключевые слова
            old_words = set(old_content.split()[:500])
            new_words = set(new_content.split()[:500])

            added_words = new_words - old_words
            if added_words:
                changes.append(f"Добавлены новые слова: {', '.join(list(added_words)[:5])}")

            # Проверяем наличие важных ключевых слов
            important_keywords = ['новый', 'обновление', 'важно', 'срочно', 'релиз']
            found_keywords = [kw for kw in important_keywords if kw in new_content.lower()]
            if found_keywords and found_keywords not in old_content.lower():
                changes.append(f"Обнаружены ключевые слова: {', '.join(found_keywords)}")

        except Exception as e:
            logger.error(f"Ошибка детектирования изменений: {e}")

        return changes[:10] if changes else ["Обнаружены изменения в содержимом сайта"]


# ============= TELEGRAM БОТ =============
class TelegramMonitorBot:
    """Основной класс бота"""

    def __init__(self, token: str, openai_key: str = None):
        self.token = token
        self.db = Database(DATABASE_FILE)
        self.ai = PersonalizationAI(openai_key)
        self.monitor = SiteMonitor(self.db, self.ai)
        self.scheduler = AsyncIOScheduler()
        self.application = None

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        user = update.effective_user

        # Регистрируем пользователя
        self.db.add_user(
            user.id,
            user.username or "",
            user.first_name or "",
            user.last_name or ""
        )

        welcome_text = (
            f"👋 Привет, {user.first_name}!\n\n"
            f"Я бот для мониторинга изменений на сайтах. Я помогу тебе:\n"
            f"• Отслеживать изменения на любых сайтах\n"
            f"• Получать уведомления о важных обновлениях\n"
            f"• Настраивать персонализированные уведомления\n\n"
            f"📋 Доступные команды:\n"
            f"/subscribe - Подписаться на уведомления\n"
            f"/unsubscribe - Отписаться от уведомлений\n"
            f"/status - Проверить статус мониторинга\n"
            f"/monitor [url] - Начать мониторинг сайта\n"
            f"/stop [url] - Остановить мониторинг\n"
            f"/preferences - Настроить уведомления\n"
            f"/help - Помощь"
        )

        await update.message.reply_text(welcome_text)

    async def subscribe(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Подписка на уведомления"""
        user_id = update.effective_user.id

        prefs = self.db.get_user_preferences(user_id)
        prefs.notification_types = {
            NotificationType.INFO: True,
            NotificationType.WARNING: True,
            NotificationType.ERROR: True,
            NotificationType.SUCCESS: True,
            NotificationType.CRITICAL: True
        }

        self.db.save_user_preferences(prefs)

        await update.message.reply_text(
            "✅ Вы успешно подписались на уведомления!\n\n"
            "Теперь вы будете получать уведомления обо всех изменениях "
            "на отслеживаемых сайтах."
        )

    async def unsubscribe(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отписка от уведомлений"""
        user_id = update.effective_user.id

        prefs = self.db.get_user_preferences(user_id)
        for notif_type in NotificationType:
            prefs.notification_types[notif_type] = False

        self.db.save_user_preferences(prefs)

        await update.message.reply_text(
            "❌ Вы отписались от уведомлений.\n\n"
            "Вы больше не будете получать уведомления. "
            "Чтобы снова подписаться, используйте /subscribe"
        )

    async def status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Проверка статуса мониторинга"""
        user_id = update.effective_user.id
        sites = self.db.get_user_sites(user_id)
        prefs = self.db.get_user_preferences(user_id)

        if not sites:
            await update.message.reply_text(
                "📊 У вас нет активных мониторингов.\n\n"
                "Чтобы добавить сайт, используйте:\n"
                "/monitor https://example.com"
            )
            return

        status_text = "📊 *Статус мониторинга*\n\n"

        for site in sites:
            status_text += f"🔍 *{site.custom_name or site.url}*\n"
            status_text += f"   Интервал: {site.check_interval} мин\n"
            if site.last_check:
                last_check = site.last_check.strftime('%d.%m.%Y %H:%M')
                status_text += f"   Последняя проверка: {last_check}\n"
            status_text += f"   Статус: {'✅ Активен' if site.is_active else '❌ Остановлен'}\n\n"

        # Информация о предпочтениях
        active_types = [t.value for t, enabled in prefs.notification_types.items() if enabled]
        status_text += f"📨 *Типы уведомлений*: {', '.join(active_types) if active_types else 'нет'}\n"
        status_text += f"⚡ *Минимальный приоритет*: {prefs.min_priority}\n"
        status_text += f"🧠 *ИИ-персонализация*: {'✅ Включена' if prefs.ai_personalization else '❌ Отключена'}"

        await update.message.reply_text(status_text, parse_mode='Markdown')

    async def monitor_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Начать мониторинг сайта"""
        user_id = update.effective_user.id

        if not context.args:
            await update.message.reply_text(
                "❌ Укажите URL сайта для мониторинга.\n\n"
                "Пример: /monitor https://habr.com/ru/all/"
            )
            return

        url = context.args[0]

        # Проверяем URL
        if not url.startswith('http'):
            url = 'https://' + url

        # Проверяем лимиты
        user_sites = self.db.get_user_sites(user_id)
        if len(user_sites) >= 10:
            await update.message.reply_text(
                "❌ Вы достигли лимита в 10 сайтов.\n"
                "Остановите мониторинг ненужных сайтов командой /stop"
            )
            return

        # Создаем новый мониторинг
        site = MonitoredSite(
            url=url,
            user_id=user_id,
            created_at=datetime.now(),
            custom_name=context.args[1] if len(context.args) > 1 else None
        )

        self.db.add_monitored_site(site)

        await update.message.reply_text(
            f"✅ Начинаю мониторинг сайта {url}\n\n"
            f"Первая проверка выполнится в течение {CHECK_INTERVAL_MINUTES} минут.\n"
            f"Уведомления будут приходить при обнаружении изменений."
        )

    async def stop_monitor(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Остановить мониторинг сайта"""
        user_id = update.effective_user.id

        if not context.args:
            await update.message.reply_text(
                "❌ Укажите URL сайта для остановки мониторинга.\n\n"
                "Пример: /stop https://habr.com/ru/all/"
            )
            return

        url = context.args[0]

        self.db.remove_monitored_site(user_id, url)

        await update.message.reply_text(
            f"✅ Мониторинг сайта {url} остановлен.\n"
            f"Вы больше не будете получать уведомления об изменениях на этом сайте."
        )

    async def preferences(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Настройка предпочтений"""
        user_id = update.effective_user.id
        prefs = self.db.get_user_preferences(user_id)

        keyboard = [
            [
                InlineKeyboardButton("🔔 Типы уведомлений", callback_data="pref_types"),
                InlineKeyboardButton("⚡ Минимальный приоритет", callback_data="pref_priority")
            ],
            [
                InlineKeyboardButton("🧠 ИИ-персонализация", callback_data="pref_ai"),
                InlineKeyboardButton("📅 Частота", callback_data="pref_frequency")
            ],
            [InlineKeyboardButton("❌ Сбросить настройки", callback_data="pref_reset")]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            "⚙️ *Настройки уведомлений*\n\n"
            f"📨 Типы: {', '.join([t.value for t, e in prefs.notification_types.items() if e])}\n"
            f"⚡ Минимальный приоритет: {prefs.min_priority}\n"
            f"🧠 ИИ-персонализация: {'Вкл' if prefs.ai_personalization else 'Выкл'}\n"
            f"📅 Частота: {prefs.notification_frequency}\n\n"
            f"Выберите настройку для изменения:",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка callback-запросов"""
        query = update.callback_query
        await query.answer()

        user_id = update.effective_user.id
        data = query.data

        if data == "pref_types":
            await self.show_notification_types(query, user_id)
        elif data == "pref_priority":
            await self.show_priority_settings(query, user_id)
        elif data == "pref_ai":
            await self.toggle_ai(query, user_id)
        elif data == "back_to_prefs":
            await self.preferences_callback(query, user_id)
        elif data.startswith("set_type_"):
            await self.set_notification_type(query, user_id, data)
        elif data.startswith("set_priority_"):
            await self.set_priority(query, user_id, data)

    async def show_notification_types(self, query, user_id: int):
        """Показать типы уведомлений"""
        prefs = self.db.get_user_preferences(user_id)

        keyboard = []
        for notif_type in NotificationType:
            status = "✅" if prefs.notification_types[notif_type] else "❌"
            keyboard.append([
                InlineKeyboardButton(
                    f"{status} {notif_type.value}",
                    callback_data=f"set_type_{notif_type.value}"
                )
            ])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_prefs")])

        await query.edit_message_text(
            "📨 *Типы уведомлений*\n\n"
            "Выберите, какие типы уведомлений вы хотите получать:",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def show_priority_settings(self, query, user_id: int):
        """Показать настройки приоритета"""
        prefs = self.db.get_user_preferences(user_id)

        keyboard = [
            [InlineKeyboardButton(
                f"{'✅ ' if prefs.min_priority == 'low' else '   '}Низкий (все уведомления)",
                callback_data="set_priority_low"
            )],
            [InlineKeyboardButton(
                f"{'✅ ' if prefs.min_priority == 'medium' else '   '}Средний (средние и высокие)",
                callback_data="set_priority_medium"
            )],
            [InlineKeyboardButton(
                f"{'✅ ' if prefs.min_priority == 'high' else '   '}Высокий (только критические)",
                callback_data="set_priority_high"
            )],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_prefs")]
        ]

        await query.edit_message_text(
            "⚡ *Минимальный приоритет уведомлений*\n\n"
            "Уведомления ниже выбранного приоритета не будут отправляться:",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def toggle_ai(self, query, user_id: int):
        """Включить/выключить ИИ-персонализацию"""
        prefs = self.db.get_user_preferences(user_id)
        prefs.ai_personalization = not prefs.ai_personalization
        self.db.save_user_preferences(prefs)

        status = "включена" if prefs.ai_personalization else "отключена"
        await query.edit_message_text(
            f"🧠 ИИ-персонализация {status}!\n\n"
            f"{'Теперь уведомления будут персонализированы под вас.' if prefs.ai_personalization else 'Уведомления будут приходить в стандартном формате.'}\n\n"
            f"Нажмите /preferences для возврата в меню настроек"
        )

    async def set_notification_type(self, query, user_id: int, data: str):
        """Установить тип уведомления"""
        notif_type_str = data.replace("set_type_", "")
        notif_type = NotificationType(notif_type_str)

        prefs = self.db.get_user_preferences(user_id)
        prefs.notification_types[notif_type] = not prefs.notification_types[notif_type]
        self.db.save_user_preferences(prefs)

        await self.show_notification_types(query, user_id)

    async def set_priority(self, query, user_id: int, data: str):
        """Установить минимальный приоритет"""
        priority = data.replace("set_priority_", "")

        prefs = self.db.get_user_preferences(user_id)
        prefs.min_priority = priority
        self.db.save_user_preferences(prefs)

        await self.show_priority_settings(query, user_id)

    async def preferences_callback(self, query, user_id: int):
        """Показать настройки (callback версия)"""
        prefs = self.db.get_user_preferences(user_id)

        keyboard = [
            [
                InlineKeyboardButton("🔔 Типы уведомлений", callback_data="pref_types"),
                InlineKeyboardButton("⚡ Минимальный приоритет", callback_data="pref_priority")
            ],
            [
                InlineKeyboardButton("🧠 ИИ-персонализация", callback_data="pref_ai"),
                InlineKeyboardButton("📅 Частота", callback_data="pref_frequency")
            ]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            "⚙️ *Настройки уведомлений*\n\n"
            f"📨 Типы: {', '.join([t.value for t, e in prefs.notification_types.items() if e])}\n"
            f"⚡ Минимальный приоритет: {prefs.min_priority}\n"
            f"🧠 ИИ-персонализация: {'Вкл' if prefs.ai_personalization else 'Выкл'}\n\n"
            f"Выберите настройку для изменения:",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )

    async def send_notification_to_user(self, user_id: int, notification: Notification):
        """Отправка уведомления пользователю"""
        emoji_map = {
            NotificationType.INFO: "ℹ️",
            NotificationType.WARNING: "⚠️",
            NotificationType.ERROR: "❌",
            NotificationType.SUCCESS: "✅",
            NotificationType.CRITICAL: "🔴"
        }

        priority_emoji = {
            'low': '🟢',
            'medium': '🟡',
            'high': '🔴'
        }

        emoji = emoji_map.get(notification.type, "📢")
        priority_icon = priority_emoji.get(notification.priority, "⚪")

        message = f"{emoji} *{notification.title}*\n\n"
        message += f"{priority_icon} *Приоритет:* {notification.priority}\n"
        message += f"🔗 *Сайт:* {notification.site_url}\n\n"
        message += notification.message

        if notification.recommendation:
            message += f"\n\n💡 *Рекомендация:* {notification.recommendation}"

        try:
            await self.application.bot.send_message(
                chat_id=user_id,
                text=message,
                parse_mode='Markdown'
            )
            logger.info(f"Уведомление отправлено пользователю {user_id}")
            return True
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления: {e}")
            return False

    async def periodic_check(self):
        """Периодическая проверка всех сайтов"""
        logger.info("🔄 Запуск периодической проверки...")

        # Получаем все активные сайты
        all_sites = self.db.get_all_active_sites()

        for site in all_sites:
            logger.info(f"Проверка сайта {site.url} для пользователя {site.user_id}")
            changes = await self.monitor.check_site(site)

            if changes:
                prefs = self.db.get_user_preferences(site.user_id)

                # Проверяем, подписан ли пользователь на уведомления
                if any(prefs.notification_types.values()):
                    notification = await self.ai.personalize_notification(
                        site.user_id, site.url, changes, prefs
                    )

                    if notification:
                        await self.send_notification_to_user(site.user_id, notification)
                        self.db.add_notification(notification)
                        logger.info(f"Уведомление создано для пользователя {site.user_id}")

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Помощь"""
        help_text = """
📚 *Помощь по командам*

*/start* - Начало работы
*/subscribe* - Подписаться на уведомления
*/unsubscribe* - Отписаться от уведомлений
*/status* - Статус мониторинга
*/monitor [url]* - Начать мониторинг сайта
*/stop [url]* - Остановить мониторинг
*/preferences* - Настройки уведомлений
*/help* - Эта справка

*Примеры:*
`/monitor https://habr.com/ru/all/` - Мониторинг Habr
`/stop https://habr.com/ru/all/` - Остановить мониторинг

*Настройки:*
Вы можете настроить:
• Типы получаемых уведомлений
• Минимальный приоритет
• ИИ-персонализацию
        """
        await update.message.reply_text(help_text, parse_mode='Markdown')

    def run(self):
        """Запуск бота"""
        # Создаем приложение
        self.application = Application.builder().token(self.token).build()

        # Регистрируем команды
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CommandHandler("subscribe", self.subscribe))
        self.application.add_handler(CommandHandler("unsubscribe", self.unsubscribe))
        self.application.add_handler(CommandHandler("status", self.status))
        self.application.add_handler(CommandHandler("monitor", self.monitor_command))
        self.application.add_handler(CommandHandler("stop", self.stop_monitor))
        self.application.add_handler(CommandHandler("preferences", self.preferences))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CallbackQueryHandler(self.handle_callback))

        # Настраиваем планировщик
        self.scheduler.add_job(
            self.periodic_check,
            trigger=IntervalTrigger(minutes=CHECK_INTERVAL_MINUTES),
            id="periodic_check"
        )
        self.scheduler.start()

        # Запускаем бота
        logger.info("🚀 Бот запущен!")
        self.application.run_polling()


# ============= ТОЧКА ВХОДА =============
def main():
    """Запуск бота"""
    if not TELEGRAM_TOKEN or TELEGRAM_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("❌ Ошибка: Укажите TELEGRAM_TOKEN в файле!")
        print("Получите токен у @BotFather в Telegram")
        print("\nИнструкция:")
        print("1. Напишите @BotFather в Telegram")
        print("2. Отправьте команду /newbot")
        print("3. Введите имя бота")
        print("4. Скопируйте полученный токен")
        print("5. Вставьте токен в файл вместо YOUR_BOT_TOKEN_HERE")
        return

    bot = TelegramMonitorBot(TELEGRAM_TOKEN, OPENAI_API_KEY if OPENAI_API_KEY else None)
    bot.run()


if __name__ == "__main__":
    main()
import os
import sys
import json
import logging
import asyncio
from collections import deque
from threading import Lock
from typing import Optional, Tuple, List
from datetime import datetime, timedelta, timezone
from telegram import Update
from telegram import helpers
from telegram.ext import Application, ContextTypes, MessageHandler, filters, CommandHandler, ChatMemberHandler
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

CONFIG_FILE = "config.json"

DEFAULT_CONFIG = {
    "bot_token": "YOUR_BOT_TOKEN_HERE",
    "bot_owner_id": None,
    "inactivity_days_limit": 21,
    "admin_check_timeout": 2,
    "log_flush_interval_seconds": 5,
    "log_dm_config_key": "log_dm_chat_id",
    "database_url": "sqlite:///user_activity.db"
}


def load_config() -> dict:
    """Load configuration from config.json. Creates it with defaults if missing."""
    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "w") as f:
            json.dump(DEFAULT_CONFIG, f, indent=4)
        print(f"⚠️  '{CONFIG_FILE}' not found — created with default values.")
        print(f"   Please open '{CONFIG_FILE}' and fill in your bot_token (and adjust other fields if needed) before running the bot.")
        sys.exit(0)

    with open(CONFIG_FILE, "r") as f:
        config = json.load(f)

    # Validate required field
    if not config.get("bot_token") or config["bot_token"] == "YOUR_BOT_TOKEN_HERE":
        print(f"❌  Please set a valid 'bot_token' in '{CONFIG_FILE}' before running the bot.")
        sys.exit(1)

    return config


# Load configuration
CONFIG = load_config()
BOT_TOKEN: str = CONFIG["bot_token"]
BOT_OWNER_ID: Optional[int] = CONFIG.get("bot_owner_id")
INACTIVITY_DAYS_LIMIT: int = CONFIG.get("inactivity_days_limit", DEFAULT_CONFIG["inactivity_days_limit"])
ADMIN_CHECK_TIMEOUT: int = CONFIG.get("admin_check_timeout", DEFAULT_CONFIG["admin_check_timeout"])
LOG_FLUSH_INTERVAL_SECONDS: int = CONFIG.get("log_flush_interval_seconds", DEFAULT_CONFIG["log_flush_interval_seconds"])
LOG_DM_CONFIG_KEY: str = CONFIG.get("log_dm_config_key", DEFAULT_CONFIG["log_dm_config_key"])
DATABASE_URL: str = CONFIG.get("database_url", DEFAULT_CONFIG["database_url"])

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class TelegramDMLogHandler(logging.Handler):
    """Queue log records and forward them to a configured Telegram chat."""
    def __init__(self) -> None:
        super().__init__()
        self._chat_id = None
        self._queue = deque()
        self._lock = Lock()

    def set_chat_id(self, chat_id: Optional[int]) -> None:
        with self._lock:
            self._chat_id = chat_id
            if chat_id is None:
                self._queue.clear()

    def get_chat_id(self) -> Optional[int]:
        with self._lock:
            return self._chat_id

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
        except Exception:
            return

        with self._lock:
            if self._chat_id is None:
                return
            self._queue.append(message[:3800])

    def drain_messages(self, limit: int = 100) -> Tuple[Optional[int], List[str]]:
        with self._lock:
            chat_id = self._chat_id
            if chat_id is None or not self._queue:
                return chat_id, []

            count = min(limit, len(self._queue))
            messages = [self._queue.popleft() for _ in range(count)]
            return chat_id, messages


def utc_now() -> datetime:
    """Return current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


def ensure_utc(dt: datetime) -> datetime:
    """Normalize datetimes to timezone-aware UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

# Database setup
Base = declarative_base()
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

class UserActivity(Base):
    __tablename__ = 'user_activity'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False)
    chat_id = Column(Integer, nullable=False)
    username = Column(String)
    last_activity = Column(DateTime(timezone=True), default=utc_now)


class BotConfig(Base):
    __tablename__ = 'bot_config'

    key = Column(String, primary_key=True)
    value = Column(String, nullable=False)

# Create tables
Base.metadata.create_all(engine)


def get_config_value(key: str) -> Optional[str]:
    """Get config value from database."""
    session = Session()
    try:
        item = session.query(BotConfig).filter_by(key=key).first()
        return item.value if item else None
    except Exception as e:
        logger.error(f"Error reading config {key}: {e}")
        return None
    finally:
        session.close()


def set_config_value(key: str, value: Optional[str]) -> bool:
    """Set or delete config value in database."""
    session = Session()
    try:
        item = session.query(BotConfig).filter_by(key=key).first()
        if value is None:
            if item:
                session.delete(item)
        elif item:
            item.value = value
        else:
            session.add(BotConfig(key=key, value=value))
        session.commit()
        return True
    except Exception as e:
        logger.error(f"Error saving config {key}: {e}")
        session.rollback()
        return False
    finally:
        session.close()


def is_owner(update: Update) -> bool:
    """Check whether update sender is configured bot owner."""
    return bool(
        update.effective_user
        and BOT_OWNER_ID is not None
        and update.effective_user.id == BOT_OWNER_ID
    )


def get_telegram_log_handler(application: Application) -> Optional[TelegramDMLogHandler]:
    """Read Telegram log handler from bot_data."""
    handler = application.bot_data.get("telegram_log_handler")
    if isinstance(handler, TelegramDMLogHandler):
        return handler
    return None


async def flush_telegram_logs(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Flush queued log records to configured Telegram DM."""
    handler = get_telegram_log_handler(context.application)
    if handler is None:
        return

    chat_id, messages = handler.drain_messages(limit=100)
    if chat_id is None or not messages:
        return

    chunk = ""
    chunks = []
    for message in messages:
        candidate = f"{chunk}\n{message}" if chunk else message
        if len(candidate) > 3800:
            chunks.append(chunk)
            chunk = message
        else:
            chunk = candidate
    if chunk:
        chunks.append(chunk)

    for text in chunks:
        try:
            await context.bot.send_message(chat_id=chat_id, text=text)
        except Exception as e:
            print(f"Failed to forward logs to chat {chat_id}: {e}")
            break


async def setlogdm_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Set or disable Telegram DM destination for log forwarding."""
    if not update.message:
        return

    if BOT_OWNER_ID is None:
        await update.message.reply_text("BOT_OWNER_ID is not configured. Set it first, then retry.")
        return
    if not is_owner(update):
        await update.message.reply_text("Only the configured bot owner can run this command.")
        return

    target_chat_id = None
    if context.args:
        arg = context.args[0].strip()
        if arg.lower() in {"off", "disable", "none"}:
            if not set_config_value(LOG_DM_CONFIG_KEY, None):
                await update.message.reply_text("Failed to disable log forwarding.")
                return

            handler = get_telegram_log_handler(context.application)
            if handler:
                handler.set_chat_id(None)
            await update.message.reply_text("Log forwarding disabled.")
            return

        try:
            target_chat_id = int(arg)
        except ValueError:
            await update.message.reply_text("Usage: /setlogdm [chat_id|off]")
            return
    else:
        if not update.effective_chat or update.effective_chat.type != "private":
            await update.message.reply_text("Run this command in DM with the bot, or pass a chat ID.")
            return
        target_chat_id = update.effective_chat.id

    if not set_config_value(LOG_DM_CONFIG_KEY, str(target_chat_id)):
        await update.message.reply_text("Failed to save log DM config.")
        return

    handler = get_telegram_log_handler(context.application)
    if handler:
        handler.set_chat_id(target_chat_id)

    test_note = ""
    try:
        await context.bot.send_message(chat_id=target_chat_id, text="Log forwarding enabled for this chat.")
    except Exception as e:
        test_note = f"\nSaved config, but test DM failed: {e}"

    await update.message.reply_text(f"Log forwarding chat set to {target_chat_id}.{test_note}")


async def logdm_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show current Telegram DM destination for log forwarding."""
    if not update.message:
        return

    if BOT_OWNER_ID is None:
        await update.message.reply_text("BOT_OWNER_ID is not configured.")
        return
    if not is_owner(update):
        await update.message.reply_text("Only the configured bot owner can run this command.")
        return

    configured = get_config_value(LOG_DM_CONFIG_KEY)
    if configured is None:
        await update.message.reply_text("No log DM configured. Use /setlogdm in DM or /setlogdm <chat_id>.")
    else:
        await update.message.reply_text(f"Current log DM chat ID: {configured}")

def get_display_name(user) -> str:
    """Get the best available display name for a user."""
    if user.first_name and user.last_name:
        return f"{user.first_name} {user.last_name}"
    elif user.first_name and user.username:
        if user.first_name == user.username:
            return user.first_name  
        return f"{user.first_name} ({user.username})"
    elif user.username:
        return user.username
    
    return f"User{user.id}"

async def update_user_activity(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Update user's last activity timestamp."""
    if not update.message or not update.message.from_user:
        return
    # Ignore leave service messages so departed users are not re-tracked.
    if update.message.left_chat_member:
        return

    user = update.message.from_user
    chat_id = update.message.chat_id
    
    session = Session()
    try:
        user_activity = session.query(UserActivity).filter_by(
            user_id=user.id,
            chat_id=chat_id
        ).first()
        
        if user_activity:
            user_activity.last_activity = utc_now()
            user_activity.username = get_display_name(user)
        else:
            user_activity = UserActivity(
                user_id=user.id,
                chat_id=chat_id,
                username=get_display_name(user),
                last_activity=utc_now()
            )
            session.add(user_activity)
        
        session.commit()
    except Exception as e:
        logger.error(f"Error updating user activity: {e}")
        session.rollback()
    finally:
        session.close()

async def handle_chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove users from tracking when they leave or are removed from a chat."""
    if not update.chat_member:
        return

    old_status = update.chat_member.old_chat_member.status
    new_status = update.chat_member.new_chat_member.status
    if new_status not in ['left', 'kicked'] or old_status in ['left', 'kicked']:
        return

    user = update.chat_member.new_chat_member.user
    chat_id = update.chat_member.chat.id

    session = Session()
    try:
        deleted_count = session.query(UserActivity).filter_by(
            user_id=user.id,
            chat_id=chat_id
        ).delete(synchronize_session=False)
        session.commit()

        if deleted_count:
            logger.error(f"Removed departed user {get_display_name(user)} (ID: {user.id}) from chat {chat_id} tracking")
    except Exception as e:
        logger.error(f"Error removing departed user {user.id} from tracking: {e}")
        session.rollback()
    finally:
        session.close()

async def check_inactive_users(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Check and remove inactive users."""
    session = Session()
    try:
        # Get all user activities
        user_activities = session.query(UserActivity).all()
        current_time = utc_now()
        
        for activity in user_activities:
            # Skip if last activity is within 21 days
            if current_time - ensure_utc(activity.last_activity) < timedelta(days=INACTIVITY_DAYS_LIMIT):
                continue
                
            try:
                # Get chat member to check if they're an admin
                chat_member = await context.bot.get_chat_member(
                    chat_id=activity.chat_id,
                    user_id=activity.user_id
                )
                
                # Skip if user is an admin
                if chat_member.status in ['administrator', 'creator']:
                    continue
                
                # Kick inactive user
                await context.bot.ban_chat_member(
                    chat_id=activity.chat_id,
                    user_id=activity.user_id
                )
                
                # Immediately unban to allow them to rejoin
                await context.bot.unban_chat_member(
                    chat_id=activity.chat_id,
                    user_id=activity.user_id
                )
                
                # Remove from database
                session.delete(activity)
                
                logger.error(f"Removed inactive user {activity.username} (ID: {activity.user_id}) from chat {activity.chat_id}")
                
            except Exception as e:
                session.delete(activity)
                logger.error(f"Error processing user {activity.user_id}: {e}. Removed from database.")
                continue
        
        session.commit()
    except Exception as e:
        logger.error(f"Error in check_inactive_users: {e}")
        session.rollback()
    finally:
        session.close()

async def get_user_status(user_id: int, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Get user status with timeout."""
    try:
        async with asyncio.timeout(ADMIN_CHECK_TIMEOUT):
            chat_member = await context.bot.get_chat_member(chat_id=chat_id, user_id=user_id)
            return chat_member.status in ['administrator', 'creator']
    except (asyncio.TimeoutError, Exception):
        return False

async def gc_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /gc command - show days remaining before users get kicked."""
    if not update.message:
        return

    chat_id = update.message.chat_id
    current_time = utc_now()

    # Send initial loading message
    loading_message = await update.message.reply_text("⏳ Loading user visa data...")

    session = Session()
    try:
        # Get total number of users from Telegram API
        total_users = await context.bot.get_chat_member_count(chat_id=chat_id)
        
        # Get all user activities for this chat
        user_activities = session.query(UserActivity).filter_by(chat_id=chat_id).all()
        
        if not user_activities:
            await loading_message.edit_text("No user activity data available.")
            return

        # Sort users by days remaining (most critical first)
        user_list = []
        admin_ids = set()
        
        # First, collect all user IDs
        user_ids = [activity.user_id for activity in user_activities]
        
        # Check admin status for all users in parallel
        admin_tasks = [get_user_status(user_id, chat_id, context) for user_id in user_ids]
        admin_results = await asyncio.gather(*admin_tasks)
        
        # Create a mapping of user_id to admin status
        admin_status = dict(zip(user_ids, admin_results))
        
        # Count non-admin users
        non_admin_count = 0
        
        # Process users
        for activity in user_activities:
            days_inactive = (current_time - ensure_utc(activity.last_activity)).days
            days_remaining = max(0, INACTIVITY_DAYS_LIMIT - days_inactive)
            
            # Check if user is admin
            if admin_status.get(activity.user_id, False):
                continue
            
            non_admin_count += 1
            user_list.append({
                'username': activity.username,
                'days_remaining': days_remaining
            })

        # Sort by days remaining
        user_list.sort(key=lambda x: x['days_remaining'])

        # Calculate percentage of non-admin users
        non_admin_percentage = (non_admin_count / total_users) * 100 if total_users > 0 else 0

        # Create message
        message = "👥 *User Visa Status*\n"
        message += f"📊 *{non_admin_percentage:.0f}%* of users are on temp visa ({non_admin_count}/{total_users})\n\n"
        
        # Add non-admins
        for user in user_list:
            message += f"{helpers.escape_markdown(user['username'])} - {user['days_remaining']} days\n"
        await loading_message.edit_text(message, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error in gc_command: {e}")
        await loading_message.edit_text("❌ Error retrieving user activity data.")
    finally:
        session.close()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /start is issued."""
    await update.message.reply_text(
        "Hello! I'm Talo, a bot for Maikiwi's personal server. I'll keep track of user activity "
        f"and remove users who haven't sent messages in {INACTIVITY_DAYS_LIMIT} days.\n\n"
        "Commands:\n"
        "/start - Show this help message\n"
        "/gc - Show remaining days before users get kicked"
    )

def main() -> None:
    """Start the bot."""
    if not BOT_TOKEN:
        logger.error("No BOT_TOKEN found in environment variables!")
        return

    # Create the Application with job queue enabled
    application = Application.builder().token(BOT_TOKEN).build()

    # Configure Telegram log forwarding handler
    telegram_log_handler = TelegramDMLogHandler()
    telegram_log_handler.setLevel(logging.ERROR)
    telegram_log_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))

    configured_log_dm = get_config_value(LOG_DM_CONFIG_KEY)
    if configured_log_dm:
        try:
            telegram_log_handler.set_chat_id(int(configured_log_dm))
        except ValueError:
            logger.error(f"Invalid configured log DM chat ID: {configured_log_dm}")

    logging.getLogger().addHandler(telegram_log_handler)
    application.bot_data["telegram_log_handler"] = telegram_log_handler

    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("gc", gc_command))
    application.add_handler(CommandHandler("setlogdm", setlogdm_command))
    application.add_handler(CommandHandler("logdm", logdm_command))
    application.add_handler(ChatMemberHandler(handle_chat_member_update, ChatMemberHandler.CHAT_MEMBER))
    application.add_handler(MessageHandler(filters.ALL, update_user_activity))
    
    # Schedule jobs
    job_queue = application.job_queue
    job_queue.run_repeating(flush_telegram_logs, interval=LOG_FLUSH_INTERVAL_SECONDS, first=5)
    job_queue.run_repeating(check_inactive_users, interval=timedelta(hours=8), first=10)

    # Start the Bot
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()

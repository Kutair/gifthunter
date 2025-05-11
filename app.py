import os
import logging
from flask import Flask
import telebot
from telebot import types
from dotenv import load_dotenv
import threading
import time

load_dotenv()

BOT_TOKEN = os.environ.get("BOT_TOKEN")
MINI_APP_URL = os.environ.get("MINI_APP_URL", "https://vasiliy-katsyka.github.io/case")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Flask Приложение ---
app = Flask(__name__) # Flask app instance

# --- Telegram Бот ---
# Проверяем, есть ли токен, прежде чем создавать объект бота
if not BOT_TOKEN:
    logger.error("Токен бота (BOT_TOKEN) не найден в переменных окружения!")
    # Можно здесь выбросить исключение или завершить работу, 
    # так как без токена бот работать не будет.
    # Для примера, просто выйдем, если это основной скрипт.
    # В контексте Gunicorn это приведет к ошибке запуска воркера.
    if __name__ == '__main__':
        exit("BOT_TOKEN is not set. Exiting.")
    else: # Если импортируется Gunicorn'ом
        raise RuntimeError("BOT_TOKEN is not set. Cannot initialize bot.")

bot = telebot.TeleBot(BOT_TOKEN)

@app.route('/')
def index():
    # Этот эндпоинт нужен, чтобы Render считал сервис "здоровым"
    return "Flask App for Bot (Bot is polling) is running!"

# --- API эндпоинты (если нужны позже) ---
# @app.route('/api/init_payment', methods=['POST'])
# async def init_payment():
#     # ...
#     return {"status": "ok"}

@bot.message_handler(commands=['start'])
def send_welcome(message):
    logger.info(f"Получена команда /start от chat_id: {message.chat.id} ({message.from_user.username})")
    markup = types.InlineKeyboardMarkup()
    if not MINI_APP_URL:
        logger.error("MINI_APP_URL не установлен!")
        bot.send_message(message.chat.id, "Ошибка конфигурации: Mini App URL не найден.")
        return
    try:
        web_app_info = types.WebAppInfo(url=MINI_APP_URL)
        app_button = types.InlineKeyboardButton(text="🎮 Открыть Игру-Рулетку", web_app=web_app_info)
        markup.add(app_button)
        bot.send_message(
            message.chat.id,
            "Добро пожаловать в TON Gift Universe! 🎁\n\n"
            "Нажмите кнопку ниже, чтобы открыть рулетку и испытать свою удачу!",
            reply_markup=markup
        )
        logger.info(f"Сообщение /start отправлено пользователю {message.chat.id}")
    except Exception as e:
        logger.error(f"Ошибка при отправке /start ({message.chat.id}): {e}")
        try:
            bot.send_message(message.chat.id, "Извините, произошла ошибка при попытке открыть игру. Попробуйте позже.")
        except Exception as e2:
            logger.error(f"Не удалось отправить сообщение об ошибке пользователю {message.chat.id}: {e2}")


@bot.message_handler(func=lambda message: True)
def echo_all(message):
    logger.info(f"Получено сообщение от {message.chat.id}: {message.text}")
    bot.reply_to(message, "Нажмите /start, чтобы открыть игру.")

# Флаг, чтобы убедиться, что polling запускается только один раз
bot_polling_started = False
bot_polling_thread = None

def run_bot_polling():
    global bot_polling_started
    if bot_polling_started:
        logger.info("Polling уже запущен.")
        return

    bot_polling_started = True
    logger.info("Запуск бота в режиме polling...")
    
    max_retries_remove_webhook = 3
    for i in range(max_retries_remove_webhook):
        try:
            bot.remove_webhook()
            logger.info("Вебхук успешно удален (если был).")
            break
        except Exception as e:
            logger.warning(f"Попытка {i+1}/{max_retries_remove_webhook} удалить вебхук не удалась: {e}")
            if i < max_retries_remove_webhook - 1:
                time.sleep(2)
            else:
                logger.error("Не удалось удалить вебхук после нескольких попыток. Polling может работать некорректно.")
                # Продолжаем попытку запуска polling, но с предупреждением
    
    while True: # Бесконечный цикл для перезапуска polling в случае сбоя
        try:
            logger.info("Старт infinity_polling...")
            bot.infinity_polling(logger_level=logging.INFO, skip_pending=True, timeout=60, long_polling_timeout=30) # Добавлены таймауты
        except telebot.apihelper.ApiTelegramException as e:
            logger.error(f"Ошибка API Telegram в polling: {e}. Код: {e.error_code}")
            if e.error_code == 401: # Unauthorized
                logger.error("Неверный токен бота. Polling остановлен.")
                bot_polling_started = False # Сброс флага, чтобы можно было попытаться перезапустить снаружи, если токен исправят
                break # Выход из цикла while True, т.к. с неверным токеном нет смысла продолжать
            elif e.error_code == 409: # Conflict: another webhook is set
                logger.error("Конфликт: для бота установлен другой вебхук. Polling не может быть запущен.")
                bot_polling_started = False
                break
            else:
                logger.error(f"Другая ошибка API Telegram, перезапуск polling через 30 секунд...")
                time.sleep(30)
        except ConnectionError as e:
            logger.error(f"Ошибка соединения в polling: {e}. Перезапуск через 60 секунд...")
            time.sleep(60)
        except Exception as e:
            logger.error(f"Критическая ошибка в polling бота: {e}. Перезапуск через 60 секунд...")
            time.sleep(60)
        else: # Если infinity_polling завершился без исключений (маловероятно)
            logger.warning("infinity_polling завершился штатно. Перезапуск через 15 секунд...")
            time.sleep(15)
        
        if not bot_polling_started: # Если флаг сброшен (например, из-за 401), выходим
            break
            
# Запускаем поток с ботом при импорте этого модуля, если он еще не запущен
# Это нужно, чтобы Gunicorn его подхватил
if BOT_TOKEN and not bot_polling_started and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
    # WERKZEUG_RUN_MAIN - это переменная, которую Flask dev server устанавливает при перезагрузке,
    # чтобы избежать двойного запуска при use_reloader=True (хотя мы его выключили)
    bot_polling_thread = threading.Thread(target=run_bot_polling)
    bot_polling_thread.daemon = True
    bot_polling_thread.start()
    logger.info("Поток для polling бота запущен на уровне модуля.")


if __name__ == '__main__':
    # Этот блок теперь в основном для локального запуска Flask dev server.
    # Поток с ботом уже должен быть запущен кодом выше.
    logger.info("Запуск Flask development server (для локального теста)...")
    # Flask dev server не очень хорошо работает с потоками, которые он сам не породил при use_reloader=True.
    # Поэтому use_reloader=False очень важно.
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False, use_reloader=False)

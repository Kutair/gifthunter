import os
import logging
from flask import Flask # request убран, т.к. вебхук не используется
import telebot
from telebot import types
from dotenv import load_dotenv
import threading # Для запуска бота в отдельном потоке
import time

load_dotenv()

BOT_TOKEN = os.environ.get("BOT_TOKEN")
MINI_APP_URL = os.environ.get("MINI_APP_URL", "https://vasiliy-katsyka.github.io/case")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__) # Flask все еще нужен, если ты планируешь API для Mini App
bot = telebot.TeleBot(BOT_TOKEN)

@app.route('/') # Оставляем базовый роут для проверки, что Flask работает
def index():
    return "Flask App for Bot (Bot is polling) is running!"

# --- API эндпоинты для Mini App (если они нужны) ---
# @app.route('/api/some_endpoint', methods=['POST'])
# def handle_api_request():
#     # Твоя логика API
#     return {"status": "success"}
# --------------------------------------------------

@bot.message_handler(commands=['start'])
def send_welcome(message):
    logger.info(f"Получена команда /start от chat_id: {message.chat.id}")
    markup = types.InlineKeyboardMarkup()
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
    except Exception as e:
        logger.error(f"Ошибка при отправке /start: {e}")
        bot.send_message(message.chat.id, "Ошибка при попытке открыть игру.")

@bot.message_handler(func=lambda message: True)
def echo_all(message):
    bot.reply_to(message, "Нажмите /start, чтобы открыть игру.")

def run_bot_polling():
    logger.info("Запуск бота в режиме polling...")
    # Перед запуском polling убедимся, что вебхук снят
    max_retries = 3
    for i in range(max_retries):
        try:
            bot.remove_webhook()
            logger.info("Вебхук успешно удален (если был).")
            break
        except Exception as e:
            logger.warning(f"Попытка {i+1}/{max_retries} удалить вебхук не удалась: {e}")
            if i < max_retries - 1:
                time.sleep(2) # Ждем перед следующей попыткой
            else:
                logger.error("Не удалось удалить вебхук после нескольких попыток. Polling может работать некорректно.")
                return # Не запускаем polling, если вебхук не снят

    try:
        bot.infinity_polling(logger_level=logging.INFO, skip_pending=True)
    except Exception as e:
        logger.error(f"Критическая ошибка в polling бота: {e}")
        # Здесь можно добавить логику перезапуска, если нужно
        time.sleep(15) # Пауза перед возможным перезапуском
        run_bot_polling() # Осторожно: рекурсивный вызов, может привести к проблемам без доп. контроля

if __name__ == '__main__':
    # Запуск бота в отдельном потоке, чтобы Flask мог работать
    bot_thread = threading.Thread(target=run_bot_polling)
    bot_thread.daemon = True # Поток завершится, когда завершится основной
    bot_thread.start()

    # Запуск Flask development server
    # На Render gunicorn будет запускать app, и этот блок __main__ не выполнится напрямую так.
    # Но для локального теста `python app.py` это сработает.
    logger.info("Запуск Flask development server...")
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False, use_reloader=False)
    # debug=False и use_reloader=False важны, чтобы не перезапускать поток с ботом постоянно при изменениях кода.

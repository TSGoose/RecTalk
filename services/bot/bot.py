import logging
import os
import json
import requests
import websocket
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, CallbackContext
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
API_URL = os.getenv("API_URL")
WEBSOCKET_URL = os.getenv("WEBSOCKET_URL")

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

def call_external_api(user_message, chat_id):
    try:
        response = requests.post(API_URL, json={
            "text": user_message,
            "service": "oldsaratov",
            "source": "oldsaratov",
            "chat_id": str(chat_id)
        })

        if response.status_code == 200:
            data = response.json()
            answer = data['answer']

            try:
                ws = websocket.create_connection(WEBSOCKET_URL)
                ws.send(json.dumps({
                    "chat_id": chat_id,
                    "user_message": user_message,
                    "bot_answer": answer
                }))
                ws.close()
            except Exception as ws_err:
                logger.warning(f"Ошибка WebSocket: {ws_err}")

            return answer
        else:
            return f"Ошибка API: {response.json()}"
    except requests.RequestException as e:
        return f"Произошла ошибка при запросе: {e}"

async def start(update: Update, context: CallbackContext) -> None:
    await update.message.reply_text('Привет! Задай мне любой вопрос, я постараюсь помочь!')

async def handle_message(update: Update, context: CallbackContext) -> None:
    user_message = update.message.text
    chat_id = update.message.chat_id
    api_response = call_external_api(user_message, chat_id)
    await update.message.reply_text(api_response)

async def error(update: Update, context: CallbackContext) -> None:
    logger.warning(f"Ошибка {context.error}")

def main() -> None:
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.run_polling()

if __name__ == '__main__':
    main()


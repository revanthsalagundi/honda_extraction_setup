# telegram_notifier.py

import asyncio
import logging
from telegram import Bot

# Telegram configuration
TELEGRAM_BOT_TOKEN = '7413799888:AAGWdtJqpsMnnXMre9M362cwJzV3sU7rXYo'  # Replace with your bot token
TELEGRAM_CHAT_ID = '900718908'      # Replace with your chat ID

class TelegramNotifier:
    def __init__(self):
        self.bot = Bot(TELEGRAM_BOT_TOKEN)
        self.chat_id = TELEGRAM_CHAT_ID

    async def send_message(self, message):
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=message)
            logging.info(f"Telegram message sent: {message}")
        except Exception as e:
            logging.error(f"Failed to send Telegram message: {e}")

# Global notifier instance
notifier = None

def initialize():
    global notifier
    notifier = TelegramNotifier()
    return notifier

async def send_notification(message):
    if notifier:
        await notifier.send_message(message)
    else:
        logging.error("Telegram notifier not initialized")

# Helper function to run async function in sync context
def send_notification_sync(message):
    asyncio.run(send_notification(message))

# Test function
def test_notification():
    try:
        print("Testing Telegram notification...")
        send_notification_sync("Test message from blob processor")
        print("Test message sent successfully!")
    except Exception as e:
        print(f"Error sending test message: {e}")
initialize()
if __name__ == "__main__":
    # Test the notification system
    initialize()
    test_notification()
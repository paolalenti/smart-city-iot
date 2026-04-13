import requests
import os

from dotenv import load_dotenv

load_dotenv(dotenv_path='../.env')

TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID=os.getenv("CHAT_ID")

URL = f"https://api.telegram.org/bot{TOKEN}/sendMessage"


def send_notification(notification_type: str, message: str):
    """
    notification_type: INFO, WARN, ERROR
    message: сообщение
    """
    params = {
        "chat_id": CHAT_ID,
        "text": f"{notification_type}\n{message}",
    }

    requests.get(URL, params=params)

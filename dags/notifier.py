# notifier.py
import requests
import logging

# Set this to your actual webhook URL from your Discord channel settings
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1258254521847648307/Iv09McljWEsLr-EHk7XMQTJ7Fgl5J5NPpGudNbubaXME7-QIBn80Guo01e0iU3c8eUPa"

def send_discord_notification(message: str, username="Airflow Bot", avatar_url=None):
    """
    Send a Discord notification using a webhook.
    
    Args:
        message (str): Message content.
        username (str): Name displayed in the Discord message.
        avatar_url (str): Optional URL of an avatar image.
    """
    if not DISCORD_WEBHOOK_URL:
        logging.warning("🚫 Discord webhook URL not set.")
        return
    
    payload = {
        "content": message,
        "username": username
    }

    if avatar_url:
        payload["avatar_url"] = avatar_url

    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        response.raise_for_status()
        logging.info("✅ Discord notification sent successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Failed to send Discord notification: {e}")

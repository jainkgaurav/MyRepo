import requests
import configparser
from telegram.ext import Updater, CommandHandler, MessageHandler 
import requests  # For making API requests
import os
 
def read_config(filename='GeminiConfig.ini'):
    config = configparser.ConfigParser()
    config.read(filename)
    return config


config = read_config('C:\\Users\\jaina\\Gaurav\\Gemini\\Config\\APIKey.ini')
bot_token = config.get('TGSIGNALBOT', 'TOKEN')
chat_id = config.get('TGSIGNALBOT', 'CHAT_ID')


# Function to send a message to the user
def send_message(chat_id, text):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    params = {
        'chat_id': chat_id,
        'text': text
    }
    response = requests.post(url, json=params)
    if response.status_code != 200:
        print(f"Failed to send message. Status code: {response.status_code}")

# Function to handle incoming updates
def handle_updates(update):
    message = update['message']
    chat_id = message['chat']['id']
    text = message.get('text', '')
    if text == '/balance':
        # Replace this with your code to retrieve the trading account balance
        # Example: balance = get_trading_account_balance()
        balance = 10000  # Example balance
        send_message(chat_id, f"Your trading account balance is ${balance}")

# Function to poll for updates
def poll_updates():
    offset = None
    while True:
        url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
        params = {'offset': offset, 'timeout': 30}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            updates = response.json().get('result', [])
            for update in updates:
                handle_updates(update)
                offset = update['update_id'] + 1
        else:
            print(f"Failed to fetch updates. Status code: {response.status_code}")


def get_file_timestamp(filename):
    if os.path.exists(filename):
        return os.path.getmtime(filename)


# Example usage:
filename = '/path/to/your/file.txt'
timestamp = get_file_timestamp(filename)
if timestamp is not None:
    print(f"The timestamp of '{filename}' is: {timestamp}")

# Example usage:
directory = '/path/to/your/directory'
latest_timestamp = get_latest_file_timestamp(directory)
if latest_timestamp:
    print(f"The latest file timestamp in {directory} is: {latest_timestamp}")
else:
    print(f"No files found in {directory}")

def main():
    poll_updates()

if __name__ == "__main__":
    main()

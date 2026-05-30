# Telegram Activity Monitor Bot (Talo)

This bot monitors user activity in Telegram groups and automatically removes inactive non-administrator users after x (default: 21) days of inactivity.

## Features

- Tracks last message date for each user in the group
- Automatically removes users who haven't sent messages in x (default: 21) days but they are kicked instead of banned so they can always join back in.
- Excludes administrators from removal
- Uses SQLite database to persist user activity data
- Chunk error logging pushed to DM via "/setlogdm" (bot owner only) ("/setlogdm off" to disable.)

## Setup

1. Create a new bot using [@BotFather](https://t.me/botfather) on Telegram
2. Run main.py once to generate config.json. (must change bot_token)
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the bot:
   ```bash
   python main.py
   ```

## Usage

1. Add the bot to your group
2. Make the bot an administrator with the ability to remove users
3. The bot will automatically start tracking user activity
4. Users who don't send messages for x (default: 21) days will be automatically removed

## Note
Bot is designed to moderate one single chat/channel.

Existing users in the channel will be tracked after they send their first message when bot is online.

Make sure the bot has the following permissions in the group:
- Delete messages
- Ban users 

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import schedule
import time
import pandas as pd
import openai
import threading

# ------------------------
# Configuration
# ------------------------
SLACK_BOT_TOKEN = "slack-bot-token"  # Your Slack bot token
SLACK_APP_TOKEN = "socket-mode-token"  # Your Socket Mode token
OPENAI_API_KEY = "openai-api-key"     # Your OpenAI API key

openai.api_key = OPENAI_API_KEY

DEMO_CHANNEL = "demo-channel"

# ------------------------
# Initialize Slack app
# ------------------------
app = App(token=SLACK_BOT_TOKEN)

# ------------------------
# Load CSV data (simulate Azure & Databricks sources)
# ------------------------
azure_data = pd.read_csv("azure_data.csv")
databricks_data = pd.read_csv("databricks_data.csv")

# ------------------------
# AI Helper Function
# ------------------------
def generate_ai_response(prompt, max_tokens=400):
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a BI assistant for Austin FC."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=max_tokens,
        temperature=0.7
    )
    return response.choices[0].message.content.strip()

# ------------------------
# Morning Update
# ------------------------
def morning_update(channel_id):
    # Pass data
    azure_context = azure_data.to_dict(orient='records')
    databricks_context = databricks_data.to_dict(orient='records')

    prompt = (
        "You are Austin FC's BI assistant. Generate a morning update for yesterday's home match. "
        "Use the following data as context. Include only the relevant home match for yesterday. "
        "Include attendance, revenue, comparison to previous match, and a placeholder link for the full report. "
        "Format it as a Slack-friendly message.\n\n"
        f"Azure data: {azure_context}\n"
        f"Databricks data: {databricks_context}"
    )

    update_text = generate_ai_response(prompt)
    app.client.chat_postMessage(channel=channel_id, text=update_text)

# ------------------------
# Dynamic BI Request Handler
# ------------------------
processed_messages = set()  # Deduplicate messages

def handle_ai_request(channel_id, user_text):
    # Pass full context and let AI decide what to do
    azure_context = azure_data.to_dict(orient='records')
    databricks_context = databricks_data.to_dict(orient='records')

    prompt = (
        "You are Austin FC's BI assistant. Answer the user's question using the following data context.\n"
        f"Azure data: {azure_context}\n"
        f"Databricks data: {databricks_context}\n"
        f"User question: {user_text}\n"
        "Decide whether to respond with a short answer, a full report, or a data summary. "
        "Format the answer for Slack."
    )

    ai_response = generate_ai_response(prompt)
    app.client.chat_postMessage(channel=channel_id, text=ai_response)

# ------------------------
# Slack Event Handler
# ------------------------
@app.event("app_mention")
def handle_mention(event, say):
    if event.get("bot_id"):
        return  # Ignore bot messages

    msg_id = event.get("client_msg_id") or event.get("ts")
    if msg_id in processed_messages:
        return
    processed_messages.add(msg_id)

    user_text = event['text']
    channel_id = event['channel']

    print("EVENT RECEIVED:", event)
    handle_ai_request(channel_id, user_text)

# ------------------------
# Schedule 8am Morning Update
# ------------------------
def scheduled_morning_update():
    morning_update(DEMO_CHANNEL)

schedule.every().day.at("08:00").do(scheduled_morning_update)

# ------------------------
# Run the bot
# ------------------------
if __name__ == "__main__":
    # Optional: run the morning update immediately for demo
    scheduled_morning_update()

    # Start Slack Socket Mode in a separate thread
    def slack_thread():
        handler = SocketModeHandler(app, SLACK_APP_TOKEN)
        handler.start()

    t = threading.Thread(target=slack_thread)
    t.start()

    # Keep scheduler running; ensures scheduled tasks execute at the right time
    while True:
        schedule.run_pending()
        time.sleep(30)
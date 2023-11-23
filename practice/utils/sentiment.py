import requests
import json

API_URL = "http://192.168.100.20:9011/api/v1/sentiment"
def call_sentiment_api(comment_text):
    payload = {
        "text": comment_text
    }
    headers = {
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(API_URL, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        raise RuntimeError(f"Error connecting to the sentiment analysis API: {e}")

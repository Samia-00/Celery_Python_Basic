# task2.py
import os
from celery import Celery
from pymongo import MongoClient

celery_app = Celery(__name__)
celery_app.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery_app.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")

@celery_app.task(name="sentiment_analysis", bind=True, autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 5, 'countdown': 3})
def sentiment_analysis(post_id, comment_text):
    sentiment_result = "Positive"
    return sentiment_result

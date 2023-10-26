# task1.py
import os
from celery import Celery
from pymongo import MongoClient

celery_app = Celery(__name__)
celery_app.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery_app.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")

@celery_app.task(name="modify_comment_text", bind=True, autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 5, 'countdown': 3})
def modify_comment_text(post_id, comment_text):
    modified_text = comment_text.upper()
    return modified_text

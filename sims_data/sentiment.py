import time
import os
import logging
from celery import Celery
from celery import Celery
from pymongo import MongoClient

# Initialize a MongoDB client
mongo_client = MongoClient('mongodb://localhost:27017/')
result_backend = 'mongodb://localhost:27017/sims_data'

logger = logging.getLogger(__name__)
celery_app = Celery(__name__)
celery_app.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
# celery_app.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")


celery_app.conf.result_backend = result_backend

@celery_app.task(name="sentiment", bind=True, autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 5, 'countdown': 3})

def modify_comment_text(comment_text):
    modified_text = comment_text.upper()
    return modified_text

def run_speaker_info(self,tasks):
    modified_comments = []
    for comment in tasks:
        modified_text = modify_comment_text(comment['comment_text'])
        modified_comments.append({"comment_text": modified_text})
    return modified_comments

import time
import os
import logging
from celery import Celery

logger = logging.getLogger(__name__)
celery_app = Celery(__name__)
celery_app.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery_app.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")

@celery_app.task(name = "cyberbullying", bind= True, autoretry_for = (Exception,),
                 retry_kwargs={'max_retries': 5, 'countdown': 3})
def run_comment_cyberbullying(self, comment_text):
    print(len(comment_text))
    output = ['cyberbullying'] * len(comment_text)
    return output


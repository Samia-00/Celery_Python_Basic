import time
import os
import logging
from celery import Celery
from sentiment import call_sentiment_api

logger = logging.getLogger(__name__)
celery_app = Celery(__name__)
celery_app.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery_app.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")

@celery_app.task(name = "sentiment_analysis", bind= True, autoretry_for = (Exception,),
                 retry_kwargs={'max_retries': 5, 'countdown': 3})

def sentiment_analysis(self, comment_text):
    try:
        logger.info("Starting adding sentiment info.")
        st = time.time()
        print(len(comment_text))
        result = sentiment_analysis.apply_async(args=(comment_text,))
        result_value = result.get()
        sentiment_label = result_value.get('sentiment', [{}])[0].get('label', 'unknown')
        sentiment_result = {"Sentiment_Analysis": sentiment_label}
        took = time.time() - st
        logger.info(f"Took {took} seconds to adding sentiment info.")
        print(len(comment_text))
        return sentiment_result
    except Exception as e:
        logger.error(f"Error in sentiment_analysis task: {e}")
        return {'error': str(e), 'status': 'error'}




# def sentiment_analysis(self, comment_text):
#     logger.info("Starting adding sentiment info.")
#     st = time.time()
#     print(len(comment_text))
#     comment_text = ("asr_text", "diarizer_text")
#     result = sentiment_analysis.apply_async(args=(comment_text,))
#     # label_value = result['sentiment'][0]['label'] if result.get('sentiment') else None
#     output = call_sentiment_api(result)
#     took = time.time() - st
#     logger.info(f"Took {took} seconds to adding sentiment info.")
#     print(len(comment_text))
#     # output = ['sentiment_analysis'] * len(comment_text)
#     return output

# def sentiment_analysis(self, comment_text):
#     logger.info("Starting adding sentiment info.")
#     st = time.time()
#     result = call_sentiment_api(comment_text)
#     label_value = result['sentiment'][0]['label'] if result.get('sentiment') else None
#     took = time.time() - st
#     logger.info(f"Took {took} seconds to add sentiment info.")
#     return label_value

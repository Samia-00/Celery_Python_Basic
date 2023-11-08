import os
import time
import uuid

from bson import ObjectId
from celery import Celery, group, chain, signature
from pymongo import MongoClient

mongo_client = MongoClient("mongodb://root:Techno1419Techno@127.0.0.1:27017/admin?authSource=admin")

celery_app = Celery(__name__)
celery_app.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery_app.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")

def fetch_comments_by_post_id(post_id):
    try:
        db = mongo_client['sims_data']
        collection = db['scraped_data']
        post = list(collection.find({'_id': post_id},['comments']))
        if post:
            return post[0], [i['comment_text'] for i in post[0]['comments']]
        else:
            return {}, []
    except Exception as e:
        print(e)
        return {}, []

def asr_output_form(post_id):
    post, comment_texts = fetch_comments_by_post_id(post_id)
    if not comment_texts:
        print("No comments found for the post.")
        return None
    task_id = str(uuid.uuid4())
    task_group = group([
        signature('cyberbullying', args=(comment_texts,), queue="cyberbullying_queue", task_id=task_id + "_cyberbullying"),
        signature('sentiment_analysis', args=(comment_texts,), queue="sentiment_analysis_queue", task_id=task_id + "_sentiment_analysis")
    ])
    chn = chain(task_group)
    result = chn()
    return task_id, post

def get_task_results(task_id):

    asr_task = celery_app.AsyncResult(task_id + "_cyberbullying")
    diarizer_task = celery_app.AsyncResult(task_id + "_sentiment_analysis")

    if asr_task.state != "SUCCESS" or \
            diarizer_task.state != "SUCCESS":

        return ({
                        'cyberbullying_status': asr_task.state,
                        'sentiment_analysis_status': diarizer_task.state,

                        })

    if asr_task.state == "SUCCESS" and \
            diarizer_task.state == "SUCCESS" :

        return ({       'overall_status': 'SUCCESS',
                        'cyberbullying_status': asr_task.state,
                        'sentiment_analysis_status': diarizer_task.state,
                        'sentiment_analysis_result': diarizer_task.result,
                        'cyberbullying_result': asr_task.result,
                })
def get_result(task_id):
    while True:
        result = get_task_results(task_id)
        if 'overall_status' in result:
            return result
        else:
            print("Waiting for SUCCESS...")
            time.sleep(5)


def update_data(post_id, post, data):
    comments = []
    for comment, cyber, senti in zip(post['comments'], data['cyberbullying_result'], data['sentiment_analysis_result']):
        d = {**comment, **{"Cyberbullying": cyber, "Sentiment_Analysis":senti}}
        comments.append(d)

    db = mongo_client['sims_data']
    collection = db['scraped_data']
    result = collection.update_one({'_id' : post_id},
                                   {'$set' : { 'comments':comments}}
    )
    # print(result.__dict__())




post_id = ObjectId('64fd60a62e7d38238abd3e5d')
task_id,post = asr_output_form(post_id)
data = get_result(task_id)
update_data(post_id, post, data)



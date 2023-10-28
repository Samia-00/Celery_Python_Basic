# import os
# import time
# import uuid
# # from inspect import signature
# from celery import Celery, group, chain, signature
# from pymongo import MongoClient
#
# mongo_client = MongoClient("mongodb://root:Techno1419Techno@127.0.0.1:27017/admin?authSource=admin")
#
#
# celery_app = Celery(__name__)
# celery_app.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
# celery_app.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")
# id= "64fd60a62e7d38238abd3e5d"
# def asr_output_form(comment_text):
#     try:
#         db = mongo_client['sims_data']
#         collection = db['scraped_data']
#         id= "64fd60a62e7d38238abd3e5d"
#         posts_data = list(collection.find({'_id':id}))
#     except Exception as e:
#         print(e)
#
#     task_id = str(uuid.uuid4())
#     # for comment_text in comment_texts:
#     grp1 = group([signature('task1', args=(comment_text,), queue="task1_queue",
#                             task_id=task_id + "_task1"),
#                   signature('task2', args=(comment_text,), queue="task2_queue",
#                             task_id=task_id + "_task2")])
#
#     chn = (grp1)
#     result = chn()
#     return task_id
#
#
# def get_task_results(task_id):
#
#     asr_task = celery_app.AsyncResult(task_id + "_task1")
#     diarizer_task = celery_app.AsyncResult(task_id + "_task2")
#
#
#     if asr_task.state != "SUCCESS" or \
#             diarizer_task.state != "SUCCESS":
#
#         return ({
#                         'transcribe_status': asr_task.state,
#                         'diarize_status': diarizer_task.state,
#
#                         })
#
#     if asr_task.state == "SUCCESS" and \
#             diarizer_task.state == "SUCCESS" :
#
#         return ({       'overall_status': 'SUCCESS',
#                         'transcribe_status': asr_task.state,
#                         'diarize_status': diarizer_task.state,
#                         'task2_result': diarizer_task.result,
#                         'task1_result': asr_task.result,
#                 })
#
#
# input = asr_output_form('শুভ কামনা করি')
#
# while True:
#     result = get_task_results(input)
#     if 'overall_status' in result:
#         print(result)
#         break
#     else:
#         print("Waiting for SUCCESS...")
#         time.sleep(5)
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

        # Find the post by ID and extract its comments
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
        signature('task1', args=(comment_texts,), queue="task1_queue", task_id=task_id + "_task1"),
        signature('task2', args=(comment_texts,), queue="task2_queue", task_id=task_id + "_task2")
    ])
    chn = chain(task_group)
    result = chn()
    return task_id, post


def get_task_results(task_id):

    asr_task = celery_app.AsyncResult(task_id + "_task1")
    diarizer_task = celery_app.AsyncResult(task_id + "_task2")


    if asr_task.state != "SUCCESS" or \
            diarizer_task.state != "SUCCESS":

        return ({
                        'transcribe_status': asr_task.state,
                        'diarize_status': diarizer_task.state,

                        })

    if asr_task.state == "SUCCESS" and \
            diarizer_task.state == "SUCCESS" :

        return ({       'overall_status': 'SUCCESS',
                        'transcribe_status': asr_task.state,
                        'diarize_status': diarizer_task.state,
                        'task2_result': diarizer_task.result,
                        'task1_result': asr_task.result,
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
    for comment, cyber, senti in zip(post['comments'], data['task1_result'], data['task2_result']):
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




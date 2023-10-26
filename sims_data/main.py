# run_tasks.py
import os
import time
import uuid
from celery import Celery, signature

celery_app = Celery(__name__)
celery_app.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery_app.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")

def asr_output_form(tasks):
    task_id = str(uuid.uuid4())

    # Create signatures for the two analysis tasks
    sentiment_task = signature('sentiment_analysis', args=(tasks,), queue="sentiment_queue",
                               task_id=task_id + "_sentiment")
    cyberbullying_task = signature('cyberbullying_analysis', args=(tasks,), queue="cyberbullying_queue",
                                  task_id=task_id + "_cyberbullying")

    # Execute the tasks
    sentiment_result = sentiment_task.apply_async()
    cyberbullying_result = cyberbullying_task.apply_async()

    return task_id

def get_task_results(task_id):
    sentiment_task = celery_app.AsyncResult(task_id + "_sentiment")
    cyberbullying_task = celery_app.AsyncResult(task_id + "_cyberbullying")

    if sentiment_task.state != "SUCCESS" or cyberbullying_task.state != "SUCCESS":
        return {
            'sentiment_status': sentiment_task.state,
            'cyberbullying_status': cyberbullying_task.state,
        }

    if sentiment_task.state == "SUCCESS" and cyberbullying_task.state == "SUCCESS":
        return {
            'overall_status': 'SUCCESS',
            'sentiment_status': sentiment_task.state,
            'cyberbullying_status': cyberbullying_task.state,
            'sentiment_result': sentiment_task.result,
            'cyberbullying_result': cyberbullying_task.result,
        }

id = asr_output_form([10, 20])

while True:
    result = get_task_results(id)
    if 'overall_status' in result:
        print(result)
        break
    else:
        print("Waiting for SUCCESS...")
        time.sleep(5)

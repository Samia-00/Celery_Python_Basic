import os
import time
import uuid
# from inspect import signature
from celery import Celery, group, chain, signature

celery_app = Celery(__name__)
celery_app.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery_app.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")

def asr_output_form(tasks):

    task_id = str(uuid.uuid4())
    grp1 = group([signature('task1', args=(tasks,), queue="task1_queue",
                            task_id=task_id + "_task1"),
                  signature('task2', args=(tasks,), queue="task2_queue",
                            task_id=task_id + "_task2")])

    chn = (grp1)
    result = chn()
    return task_id


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

id = asr_output_form([10,20])

while True:
    result = get_task_results(id)
    if 'overall_status' in result:
        print(result)
        break
    else:
        print("Waiting for SUCCESS...")
        time.sleep(5)

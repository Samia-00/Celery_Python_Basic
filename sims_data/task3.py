import os
import time
import uuid
from celery import Celery, group, signature
from pymongo import MongoClient

# Initialize a MongoDB client with authentication credentials
mongo_client = MongoClient('mongodb://username:password@localhost:27017/your_database_name')

# Create a Celery application
celery_app = Celery(__name__)
celery_app.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery_app.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")

def asr_output_form(posts_data):
    task_id = str(uuid.uuid4())
    task_signatures = []

    for post_data in posts_data:
        # Check if 'comment_text' key exists in the post_data
        if 'comment_text' in post_data:
            comment_text = post_data['comment_text']
            post_id = post_data['_id']  # Assuming you want to pass the post ID
            # Create a task signature for processing the comment_text
            task_signature = signature('modify_comment_text', args=(post_id, comment_text))
            task_signatures.append(task_signature)
        else:
            print("Warning: 'comment_text' key not found in post_data")

    if task_signatures:
        # Process the tasks if there are valid task signatures
        grp = group(task_signatures)
        result = grp.apply_async()
        return task_id, task_signatures
    else:
        print("No valid tasks to process.")
        return None, None

# Rest of your code remains the same
def get_task_results(task_id, task_signatures):
    results = {}

    for post_id, modify_comment_result, sentiment_result in task_signatures:
        modify_comment_task = celery_app.AsyncResult(modify_comment_result.task_id)
        sentiment_task = celery_app.AsyncResult(sentiment_result.task_id)

        results[post_id] = {
            'modify_comment_status': modify_comment_task.state,
            'sentiment_status': sentiment_task.state,
        }

        if modify_comment_task.state == "SUCCESS":
            results[post_id]['modify_comment_result'] = modify_comment_task.result

        if sentiment_task.state == "SUCCESS":
            results[post_id]['sentiment_result'] = sentiment_task.result

    return task_id, results
if __name__ == '__main__':
    # Retrieve the MongoDB data you want to process
    db = mongo_client['sims_data']
    collection = db['scraped_data']
    # Define the variable posts_data and retrieve data from MongoDB
    try:
        posts_data = list(collection.find({}))
    except Exception as e:
        print(f"An error occurred while querying MongoDB: {e}")

        if posts_data:
            # Process the data if it's available
            task_id, task_signatures = asr_output_form(posts_data)

            if task_id:
                while True:
                    result = get_task_results(task_id)
                    if 'overall_status' in result:
                        print(result)
                        break
                    else:
                        print("Waiting for SUCCESS...")
                        time.sleep(5)
        else:
            print("No data available for processing.")

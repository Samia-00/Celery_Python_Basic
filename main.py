from celery import Celery
from time import sleep

app = Celery('main', broker='redis://localhost:6379/0', backend='db+sqlite:///db.sqlite3')


@app.task
def reverse(text):
    sleep(5)
    return text[::-1]

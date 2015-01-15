import os
from flask import Flask, request
from celery import Celery
from ldsearch import tasks


app = Flask('ldsearch')
app.config.update(
    DEBUG = os.getenv('DEBUG', False)
)
celery = Celery('ldsearch', broker=os.getenv('BROKER_URL', None))
celery.conf.update(app.config)


notify = celery.task(name='notify', queue='notify')(tasks.notify)
enrich = celery.task(name='enrich', queue='enrich')(tasks.enrich)
infer = celery.task(name='infer', queue='infer')(tasks.infer)
ingest = celery.task(name='ingest', queue='ingest')(tasks.ingest)


@app.route('/', methods=['POST'])
def index():
    uris = [line.strip() # trim any whitespace
            for line in request.get_data().decode('utf-8').splitlines()
            if not line.startswith('#')]

    for uri in uris:
        (notify.s(uri) | enrich.s() | infer.s() | ingest.s()).apply_async()

    return 'Accepted!\n', 202, {'Content-Type': 'text/plain'}


if __name__ == '__main__':
    app.run(debug=True, port=int(os.getenv('PORT', 5000)))

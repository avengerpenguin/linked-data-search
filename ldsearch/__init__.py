import os
from flask import Flask, request
from ldsearch.tasks import notify, enrich, infer, ingest


app = Flask('ldsearch')
app.config.update(
    DEBUG = os.getenv('DEBUG', False)
)

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

web: gunicorn ldsearch:app --workers 4 --log-file -
flower: celery -A ldsearch.tasks.celery flower --port=$PORT
worker_notify: celery --autoscale=64,1 --without-gossip --loglevel=INFO -P eventlet -A ldsearch.tasks.celery worker -Q notify -n worker_notify.$PORT
worker_enrich: celery --autoscale=64,1 --without-gossip --loglevel=INFO -P eventlet -A ldsearch.tasks.celery worker -Q enrich -n worker_enrich.$PORT
worker_infer: celery --autoscale=4,1 --without-gossip --loglevel=INFO -A ldsearch.tasks.celery worker -Q infer -n worker_infer.$PORT
worker_ingest: celery --autoscale=4,1 --without-gossip --loglevel=INFO -P eventlet -A ldsearch.tasks.celery worker -Q ingest -n worker_ingest.$PORT
worker_beat: celery -A ldsearch.tasks.celery beat

# app/tasks/celery_worker.py  (or whatever module you use to start Celery)
from celery import Celery
from app.core.config import settings

celery_app = Celery(
    'dataload_service',
    broker=str(settings.CELERY_BROKER_URL),
    backend=str(settings.CELERY_RESULT_BACKEND_URL),
    include=['app.tasks.load_jobs']   # make sure your tasks module is imported
)

# now load any additional Celery config (if you still need it)
celery_app.config_from_object('celeryconfig', namespace='CELERY')
celery_app.autodiscover_tasks(['app.tasks'])

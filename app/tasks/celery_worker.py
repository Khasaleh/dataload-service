from celery import Celery
from app.core.config import settings  # centralized settings
import logging

logger = logging.getLogger(__name__)

# Initialize Celery application with broker and backend URLs from settings
celery_app = Celery(
    'dataload_service',
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND_URL,
    include=['app.tasks.load_jobs']  # ensure load_jobs module is imported
)

# Load additional configuration from celeryconfig.py
# Using namespace 'CELERY' to pick up env vars like CELERY_BROKER_URL, etc.
celery_app.config_from_object('celeryconfig', namespace='CELERY')

# Auto-discover tasks in the app.tasks package
celery_app.autodiscover_tasks(['app.tasks'])

# Optional: configure logging
# from celery.signals import setup_logging
# @setup_logging.connect
# def config_loggers(*args, **kwargs):
#     ...
# celery_app.log.setup()

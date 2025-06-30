from app.core.config import settings  # Import centralized settings
import logging

logger = logging.getLogger(__name__)

# Celery Configuration from Centralized Settings
# Celery connection URLs are now sourced from the `settings` object,
# which constructs them from base Redis settings or uses fully provided URLs.

# Build Redis URL with password
broker_url = str(settings.CELERY_BROKER_URL) if settings.CELERY_BROKER_URL else None
result_backend = str(settings.CELERY_RESULT_BACKEND_URL) if settings.CELERY_RESULT_BACKEND_URL else None

if not broker_url:
    logger.error(
        "Celery broker URL is not configured in settings. "
        "Ensure Redis host, port, and Celery DB number are set, or a full CELERY_BROKER_URL is provided. "
        "Celery worker will likely fail to start."
    )

if not result_backend:
    logger.warning(
        "Celery result backend URL is not configured in settings. Task results may not be stored."
    )

# Celery Configuration
task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']

# Optional: Configure Celery logging to use the application's log level
# Uncomment to set up custom logging
# from celery.signals import setup_logging
# @setup_logging.connect
# def config_loggers(*args, **kwargs):
#     from logging.config import dictConfig
#     from app.core.config import settings as app_settings  # Re-import to avoid cycle if this file is top-level imported
#     LOGGING = {
#         'version': 1,
#         'disable_existing_loggers': False,
#         'formatters': {
#             'standard': {
#                 'format': '[%(asctime)s] [%(levelname)s] [%(name)s::%(module)s::%(funcName)s::%(lineno)d] %(message)s',
#                 'datefmt': '%Y-%m-%d %H:%M:%S'
#             },
#         },
#         'handlers': {
#             'console': {
#                 'level': app_settings.LOG_LEVEL.upper(),
#                 'class': 'logging.StreamHandler',
#                 'formatter': 'standard',
#             },
#         },
#         'loggers': {
#             'celery': {
#                 'handlers': ['console'],
#                 'level': app_settings.LOG_LEVEL.upper(),
#                 'propagate': False,
#             },
#             'app': {  # Configure your app's logger if needed
#                 'handlers': ['console'],
#                 'level': app_settings.LOG_LEVEL.upper(),
#                 'propagate': False,
#             },
#             '': {  # Root logger
#                 'handlers': ['console'],
#                 'level': app_settings.LOG_LEVEL.upper(),
#                 'propagate': False,
#             }
#         }
#     }
#     dictConfig(LOGGING)
# logger.info(f"Celery logging configured with level: {app_settings.LOG_LEVEL.upper()}")

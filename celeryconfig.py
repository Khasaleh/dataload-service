import os

# --- Redis Configuration for Celery ---
# Fetches Redis connection details from environment variables.
# These should align with how the main application connects to Redis (e.g., in app/tasks/load_jobs.py).

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")

# Specific DB numbers for Celery broker and result backend
# It's good practice to use different DB numbers if they share the same Redis instance
# to avoid key collisions, though 0 is a common default for both if not specified.
CELERY_BROKER_DB_NUMBER = os.getenv("CELERY_BROKER_DB_NUMBER", "0")
CELERY_RESULT_BACKEND_DB_NUMBER = os.getenv("CELERY_RESULT_BACKEND_DB_NUMBER", "0") # Often same as broker

broker_url = f'redis://{REDIS_HOST}:{REDIS_PORT}/{CELERY_BROKER_DB_NUMBER}'
result_backend = f'redis://{REDIS_HOST}:{REDIS_PORT}/{CELERY_RESULT_BACKEND_DB_NUMBER}'

task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']

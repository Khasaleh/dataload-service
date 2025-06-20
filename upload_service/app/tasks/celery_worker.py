from celery import Celery

# The celeryconfig.py should be in the same directory where you run celery worker,
# or Celery needs to be configured to find it.
# Assuming celery worker is run from the root of 'upload_service' directory.
# If celeryconfig is in the root, this will work.
# Otherwise, use app.config_from_object('project.celeryconfig')

celery_app = Celery(
    'upload_service_worker', # Application name
    # broker='redis://redis:6379/0', # Can be set here or in celeryconfig.py
    # backend='redis://redis:6379/0', # Can be set here or in celeryconfig.py
    # include=['app.tasks.load_jobs'] # Tell Celery where to find tasks
)

# Load configuration from celeryconfig.py
# Assumes celeryconfig.py is in the PYTHONPATH. If it's in the root of upload_service,
# and the worker is started from there, it should be found.
celery_app.config_from_object('celeryconfig')

# Optional: Autodiscover tasks from installed apps (if you have a more Django-like structure)
# For this structure, explicitly listing task modules in 'include' is common.
celery_app.autodiscover_tasks(['app.tasks']) # Looks for tasks.py in specified modules, or use include=['app.tasks.load_jobs']

# To ensure tasks are registered, it's often good to import them or the modules containing them.
# from . import load_jobs # This might cause circular import if load_jobs imports celery_app
                          # Using autodiscover_tasks or include in Celery constructor is safer.

if __name__ == '__main__':
    celery_app.start()

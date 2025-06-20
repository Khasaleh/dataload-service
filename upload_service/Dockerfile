FROM python:3.9-slim

WORKDIR /app

COPY ./upload_service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY ./upload_service /app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

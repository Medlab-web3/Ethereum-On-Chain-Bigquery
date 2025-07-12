FROM python:3.11-slim

WORKDIR /app

COPY main.py .
COPY requirements.txt .

RUN pip install --upgrade pip && pip install -r requirements.txt

ENTRYPOINT ["python", "main.py"]

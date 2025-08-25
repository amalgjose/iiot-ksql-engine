FROM python:3.11-slim

WORKDIR /app

COPY producer.py /app/

RUN pip install --no-cache-dir confluent-kafka

CMD ["python", "tag_data_generator.py"]

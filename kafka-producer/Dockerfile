FROM python:3.9

WORKDIR /app

COPY kafka-producer/requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY kafka-producer/ .
COPY data/combined_taxis_data.csv data/combined_taxis_data.csv

CMD ["bash", "-u", "wait-for-kafka.sh"]
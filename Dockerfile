FROM ubuntu:18.04

RUN apt-get update && \
    apt-get install -y default-jdk scala wget software-properties-common python3.8 python3-pip curl libpq-dev build-essential libssl-dev libffi-dev python3-dev && \
    apt-get clean

RUN wget https://archive.apache.org/dist/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz && \
    tar xvf spark-3.0.1-bin-hadoop3.2.tgz && \
    mv spark-3.0.1-bin-hadoop3.2/ /usr/local/spark && \
    ln -s /usr/local/spark spark

RUN pip3 install pytest pyspark

WORKDIR app
COPY . /app

ENV PYSPARK_PYTHON=python3

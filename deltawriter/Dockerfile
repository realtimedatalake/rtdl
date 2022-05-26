FROM python:3.8-alpine
WORKDIR /usr/src/app
COPY requirements.txt requirements.txt
COPY module.yaml module.yaml
COPY delta_writer.py delta_writer.py
ENV SPARK_MASTER_HOST=0.0.0.0
ENV SPARK_MASTER_PORT=7077
RUN apk update && \
    apk add bash && \
    apk add openjdk11-jre
RUN pip3 install -r requirements.txt
EXPOSE 8083
USER 1001
CMD ["python3", "-u", "delta_writer.py"]

FROM python:3.8-alpine
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
RUN apk update && apk add bash && apk add openjdk11-jre
COPY . .
CMD ["python3", "-u", "delta_writer.py"]
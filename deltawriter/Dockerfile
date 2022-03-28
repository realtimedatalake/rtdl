FROM python:3.8-alpine
WORKDIR /usr/src/app
COPY requirements.txt requirements.txt
RUN apk update && apk add bash && apk add openjdk11-jre
RUN pip3 install -r requirements.txt
COPY . .
CMD ["python3", "-u", "delta_writer.py"]

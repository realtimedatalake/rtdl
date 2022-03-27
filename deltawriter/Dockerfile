FROM jupyter/pyspark-notebook
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY . .
CMD ["python3", "-u", "delta_writer.py"]

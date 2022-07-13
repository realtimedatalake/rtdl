 FROM debian:11

   # System packages
   RUN apt-get clean && apt-get update -y && \
      apt-get install -y python3 python3-pip curl wget unzip procps openjdk-11-jdk cmake make gcc libc6-dev && \
      ln -s /usr/bin/python3 /usr/bin/python && \
      rm -rf /var/lib/apt/lists/*

      # Install Spark
   RUN curl https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz -o spark.tgz && \
      tar -xf spark.tgz && \
      mv spark-3.2.1-bin-hadoop3.2 /usr/bin/ && \
      mkdir /usr/bin/spark-3.2.1-bin-hadoop3.2/logs && \
      rm spark.tgz

   ENV TINI_VERSION v0.19.0
   ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/local/sbin/tini
   RUN chmod +x /usr/local/sbin/tini
   #ENTRYPOINT ["/usr/local/sbin/tini", "--"]

   # Prepare dirs
   RUN mkdir -p /tmp/logs/ && chmod a+w /tmp/logs/ && mkdir /app && chmod a+rwx /app && mkdir /data && chmod a+rwx /data
   ENV JAVA_HOME=/usr
   ENV SPARK_HOME=/usr/bin/spark-3.2.1-bin-hadoop3.2
   ENV PATH=$SPARK_HOME:$PATH:/bin:$JAVA_HOME/bin:$JAVA_HOME/jre/bin
   ENV SPARK_MASTER_HOST spark-master
   ENV SPARK_MASTER_PORT 7077
   ENV PYSPARK_PYTHON=/usr/bin/python
   ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
   ENV APP=/app
   ENV SHARED_WORKSPACE=/opt/workspace
   RUN mkdir -p ${SHARED_WORKSPACE}
   VOLUME ${SHARED_WORKSPACE}
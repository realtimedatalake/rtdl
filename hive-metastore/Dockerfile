FROM openjdk:8-jre-slim

WORKDIR /opt

ENV HADOOP_VERSION=3.3.1
ENV METASTORE_VERSION=3.1.2

ENV PSQL_CONN_VERSION=42.3.1
ENV LOG4J_VERSION=2.16.0

ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV HIVE_HOME=/opt/apache-hive-metastore-${METASTORE_VERSION}-bin

RUN apt update && \
    apt install -y curl && \
    curl -L https://repo1.maven.org/maven2/org/apache/hive/hive-standalone-metastore/${METASTORE_VERSION}/hive-standalone-metastore-${METASTORE_VERSION}-bin.tar.gz | tar zxf - && \
    curl -L https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | tar zxf - && \
    curl -L https://jdbc.postgresql.org/download/postgresql-${PSQL_CONN_VERSION}.jar -o postgresql-${PSQL_CONN_VERSION}.jar && \
    cp postgresql-${PSQL_CONN_VERSION}.jar ${HIVE_HOME}/lib/ && \
    rm  postgresql-${PSQL_CONN_VERSION}.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-web/${LOG4J_VERSION}/log4j-web-${LOG4J_VERSION}.jar -o log4j-web-${LOG4J_VERSION}.jar && \
    cp log4j-web-${LOG4J_VERSION}.jar ${HIVE_HOME}/lib/ && \
    rm log4j-web-${LOG4J_VERSION}.jar

COPY /conf/metastore-site.xml ${HIVE_HOME}/conf
COPY /scripts/entrypoint.sh /entrypoint.sh

RUN groupadd -r hive --gid=1000 && \
    useradd -r -g hive --uid=1000 -d ${HIVE_HOME} hive && \
    chown hive:hive -R ${HIVE_HOME} && \
    chown hive:hive /entrypoint.sh && chmod +x /entrypoint.sh

USER hive
EXPOSE 9083

ENTRYPOINT ["sh", "-c", "/entrypoint.sh"]

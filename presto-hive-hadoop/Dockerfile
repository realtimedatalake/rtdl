FROM ubuntu:18.04

RUN apt update
RUN apt install -y openjdk-8-jre python less curl openssh-server openssh-client
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

# Setup Hadoop
RUN curl -s https://archive.apache.org/dist/hadoop/core/hadoop-2.9.2/hadoop-2.9.2.tar.gz | tar -xz -C /root
RUN mkdir /root/hadoop-2.9.2/dfs
COPY hadoop/hadoop-env.sh /root/hadoop-2.9.2/etc/hadoop/hadoop-env.sh
COPY hadoop/core-site.xml /root/hadoop-2.9.2/etc/hadoop/core-site.xml
COPY hadoop/hdfs-site.xml /root/hadoop-2.9.2/etc/hadoop/hdfs-site.xml

## ssh without password
RUN ssh-keygen -t rsa -P '' -f /root/.ssh/id_rsa
RUN cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys
RUN chmod 0600 /root/.ssh/authorized_keys
COPY ssh_config /root/.ssh/config
RUN chmod 400 /root/.ssh/config

ENV HADOOP_HOME /root/hadoop-2.9.2

## Format namenode
RUN /root/hadoop-2.9.2/bin/hdfs namenode -format


# Setup Hive
RUN curl -s https://archive.apache.org/dist/hive/hive-2.3.6/apache-hive-2.3.6-src.tar.gz | tar -xz -C /root
COPY hive/hive-site.xml /root/apache-hive-2.3.6-bin/conf/hive-site.xml
#RUN curl -s https://jdbc.postgresql.org/download/postgresql-42.2.6.jar -o /root/apache-hive-2.3.6-bin/lib/postgresql-42.2.6.jar
COPY postgresql/postgresql-42.2.6.jar /root/apache-hive-2.3.6-bin/lib/postgresql-42.2.6.jar

## Setup Postgres
RUN DEBIAN_FRONTEND=noninteractive apt install -y postgresql postgresql-contrib
RUN su postgres -c '/usr/lib/postgresql/10/bin/initdb -D /var/lib/postgresql/10/main2 --auth-local trust --auth-host md5'


# Setup Presto
ENV PRESTO_HOME /root/presto-server-318
RUN curl -s https://repo1.maven.org/maven2/io/prestosql/presto-server/318/presto-server-318.tar.gz | tar -xz -C /root
RUN curl -s https://repo1.maven.org/maven2/io/prestosql/presto-cli/318/presto-cli-318-executable.jar -o $PRESTO_HOME/bin/presto-cli 
RUN chmod +x $PRESTO_HOME/bin/presto-cli
RUN ln -s $PRESTO_HOME/bin/presto-cli /usr/local/bin/presto-cli
RUN chmod +x /usr/local/bin/presto-cli
COPY presto/catalog $PRESTO_HOME/etc/catalog
COPY presto/jvm.config.template $PRESTO_HOME/etc/jvm.config.template
COPY presto/config.properties.template $PRESTO_HOME/etc/config.properties.template
COPY presto/log.properties $PRESTO_HOME/etc/log.properties
COPY presto/node.properties $PRESTO_HOME/etc/node.properties


# Copy setup script
COPY start_services.sh /root/start_services.sh
RUN chown root:root /root/start_services.sh
RUN chmod 700 /root/start_services.sh

# Start services
CMD ["/root/start_services.sh"]

EXPOSE 8080
EXPOSE 9083

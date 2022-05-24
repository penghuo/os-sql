FROM ubuntu:18.04

LABEL authors="penghuo@gmail.com"

RUN apt update
RUN apt install default-jdk -y \
&& apt install wget -y \
&& apt install supervisor -y

ENV SPARK_VERSION 3.1.3
ENV HADOOP_VERSION 3.2

# download and extract Spark 
RUN wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
&&  tar -xzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz \
&&  mv spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark

# add configuration
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY spark-env.sh /opt/spark/conf/spark-env.sh
COPY log4j.properties /opt/spark/conf/log4j.properties
COPY jars/*.* /opt/spark/jars
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# expose port 8080 for spark UI
EXPOSE 4040 6066 7077 8080 18080 8081 10000

# RUN wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-oss-7.10.2-linux-x86_64.tar.gz \
# &&  tar -xf elasticsearch-oss-7.10.2-linux-x86_64.tar.gz \
# &&  mv elasticsearch-7.10.2 /opt/elasticsearch


# RUN groupadd arion
# RUN useradd arion -g arion -p arion
# RUN chown -R arion:arion /opt/elasticsearch
# RUN chown -R arion:arion /opt/spark
# RUN chown -R arion:arion /opt/elasticsearch/config/elasticsearch.keystore
# RUN chmod o+x /opt/ /opt/elasticsearch/ /opt/spark
# RUN chgrp arion /opt/elasticsearch/
# RUN su - arion

#default command: this is just an option 
# CMD ["/opt/spark/sbin/start-thriftserver.sh"]
CMD ["/usr/bin/supervisord"]
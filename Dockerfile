FROM ubuntu:18.04

WORKDIR /root

# install openssh-server, openjdk and wget
#RUN apt-get update && apt-get install -y openssh-server openjdk-8-jdk wget
RUN apt-get update &&\
    apt-get -y install openjdk-8-jdk openssh-server wget vim 
    

# install hadoop 2.10.1


RUN wget https://downloads.apache.org/hadoop/common/hadoop-2.10.1/hadoop-2.10.1.tar.gz && \
    tar -xzf hadoop-2.10.1.tar.gz && \
    mv hadoop-2.10.1 /usr/local/hadoop && \
    rm hadoop-2.10.1.tar.gz

# install hadoop Pig 0.17

RUN wget http://apache.rediris.es/pig/latest/pig-0.17.0.tar.gz &&\
    tar -xzvf pig-0.17.0.tar.gz &&\
    mv pig-0.17.0 /opt/pig &&\
    rm pig-0.17.0.tar.gz

# set environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
ENV HADOOP_HOME=/usr/local/hadoop 
ENV PATH=$PATH:$HADOOP_HOME/bin:$JAVA_HOME/bin
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop/
ENV PIG_CLASSPATH=$HADOOP_CONF_DIR
ENV PIG_HOME=/opt/pig
ENV PATH=$PATH:$PIG_HOME/bin

# ssh without key
RUN ssh-keygen -t rsa -f ~/.ssh/id_rsa -P '' && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

RUN mkdir -p ~/hdfs/namenode && \ 
    mkdir -p ~/hdfs/datanode && \
    mkdir $HADOOP_HOME/logs

COPY config/* /tmp/

RUN mv /tmp/ssh_config ~/.ssh/config && \
    mv /tmp/hadoop-env.sh /usr/local/hadoop/etc/hadoop/hadoop-env.sh && \
    mv /tmp/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml && \ 
    mv /tmp/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml && \
    mv /tmp/mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml && \
    mv /tmp/yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml && \
    mv /tmp/slaves $HADOOP_HOME/etc/hadoop/slaves && \
    mv /tmp/start-hadoop.sh ~/start-hadoop.sh && \
    mv /tmp/run-wordcount.sh ~/run-wordcount.sh && \
    mv /tmp/block0.txt ~/block0.txt && \ 
    mv /tmp/img.txt ~/img.txt && \ 
    mv /tmp/img_parser.py ~/img_parser.py && \
    mv /tmp/g2-hlca.pig ~/g2-hlca.pig.pig

RUN chmod +x ~/start-hadoop.sh && \
    chmod +x ~/run-wordcount.sh && \
    chmod +x ~/g2-hlca.pig.pig && \
    chmod +x $HADOOP_HOME/sbin/start-dfs.sh && \
    chmod +x $HADOOP_HOME/sbin/start-yarn.sh 

# format namenode
RUN /usr/local/hadoop/bin/hdfs namenode -format

CMD [ "sh", "-c", "service ssh start; bash"]


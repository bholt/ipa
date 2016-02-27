#######################################################
# Cassandra image, customized to run Scala server, too.
#
# (based on https://github.com/docker-library/cassandra/blob/master/2.2/Dockerfile)
#######################################################
FROM cassandra:2.2.4
MAINTAINER Brandon Holt <holt.bg@gmail.com>

# Manually download and install latest scala and sbt.
RUN apt-get remove -y scala-library scala; \
    apt-get update && \
    apt-get install -y inetutils-ping wget && \
    wget http://www.scala-lang.org/files/archive/scala-2.11.7.deb && \
    dpkg -i scala-2.11.7.deb && \
    wget http://dl.bintray.com/sbt/debian/sbt-0.13.9.deb && \
    dpkg -i sbt-0.13.9.deb

# Fix java certs & pre-install latest sbt version
RUN update-ca-certificates -f && sbt version

RUN apt-get install -y ssh rsync git openjdk-8-jdk apt-transport-https

# ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
RUN update-alternatives --install "/usr/bin/javac" "javac" "/usr/lib/jvm/java-8-openjdk-amd64/bin/javac" 1 && \
    update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java" 1 && \
    update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java && \
    update-alternatives --set javac /usr/lib/jvm/java-8-openjdk-amd64/bin/javac

RUN mkdir -p /src; \
    git clone https://github.com/bholt/owl.git /src/owl && \
    cd /src/owl && \
    sbt test:compile

# override some configurations
ADD conf/cassandra.yaml /etc/cassandra/cassandra.yaml

RUN sed -ri "s/(hostname --ip-address)/\1 | awk '{print \$1}'/" /docker-entrypoint.sh

# 7000: intra-node communication
# 7001: TLS intra-node communication
# 7199: JMX
# 9042: CQL
# 9160: thrift service
EXPOSE 7000 7001 7199 9042 9160

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["cassandra", "-f"]

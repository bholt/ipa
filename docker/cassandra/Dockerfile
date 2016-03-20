#######################################################
# Cassandra image, customized to run Scala server, too.
#
# (based on https://github.com/docker-library/cassandra/blob/master/2.2/Dockerfile)
#######################################################
FROM cassandra:3.3
MAINTAINER Brandon Holt <holt.bg@gmail.com>

# Manually download and install latest scala and sbt.
RUN apt-get remove -y scala-library scala; \
    apt-get update && \
    apt-get install -y inetutils-ping wget && \
    wget http://www.scala-lang.org/files/archive/scala-2.11.7.deb && \
    dpkg -i scala-2.11.7.deb && \
    wget http://dl.bintray.com/sbt/debian/sbt-0.13.9.deb && \
    dpkg -i sbt-0.13.9.deb

# Fix java certs & pre-install latest sbt versionk
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
    sbt compile test:compile

# override some configurations
# ADD conf/cassandra.yaml /etc/cassandra/cassandra.yaml
RUN sed -ri "s/(listen_address:)/# \1/" /etc/cassandra/cassandra.yaml &&\
    sed -ri "s/# (listen_interface:).*$/\1 eth0/" /etc/cassandra/cassandra.yaml &&\
    sed -ri "s/(rpc_address:)/# \1/" /etc/cassandra/cassandra.yaml &&\
    sed -ri "s/# (rpc_interface:).*$/\1 eth0/" /etc/cassandra/cassandra.yaml &&\
    sed -ri "s/(enable_user_defined_functions:) false/\1 true/" /etc/cassandra/cassandra.yaml &&\
    sed -ri "s/(read_request_timeout_in_ms:).*$/\1 10000/" /etc/cassandra/cassandra.yaml &&\
    sed -ri "s/(range_request_timeout_in_ms:).*$/\1 20000/" /etc/cassandra/cassandra.yaml &&\
    sed -ri "s/(write_request_timeout_in_ms:).*$/\1 10000/" /etc/cassandra/cassandra.yaml &&\
    sed -ri "s/(counter_write_request_timeout_in_ms:).*$/\1 20000/" /etc/cassandra/cassandra.yaml

# Override with my own version of docker-entrypoint that doesn't overwrite addresses (uses interface eth0 instead)
ADD docker-entrypoint.sh /docker-entrypoint.sh

# 7000: intra-node communication
# 7001: TLS intra-node communication
# 7199: JMX
# 9042: CQL
# 9160: thrift service
EXPOSE 7000 7001 7199 9042 9160

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["cassandra", "-f"]

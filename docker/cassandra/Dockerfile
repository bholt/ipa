#######################################################
# My default image customized for Cassandra.
#
# (based on https://github.com/docker-library/cassandra/blob/master/2.2/Dockerfile)
#######################################################
FROM cassandra:3.3
MAINTAINER Brandon Holt <holt.bg@gmail.com>

# Manually download and install latest scala and sbt.
RUN apt-get remove -y scala-library scala
    apt-get update && \
    apt-get install -y inetutils-ping wget && \
    wget http://www.scala-lang.org/files/archive/scala-2.11.6.deb && \
    dpkg -i scala-2.11.6.deb && \
    wget http://dl.bintray.com/sbt/debian/sbt-0.13.9.deb && \
    dpkg -i sbt-0.13.9.deb

# Fix java certs & pre-install latest sbt version
RUN update-ca-certificates -f && sbt version

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

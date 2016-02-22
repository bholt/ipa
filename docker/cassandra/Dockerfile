#######################################################
# My default image customized for Cassandra.
#
# (based on https://github.com/docker-library/cassandra/blob/master/2.2/Dockerfile)
#######################################################
FROM cassandra:3.3
MAINTAINER Brandon Holt <holt.bg@gmail.com>

# Add source for latest sbt to apt
RUN apt-get update && \
    apt-get install -y apt-transport-https && \
    echo deb https://dl.bintray.com/sbt/debian / > /etc/apt/sources.list.d/sbt.list && \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823 && \
    apt-get update

# Install latest scala & sbt
RUN apt-get install -y scala sbt inetutils-ping

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

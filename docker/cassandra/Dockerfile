#######################################################
# My default image customized for Cassandra.
#
# (based on https://github.com/docker-library/cassandra/blob/master/2.2/Dockerfile)
#######################################################
FROM cassandra:2.2.4
MAINTAINER Brandon Holt <holt.bg@gmail.com>

# override some configurations
ADD conf/cassandra.yaml /etc/cassandra/cassandra.yaml

# 7000: intra-node communication
# 7001: TLS intra-node communication
# 7199: JMX
# 9042: CQL
# 9160: thrift service
EXPOSE 7000 7001 7199 9042 9160

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["cassandra", "-f"]

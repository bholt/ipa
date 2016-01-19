#######################################################
# Extension of basic dev image for Scala development.
#######################################################
FROM bholt/base
MAINTAINER Brandon Holt <holt.bg@gmail.com>

RUN echo deb https://dl.bintray.com/sbt/debian / > /etc/apt/sources.list.d/sbt.list && \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823 && \
    apt-get install apt-transport-https && \
    apt-get update

RUN apt-get install -y openjdk-8-jdk scala sbt

# fix java certs & pre-install latest sbt version
RUN update-ca-certificates -f && sbt version

CMD ["/usr/bin/zsh"]

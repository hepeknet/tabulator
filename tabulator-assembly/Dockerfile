FROM anapsix/alpine-java:8_jdk

MAINTAINER Hepek.net

ENV JAVA_OPTS -Xmx2g -Xms2g -XX:MaxPermSize=1024m -XX:+UseParallelGC

RUN mkdir -p /opt/hepek/tabulator/services/ /opt/hepek/tabulator/conf/
RUN mkdir -p /opt/hepek/data/
ADD target/tabulator*.jar /opt/hepek/tabulator/services/
ADD dockerStartupCommand.sh /opt/hepek/tabulator/services/
RUN chmod +x /opt/hepek/tabulator/services/dockerStartupCommand.sh

CMD ["/opt/hepek/tabulator/services/dockerStartupCommand.sh"]
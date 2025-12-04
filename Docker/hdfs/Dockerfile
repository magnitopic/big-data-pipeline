FROM apache/hadoop:3.3.6

USER root

# Create directories for HDFS data
RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data

# Copy Hadoop configuration files
COPY ./tools/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY ./tools/hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml

USER hadoop

CMD ["/bin/bash"]

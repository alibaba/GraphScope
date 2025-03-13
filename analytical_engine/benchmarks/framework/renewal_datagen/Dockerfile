FROM openjdk:8-jdk-buster

ENV DEBIAN_FRONTEND noninteractive

# Download Hadoop
WORKDIR /opt
RUN apt-get update
RUN apt-get install -y bash curl maven python
RUN curl -L 'https://archive.apache.org/dist/hadoop/core/hadoop-3.2.1/hadoop-3.2.1.tar.gz' | tar -xz

# Copy the project
COPY . /opt/ldbc_snb_datagen
WORKDIR /opt/ldbc_snb_datagen

# Fetch dependencies
RUN mvn dependency:resolve
# Build jar bundle
RUN mvn -DskipTests clean assembly:assembly

ENV HADOOP_CLIENT_OPTS '-Xmx6G'
CMD /opt/ldbc_snb_datagen/docker_run.sh

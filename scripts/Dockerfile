FROM centos:8

# to avoid "No URLs in mirrorlist"
RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
RUN sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*

ADD Gluten-Trino /opt/Gluten-Trino

RUN /opt/Gluten-Trino/cpp/scripts/setup-velox-centos8.sh
RUN rm -rf /opt/Gluten-Trino/cpp/thirdparty
RUN dnf install -y jq

RUN echo "source /opt/rh/gcc-toolset-9/enable" >> ~/.bashrc

RUN wget "https://cdn.azul.com/zulu/bin/zulu17.44.53-ca-jdk17.0.8.1-linux_x64.tar.gz"
RUN tar zxf zulu17.44.53-ca-jdk17.0.8.1-linux_x64.tar.gz
RUN rm -rf zulu17.44.53-ca-jdk17.0.8.1-linux_x64.tar.gz

ENV JAVA_HOME=/zulu17.44.53-ca-jdk17.0.8.1-linux_x64
ENV PATH=$PATH:$JAVA_HOME/bin

RUN wget "https://dlcdn.apache.org/maven/maven-3/3.9.5/binaries/apache-maven-3.9.5-bin.tar.gz"
RUN tar zxf apache-maven-3.9.5-bin.tar.gz
RUN rm -rf apache-maven-3.9.5-bin.tar.gz

ENV MAVEN_HOME=/apache-maven-3.9.5
ENV PATH=$PATH:$MAVEN_HOME/bin

RUN /opt/Gluten-Trino/scripts/build_all.sh

RUN mv /opt/Gluten-Trino/release /Gluten-Trino
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/Gluten-Trino
RUN rm -rf /opt/Gluten-Trino
 
WORKDIR /
RUN wget "https://repo1.maven.org/maven2/io/trino/trino-server/411/trino-server-411.tar.gz"
RUN tar zxf trino-server-411.tar.gz
RUN rm -rf trino-server-411.tar.gz
 
RUN sed -i 's/classpath = pathjoin/classpath = \"\/Gluten-Trino\/Gluten-Trino-411.jar\:\" + pathjoin/g' /trino-server-411/bin/launcher.py

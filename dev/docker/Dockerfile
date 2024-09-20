FROM mirrors.tencent.com/tlinux/tlinux3.2-minimal:v20221214

MAINTAINER sharkdtu@tencent.com

RUN dnf install -y epel-release

RUN dnf install -y \
  kernel-headers sudo wget curl git which perl bison flex patch \
  gcc-toolset-9 ninja-build ccache autoconf autoconf-archive automake libtool \
  java-8-konajdk python39 python39-devel python39-pip libxml2 libxml2-devel libgsasl libgsasl-devel

RUN dnf clean all

ENV JAVA_HOME /usr/lib/jvm/java

RUN wget https://dlcdn.apache.org/maven/maven-3/3.8.8/binaries/apache-maven-3.8.8-bin.tar.gz
RUN tar -xvf apache-maven-3.8.8-bin.tar.gz
RUN mv apache-maven-3.8.8 /usr/lib/maven
RUN rm apache-maven-3.8.8-bin.tar.gz
ENV MAVEN_HOME=/usr/lib/maven
ENV PATH=${MAVEN_HOME}/bin:${PATH}

RUN ulimit -c unlimited

CMD ["/bin/bash"]

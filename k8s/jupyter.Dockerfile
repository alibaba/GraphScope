FROM centos:7

RUN yum -y update && \
    yum -y install epel-release && \
    yum -y install gcc openssl-devel bzip2-devel libffi-devel && \
    yum -y install yum-utils && yum -y groupinstall development && \
    yum install -y python36 python3-libs python3-devel python3-setuptools python3-pip && \
    yum clean all && \
    rm -fr /var/cache/yum


# Add the user UID:1000, GID:1000, home at /app
RUN groupadd -r app -g 1000 && useradd -u 1000 -r -g app -m -d /app -s /sbin/nologin -c "App user" app && \
    chmod 755 /app

RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install jupyterlab graphscope

RUN mkdir -p /main

WORKDIR /main

CMD ["jupyter", "lab", "--port=8888", "--no-browser", "--ip=0.0.0.0", "--allow-root"]

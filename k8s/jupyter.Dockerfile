FROM centos:7

RUN yum -y update && \
    yum -y install epel-release && \
    yum -y install gcc openssl-devel bzip2-devel libffi-devel && \
    yum -y install yum-utils && yum -y groupinstall development && \
    yum install -y python36 python3-libs python3-devel python3-setuptools python3-pip && \
    yum clean all && \
    rm -fr /var/cache/yum


RUN useradd -m graphscope -u 1001 && \
    echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

USER graphscope
WORKDIR /home/graphscope

ENV PATH=${PATH}:/home/graphscope/.local/bin

RUN python3 -m pip install --upgrade pip --user
RUN python3 -m pip install jupyterlab graphscope-client==0.8.0 --user

CMD ["jupyter", "lab", "--port=8888", "--no-browser", "--ip=0.0.0.0"]

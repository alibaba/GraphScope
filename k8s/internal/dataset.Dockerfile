FROM centos:7

RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-Linux-*
RUN sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-Linux-*

RUN yum localinstall -y https://github.com/aliyun/ossfs/releases/download/v1.80.5/ossfs_1.80.5_centos7.0_x86_64.rpm && \
    yum clean all && \
    rm -fr /var/cache/yum

RUN mkdir /dataset

CMD ["ossfs", "graphscope:/dataset", "/dataset", "-o", "url=https://oss-cn-beijing.aliyuncs.com", "-o", "public_bucket=1", "-o", "ro,nosuid,nodev,allow_other", "-o", "dbglevel=debug", "-f", "-d"]
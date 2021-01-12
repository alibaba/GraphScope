# graphscope-vineyard image is based on graphscope-runtime, and will install
# libgrape-lite, vineyard, as well as necessary IO dependencies (e.g., hdfs, oss)
# in the image

FROM reg.docker.alibaba-inc.com/weibin/vineyard-mars:0.1.6

ADD entrypoint.sh /srv/entrypoint.sh

RUN chmod a+x /srv/entrypoint.sh

COPY ./k8s/kube_ssh /opt/graphscope/bin/kube_ssh
COPY ./k8s/pre_stop.py /opt/graphscope/bin/pre_stop.py
COPY . /root/gs

# build analytical engine
RUN export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/graphscope/lib:/opt/graphscope/lib64 && \
    cd /root/gs/analytical_engine && \
    mkdir -p build && \
    cd build && \
    cmake .. -DCMAKE_PREFIX_PATH="/opt/graphscope;/opt/conda" \
             -DEXPERIMENTAL_ON=OFF \
             -DBUILD_TESTS=OFF && \
    make gsa_cpplint && \
    make -j`nproc` && \
    make install && \
    rm -fr CMake* && \
    echo "Build and install analytical_engine done."

# build python bdist_wheel
RUN export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/graphscope/lib:/opt/graphscope/lib64 && \
    export WITH_LEARNING_ENGINE=OFF && \
    cd /root/gs/python && \
    pip install -U setuptools && \
    pip install -r requirements.txt -r requirements-dev.txt && \
    python setup.py bdist_wheel && \
    cd ./dist && \
    pip install ./*.whl && \
    cd /root/gs/coordinator && \
    pip install -r requirements.txt -r requirements-dev.txt && \
    python setup.py bdist_wheel && \
    pip install dist/*.whl

RUN rm -f /dev/null && mknod -m 666 /dev/null c 1 3 \
  && mkdir /var/run/sshd \
  && echo 'root:123456' | chpasswd \
  && sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \
  && sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd \
  && echo "export VISIBLE=now" >> /etc/profile \
  && echo 'admin:123456' | chpasswd \
  && mkdir /home/admin/.ssh && ssh-keygen -f /home/admin/.ssh/id_rsa -N "" \
  && chown -R admin:admin /home/admin/.ssh \
  && ssh-keygen -t dsa -f /etc/ssh/ssh_host_dsa_key -N "" && chmod 600 /etc/ssh/ssh_host_dsa_key \
  && ssh-keygen -t rsa -f /etc/ssh/ssh_host_rsa_key -N "" && chmod 600 /etc/ssh/ssh_host_rsa_key \
  && echo '%admin ALL=(ALL) NOPASSWD:SETENV:ALL' >> /etc/sudoers

RUN /bin/cp -f /home/admin/.ssh/id_rsa.pub /home/admin/.ssh/authorized_keys && \
    /bin/cp -rf /home/admin/.ssh /root/.ssh && \
    chmod 700 /root/.ssh && \
    chmod 600 /root/.ssh/* 

RUN rm -fr /roo/gs



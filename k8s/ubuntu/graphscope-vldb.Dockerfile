FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:ubuntu0.2.5

COPY . /tmp/GraphScope

RUN useradd -m jovyan && \
  echo 'jovyan ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers && \
  cp -r ~/.cargo /home/jovyan/.cargo && \
  chown -R jovyan:jovyan /home/jovyan/.cargo && \
  chown -R jovyan:jovyan /tmp/GraphScope
  
USER jovyan

SHELL ["/bin/bash", "-c"]

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=/home/jovyan/.local/bin:${JAVA_HOME}/bin:${PATH}:/usr/local/go/bin:/usr/local/zookeeper/bin
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib
ENV GS_TEST_DIR=/home/jovyan/datasets
ENV GRAPHSCOPE_PREFIX=/usr/local
ENV WITH_LEARNING_ENGINE=ON

RUN source /home/jovyan/.cargo/env && \
  rustup install stable && rustup default stable && rustup component add rustfmt && \
  cd /tmp/GraphScope && make install && \
  echo "source ~/.cargo/env" >> ~/.bashrc && \
  python3 /tmp/GraphScope/k8s/precompile.py

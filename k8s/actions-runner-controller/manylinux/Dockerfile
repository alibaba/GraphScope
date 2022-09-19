FROM registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-vineyard:v0.8.3

ARG TARGETPLATFORM
ARG RUNNER_VERSION=2.287.1
ARG DOCKER_CHANNEL=stable
ARG DOCKER_VERSION=20.10.12
ARG DUMB_INIT_VERSION=1.2.5

RUN test -n "$TARGETPLATFORM" || (echo "TARGETPLATFORM must be set" && false)


# arch command on OS X reports "i386" for Intel CPUs regardless of bitness
RUN export ARCH=$(echo ${TARGETPLATFORM} | cut -d / -f2) \
    && if [ "$ARCH" = "arm64" ]; then export ARCH=aarch64 ; fi \
    && if [ "$ARCH" = "amd64" ] || [ "$ARCH" = "i386" ]; then export ARCH=x86_64 ; fi \
    && sudo curl -f -L -o /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v${DUMB_INIT_VERSION}/dumb-init_${DUMB_INIT_VERSION}_${ARCH} \
    && sudo chmod +x /usr/local/bin/dumb-init

# Docker download supports arm64 as aarch64 & amd64 / i386 as x86_64
RUN set -vx; \
    export ARCH=$(echo ${TARGETPLATFORM} | cut -d / -f2) \
    && if [ "$ARCH" = "arm64" ]; then export ARCH=aarch64 ; fi \
    && if [ "$ARCH" = "amd64" ] || [ "$ARCH" = "i386" ]; then export ARCH=x86_64 ; fi \
    && curl -f -L -o docker.tgz https://download.docker.com/linux/static/${DOCKER_CHANNEL}/${ARCH}/docker-${DOCKER_VERSION}.tgz \
    && tar zxvf docker.tgz \
    && sudo install -o root -g root -m 755 docker/docker /usr/local/bin/docker \
    && rm -rf docker docker.tgz

ENV RUNNER_ASSETS_DIR=/runnertmp
ENV HOME=/home/graphscope

# Uncomment the below COPY to use your own custom build of actions-runner.
#
# To build a custom runner:
# - Clone the actions/runner repo `git clone git@github.com:actions/runner.git $repo`
# - Run `cd $repo/src`
# - Run `./dev.sh layout Release linux-x64`
# - Run `./dev.sh package Release linux-x64`
# - Run cp ../_package/actions-runner-linux-x64-2.280.3.tar.gz ../../actions-runner-controller/runner/
#   - Beware that `2.280.3` might change across versions
#
# See https://github.com/actions/runner/blob/main/.github/workflows/release.yml for more informatino on how you can use dev.sh
#
# If you're willing to uncomment the following line, you'd also need to comment-out the
#   && curl -L -o runner.tar.gz https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-${ARCH}-${RUNNER_VERSION}.tar.gz \
# line in the next `RUN` command in this Dockerfile, to avoid overwirting this runner.tar.gz with a remote one.

# COPY actions-runner-linux-x64-2.280.3.tar.gz /runnertmp/runner.tar.gz

# Runner download supports amd64 as x64. Externalstmp is needed for making mount points work inside DinD.
#
# libyaml-dev is required for ruby/setup-ruby action.
# It is installed after installdependencies.sh and before removing /var/lib/apt/lists
# to avoid rerunning apt-update on its own.
RUN export ARCH=$(echo ${TARGETPLATFORM} | cut -d / -f2) \
    && if [ "$ARCH" = "amd64" ] || [ "$ARCH" = "x86_64" ] || [ "$ARCH" = "i386" ]; then export ARCH=x64 ; fi \
    && sudo mkdir -p "$RUNNER_ASSETS_DIR" \
    && cd "$RUNNER_ASSETS_DIR" \
    # Comment-out the below curl invocation when you use your own build of actions/runner
    && sudo curl -f -L -o runner.tar.gz https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-${ARCH}-${RUNNER_VERSION}.tar.gz \
    && sudo tar xzf ./runner.tar.gz \
    && sudo rm runner.tar.gz \
    && sudo ./bin/installdependencies.sh \
    && sudo mv ./externals ./externalstmp

RUN curl -L https://github.com/mozilla/sccache/releases/download/v0.2.15/sccache-v0.2.15-x86_64-unknown-linux-musl.tar.gz | sudo tar xz \
    && sudo mv sccache-v0.2.15-x86_64-unknown-linux-musl/sccache /usr/local/bin/ \
    &&sudo chmod +x /usr/local/bin/sccache

ENV RUNNER_TOOL_CACHE=/opt/hostedtoolcache
RUN sudo mkdir /opt/hostedtoolcache \
    && sudo chgrp graphscope /opt/hostedtoolcache \
    && sudo chmod g+rwx /opt/hostedtoolcache

COPY entrypoint.sh /

# Add the Python "User Script Directory" to the PATH
ENV PATH="${PATH}:${HOME}/.local/bin"
ENV ImageOS=manylinux2014

USER graphscope

ENTRYPOINT ["/usr/local/bin/dumb-init", "--"]
CMD ["/entrypoint.sh"]


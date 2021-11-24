# The graphscope image includes all runtime stuffs of graphscope, with analytical engine,
# learning engine and interactive engine installed.

FROM python:3.9

# Add graphscope user with user id 1001
RUN apt update -y && apt install sudo openjdk-11-jdk -y && \
    useradd -m graphscope -u 1001 && \
    echo 'graphscope ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers && \
    rm -fr /var/lib/apt/lists/*

# Change to graphscope user
USER graphscope
WORKDIR /home/graphscope

ENV PATH=${PATH}:/home/graphscope/.local/bin

COPY . /home/graphscope/gs

# Install wheel package from current directory if "artifacts" exists.
# Otherwise, exec `pip install graphscope` from Pypi.
# The "artifacts" directory is corresponds to ".github/workflow/ci.yml" file.
RUN cd /home/graphscope/gs && \
    # install graphscope
    if [ -d "/home/graphscope/gs/artifacts" ]; then \
        pushd artifacts/python/dist/wheelhouse; \
        for f in * ; do python3 -m pip install $f; done || true; \
        popd; \
        pushd artifacts/coordinator/dist/wheelhouse; \
        python3 -m pip install ./*.whl; \
        popd; \
        pushd coordinator/dist; \
        python3 -m pip install ./*.whl; \
    else \
        python3 -m pip install graphscope; \
    fi && \
    pip3 install git+https://github.com/mars-project/mars.git@d09e1e4c3e32ceb05f42d0b5b79775b1ebd299fb#egg=pymars

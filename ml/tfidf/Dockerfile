FROM ubuntu:18.04

# Install dependencies
RUN apt-get update \
    && apt-get -y install software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get -y install wget python3.8

# Copy the config file in as a secret
RUN --mount=type=secret,id=config cp /run/secrets/config config.yml

# Download and install Miniconda
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh && \
    mkdir -p /opt && \
    sh miniconda.sh -b -p /opt/conda && \
    rm miniconda.sh && \
    ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "conda activate base" >> ~/.bashrc && \
    find /opt/conda/ -follow -type f -name '*.a' -delete && \
    find /opt/conda/ -follow -type f -name '*.js.map' -delete

# Install dependencies
ADD requirements.txt .
RUN /opt/conda/bin/conda install --file requirements.txt

# Cleanup conda environment to save space
RUN /opt/conda/bin/conda clean -afy && \
    /opt/conda/bin/conda clean -pfy

# Copy over the script and support files
ADD apostrophe_terms_to_save.txt .
ADD autotagging.py .
ADD stopwords.txt .

# Launch
EXPOSE 80

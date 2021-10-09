FROM ubuntu:18.04

# Install dependencies
RUN apt-get update \
    && apt-get -y install software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get -y install wget git python3.8

# Checkout the repo
RUN git clone https://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@github.com/davenovelli/dogbeach.git

# Move into the repo directory
WORKDIR "/dogbeach"

# Delete everything in the root dir except the /ml directory
#RUN find . ! -name 'ml' -type d -exec rm -rf {} +
RUN ls -Q | grep -v ml | xargs rm -rf

# Switch to the tagging code directory
WORKDIR "/dogbeach/ml/tfidf"

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

### TEMP FOR TESTING, THESE WILL COME FROM GITHUB #########################
ADD autotagging.py .
ADD requirements.txt .

# Install dependencies
RUN /opt/conda/bin/conda install --file requirements.txt

# Cleanup conda environment to save space
RUN /opt/conda/bin/conda clean -afy && \
    /opt/conda/bin/conda clean -pfy

# Launch
EXPOSE 80

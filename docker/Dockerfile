# Use an argument for the Python version, with a default
ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim

# Install system dependencies
RUN apt-get update \
    && apt-get install -y vim ca-certificates --no-install-recommends \
    && update-ca-certificates \
    && mkdir -p /etc/pki/tls/certs \
    && ln -s /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt

# Upgrade pip and install the Python package
RUN pip install --upgrade pip

# Copy your Python package and scripts
COPY . /app

# Set the working directory to /app
WORKDIR /app

# Install your Python package
RUN python setup.py install

# Assuming docker_test.sh is executable and located in the root of your project
#CMD ["./tests/docker_test.sh"]
#CMD ["./tests/hello.sh"]


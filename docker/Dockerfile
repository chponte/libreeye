FROM python:3.7.6-slim-buster

# Install ffmpeg
RUN apt-get update && apt-get install --no-install-recommends -y ffmpeg

# Install package
COPY . /tmp/libreeye
RUN /bin/bash -c " \
    cd /tmp/libreeye && \
    python3 setup.py sdist && \
    pip install --no-cache-dir dist/*.tar.gz && \
    rm -rf /tmp/*"

# Clear cache
RUN apt-get clean

CMD ["pip", "show", "libreeye"]
FROM python:3.7.4-stretch

# FFmpeg version
ENV NASM_VERSION 2.14.02
ENV FFMPEG_VERSION 4.1.3

# Build FFmpeg from source
RUN set -ex && \
    # Install dependencies available in Debian stretch repo
    apt-get update -qq && \
    apt-get -y install \
        autoconf \
        automake \
        build-essential \
        cmake \
        git-core \
        fonts-liberation \
        libass-dev \
        libfreetype6-dev \
        libtool \
        libvorbis-dev \
        pkg-config \
        texinfo \
        wget \
        zlib1g-dev \
        libx264-dev \
        libx265-dev \
        libnuma-dev \
        libvpx-dev && \
    # NASM version ≥ 2.13 not available in repo, build from source
    mkdir -p /usr/src/nasm && \
    wget -O nasm.tar.xz -q https://www.nasm.us/pub/nasm/releasebuilds/$NASM_VERSION/nasm-$NASM_VERSION.tar.xz && \
    tar -xf nasm.tar.xz -C /usr/src/nasm --strip 1 && \
    rm nasm.tar.xz && \
    cd /usr/src/nasm && \
    ./autogen.sh && \
    ./configure && \
    make && \
    make install && \
    cd && \
    rm -r /usr/src/nasm && \
    # Build FFmpeg
    mkdir -p /usr/src/ffmpeg && \
    wget -O ffmpeg.tar.xz -q https://ffmpeg.org/releases/ffmpeg-$FFMPEG_VERSION.tar.xz && \
    tar -xf ffmpeg.tar.xz -C /usr/src/ffmpeg --strip 1 && \
    rm ffmpeg.tar.xz && \
    cd /usr/src/ffmpeg && \
    PKG_CONFIG_PATH="/usr/local/lib/pkgconfig" ./configure \
        --pkg-config-flags='--static' \
        --extra-libs="-lpthread -lm" \
        --enable-gpl \
#        --enable-libaom \
#        --enable-libass \
#        --enable-libfdk-aac \
        --enable-libfreetype \
        --enable-libfontconfig \
        --enable-libfribidi \
#        --enable-libmp3lame \
#        --enable-libopus \
#        --enable-libvorbis \
        --enable-libx264 \
        --enable-libx265 \
        --enable-libvpx \
        --enable-nonfree && \
    make && \
    make install && \
    cd && \
    rm -r /usr/src

WORKDIR /surveillance

COPY . /surveillance

RUN pip install -r requirements.txt

CMD [ "python", "main.py" ]

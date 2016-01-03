#!/bin/sh
set -e

case $1 in
  prepare)
    apt-get update
    apt-get -y install gcc xz-utils libpq5 libpq-dev curl
    apt-get clean

    curl http://johnvansickle.com/ffmpeg/releases/ffmpeg-release-64bit-static.tar.xz \
        | xz -d \
        | tar xC /usr/bin --strip-components=1

    rm /usr/bin/ffserver
    rm /usr/bin/ffmpeg-10bit
    ;;

  cleanup)
    apt-get -y purge gcc curl xz-utils libpq-dev
    apt-get -y autoremove
    ;;
esac

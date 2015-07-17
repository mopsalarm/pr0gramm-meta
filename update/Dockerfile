from python:3-onbuild
MAINTAINER Mopsalarm

RUN curl http://johnvansickle.com/ffmpeg/releases/ffmpeg-release-64bit-static.tar.xz \
    | xz -d \
    | tar xC /usr/bin --strip-components=1

# this script starts at /usr/src/app
CMD python main.py

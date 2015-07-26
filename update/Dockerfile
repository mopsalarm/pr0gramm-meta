FROM gliderlabs/python-runtime:3.4
MAINTAINER Mopsalarm

RUN curl http://johnvansickle.com/ffmpeg/releases/ffmpeg-release-64bit-static.tar.xz \
    | xz -d \
    | tar xC /usr/bin --strip-components=1

RUN apk del git tar xz && apk cache clean
RUN mkdir -p /data

WORKDIR /data
CMD /env/bin/python /app/main.py

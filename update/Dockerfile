from python:2-onbuild
MAINTAINER Mopsalarm

RUN apt-get update && apt-get install -y --force-yes libav-tools && apt-get clean

# this script starts at /usr/src/app
CMD python main.py

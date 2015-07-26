FROM gliderlabs/python-runtime:3.4
MAINTAINER Mopsalarm

# this script starts at /app
CMD /env/bin/python -m bottle -s wsgiref -b 0.0.0.0:8080 service

EXPOSE 8080

FROM gliderlabs/python-runtime:3.4
MAINTAINER Mopsalarm

EXPOSE 8080
WORKDIR /data
CMD PYTHONPATH=/app /env/bin/python -m bottle -s wsgiref -b 0.0.0.0:8080 service

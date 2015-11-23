FROM gliderlabs/python-runtime:3.5
MAINTAINER Mopsalarm

EXPOSE 8080
CMD PYTHONPATH=/app /env/bin/python -m bottle -s cherrypy -b 0.0.0.0:8080 service

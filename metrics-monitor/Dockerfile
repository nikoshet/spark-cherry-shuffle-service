# base image
FROM python:3.7-slim-stretch

RUN rm -rf /var/cache/apk/* && \
    rm -rf /tmp/* && \
    apt-get update && \
    pip3 install requests && \
    apt-get install -y curl
    #apt-get install -y maven scala

WORKDIR metrics-monitor
ADD metrics-monitor ./

# python
# RUN apt-get install -y python3 python3-setuptools python3-pip # && pip3 install numpy
# RUN python3 --version

RUN chmod +x *.py

## Final Command
ENTRYPOINT ["python3", "-u", "./metricsMonitor.py"]

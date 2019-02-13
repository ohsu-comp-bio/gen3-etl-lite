#

FROM python:2.7

LABEL maintainer="walsbr@ohsu.edu"

RUN mkdir /gen3_replicator
WORKDIR /gen3_replicator
COPY requirements.txt /gen3_replicator

VOLUME ["/config"]

RUN pip install -r requirements.txt

COPY . /gen3_replicator

# RUN COMMIT=`git rev-parse HEAD` && echo "COMMIT=\"${COMMIT}\"" >version_data.py \
#   && VERSION=`git describe --always --tags` && echo "VERSION=\"${VERSION}\"" >>version_data.py


HEALTHCHECK --interval=5m --timeout=3s \
  CMD healthcheck.sh

ENTRYPOINT [ "/bin/sh", "/gen3_replicator/dockerrun.sh" ]

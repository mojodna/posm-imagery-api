FROM ubuntu:16.04
MAINTAINER Seth Fitzsimmons <seth@mojodna.net>

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && \
  apt-get install -y --no-install-recommends software-properties-common && \
  add-apt-repository ppa:ubuntugis/ubuntugis-unstable && \
  apt-get update && \
  apt-get upgrade -y && \
  apt-get install -y --no-install-recommends \
    apt-transport-https \
    build-essential \
    gdal-bin \
    git \
    libgdal-dev \
    lsb-release \
    python-dev \
    python-pip \
    python-setuptools \
    python-wheel \
    software-properties-common \
    wget && \
  wget -q -O - https://deb.nodesource.com/gpgkey/nodesource.gpg.key | apt-key add - && \
  add-apt-repository -s "deb https://deb.nodesource.com/node_4.x $(lsb_release -c -s) main" && \
  apt-get update && \
  apt-get install --no-install-recommends -y nodejs && \
  apt-get clean

COPY package.json /app/package.json
COPY requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip install -U numpy && \
  pip install -Ur requirements.txt && \
  pip install -U gevent gunicorn && \
  rm -rf /root/.cache

RUN npm install && \
  rm -rf /root/.npm

COPY . /app

EXPOSE 8000
USER nobody
VOLUME /app/imagery

ENTRYPOINT ["gunicorn", "-k", "gevent", "-b", "0.0.0.0", "--access-logfile", "-", "app:app"]

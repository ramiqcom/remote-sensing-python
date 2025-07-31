FROM gcr.io/ramadhan-s4g/eo-base:latest

WORKDIR /usr/src/app

COPY requirements.txt requirements.txt

RUN python3 -m venv .venv && \
  .venv/bin/pip install -r requirements.txt

COPY . .

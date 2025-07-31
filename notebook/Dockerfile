FROM gcr.io/ramadhan-s4g/eo-base:latest

WORKDIR /usr/src/app

COPY . .

RUN python3 -m venv .venv \
  .venv/bin/pip install -r requirements.txt

EXPOSE 8888

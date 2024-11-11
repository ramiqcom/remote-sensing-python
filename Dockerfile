FROM python:3.11-bookworm

EXPOSE 8888

COPY ./ .

RUN pip install -r requirements.txt

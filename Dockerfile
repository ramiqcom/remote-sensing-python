FROM python:3.12-bookworm

WORKDIR /app

COPY ./ .

RUN python.exe -m pip install --upgrade pip
RUN pip install -r requirements.txt

EXPOSE 8888

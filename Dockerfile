FROM quay.io/jupyter/base-notebook

COPY ./ .

RUN pip install -r requirements.txt

FROM python:3


COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY . /script
WORKDIR /script

CMD sleep 3600
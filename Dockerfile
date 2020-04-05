FROM python:3.7

LABEL maintainer="Martyn Green <martyn.robert.green@gmail.com>"

COPY . /app
WORKDIR /app/

ENV PYTHONPATH=/app

CMD ["glitterbot.py"]

FROM python:3.7
LABEL maintainer="Martyn Green <martyn.robert.green@gmail.com>"

RUN pip install pipenv

COPY . /app
WORKDIR /app/

ENV PYTHONPATH=/app
RUN pipenv lock -r > requirements.txt && pip install -r requirements.txt

CMD ["python", "/app/glitterbot.py"]

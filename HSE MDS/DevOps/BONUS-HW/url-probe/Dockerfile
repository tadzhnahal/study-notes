FROM python:3.12-alpine

RUN adduser -D probe

WORKDIR /app

COPY probe.py run-probe.sh ./
COPY crontab /etc/crontabs/probe

RUN chmod +x run-probe.sh

CMD ["crond", "-f", "-d", "8"]

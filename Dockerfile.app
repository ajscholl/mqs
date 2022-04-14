FROM debian:buster

ARG app

RUN apt-get update && \
    apt-get upgrade -yy && \
    apt-get install -yy libpq5 && \
    apt-get autoremove && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /app

WORKDIR /app
CMD ["/usr/local/bin/app"]

COPY ./bin/${app} /usr/local/bin/app

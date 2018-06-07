FROM node:8.5.0-alpine

RUN apk add --no-cache unzip tar gzip

WORKDIR /cassandra-exporter

ADD ./ ./
RUN chmod +x entrypoint.sh

RUN npm install

VOLUME /data
ENV DIRECTORY /data
ENTRYPOINT ["/cassandra-exporter/entrypoint.sh"]
CMD ["export"]

#!/bin/sh

case "$1" in
import)
    shift
    exec node /cassandra-exporter/import.js "$@"
;;
export)
    shift
    exec node /cassandra-exporter/export.js "$@"
;;
*)
    exec "$@"
esac

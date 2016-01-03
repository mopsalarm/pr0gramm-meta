#!/bin/sh
set -e

case $1 in
  prepare)
    apt-get update
    apt-get -y install gcc libpq5 libpq-dev
    apt-get clean
    ;;

  cleanup)
    apt-get -y purge gcc libpq-dev
    apt-get -y autoremove
    ;;
esac

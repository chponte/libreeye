#!/usr/bin/env bash

if [[ "$#" -ne 4 ]] ; then
    echo "Incorrect number of arguments:\n$0 <SERVICE_NAME> <CONFIG_FILE> <STORAGE_DIR> <LOG_DIR>"
    exit 1
fi

SERVICE_NAME=$1
CONFIG_FILE=$2
STORAGE_DIR=$3
LOG_DIR=$4

INSTALL_DIR="$(dirname $(readlink -f "$0"))"

docker build -t chponte/surveillance ${INSTALL_DIR}
sed -e "s^{USER}^$(whoami)^" \
    -e "s^{CONTAINER_NAME}^$SERVICE_NAME^" \
    -e "s^{CONFIG_DIR}^$(dirname $(readlink -f "${CONFIG_FILE}"))^" \
    -e "s^{STORAGE_DIR}^$STORAGE_DIR^" \
    -e "s^{LOG_DIR}^$(readlink -f "${LOG_DIR}")^" \
    -e "s^{IMAGE_NAME}^chponte/surveillance^" \
    -e "s^{CONFIG_NAME}^$(basename ${CONFIG_FILE})^" \
    ${INSTALL_DIR}/systemd/service.template > ${INSTALL_DIR}/systemd/${SERVICE_NAME}.service
sudo ln -s ${INSTALL_DIR}/systemd/${SERVICE_NAME}.service /etc/systemd/system/${SERVICE_NAME}.service

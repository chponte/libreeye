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
sed -e "s^{USER}^$(whoami)^g" \
    -e "s^{DOCKER}^$(which docker)^g" \
    -e "s^{CONTAINER_NAME}^$SERVICE_NAME^g" \
    -e "s^{UID}^$(id -u)^g" \
    -e "s^{CONFIG_DIR}^$(dirname $(readlink -f "${CONFIG_FILE}"))^g" \
    -e "s^{STORAGE_DIR}^$STORAGE_DIR^g" \
    -e "s^{LOG_DIR}^$(readlink -f "${LOG_DIR}")^g" \
    -e "s^{IMAGE_NAME}^chponte/surveillance^g" \
    -e "s^{CONFIG_NAME}^$(basename ${CONFIG_FILE})^g" \
    ${INSTALL_DIR}/systemd/service.template > ${INSTALL_DIR}/systemd/${SERVICE_NAME}.service
sudo systemctl enable ${INSTALL_DIR}/systemd/${SERVICE_NAME}.service

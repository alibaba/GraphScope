#!/bin/bash

until nslookup "${DNS_NAME_SERVICE_KAFKA}"; do
  echo "waiting for kafka...";
  sleep 1;
done;

for ((i=0; i < ${FRONTEND_COUNT}; i++)); do
  until nslookup "${DNS_NAME_PREFIX_FRONTEND/\{\}/${i}}"; do
    echo "waiting for frontend...";
    sleep 1;
  done;
done;

for ((i=0; i < ${INGESTOR_COUNT}; i++)); do
  until nslookup "${DNS_NAME_PREFIX_INGESTOR/\{\}/${i}}"; do
    echo "waiting for ingestor...";
    sleep 1;
  done;
done;

for ((i=0; i < ${COORDINATOR_COUNT}; i++)); do
  until nslookup "${DNS_NAME_PREFIX_COORDINATOR/\{\}/${i}}"; do
    echo "waiting for coordinator...";
    sleep 1;
  done;
done;

for ((i=0; i < ${STORE_COUNT}; i++)); do
  until nslookup "${DNS_NAME_PREFIX_STORE/\{\}/${i}}"; do
    echo "waiting for store...";
    sleep 1;
  done;
done;

echo "All component started"


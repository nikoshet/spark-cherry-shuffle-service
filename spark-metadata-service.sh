#!/bin/bash

. common.sh

echo "$(hostname -i) spark-metadata-service"
echo "$(hostname -i) spark-metadata-service" >> /etc/hosts


echo "Starting Metadata Service..."
/bin/bash sbin/start-metadata-service.sh --host spark-metadata-service --port 5555

exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"
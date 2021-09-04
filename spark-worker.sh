#!/bin/bash

. common.sh

if ! getent hosts spark-master; then
  sleep 5
  exit 0
fi

cat /etc/hosts

echo "$(hostname -i) spark-worker"

#sudo
/bin/bash sbin/start-worker.sh spark://spark-master:7077 --webui-port 8081

exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"

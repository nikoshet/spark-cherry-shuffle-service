#!/bin/bash

. common.sh
#ls
#cd /opt/spark/bin
#ls
echo "$(hostname -i) spark-master"
echo "$(hostname -i) spark-master" >> /etc/hosts

#sudo
/bin/bash sbin/start-master.sh --ip spark-master --host spark-master --port 7077 --webui-port 8080
                                                          #$(hostname -i)

exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"

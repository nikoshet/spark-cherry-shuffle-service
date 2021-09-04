#!/bin/bash

. common.sh

echo "$(hostname -i) spark-cherry-shuffle-service"
echo "$(hostname -i) spark-cherry-shuffle-service" >> /etc/hosts


####----------- FLAGS TO USE -----------####
## -c: llok.ahead.caching
# values: true / false(default)
## -p: port for rpcEndpoint
# values: Int, default:7788


### Examples ###

# /bin/bash sbin/start-cherry.sh --host spark-cherry-shuffle-service --port 7777

# /bin/bash sbin/start-cherry.sh --host spark-cherry-shuffle-service --port 7777 --look.ahead.caching true --look.ahead.caching.port 7788

### /Examples ###

while getopts "l:r:s:z:g:" opt; do
  case $opt in
    l) caching=$OPTARG     ;;
    r) cachingPort=$OPTARG    ;;
    s) size=$OPTARG    ;;
    z) distributed=$OPTARG    ;;
    g) gc=$OPTARG    ;;
    *) echo 'error' >&2
       exit 1
  esac
done

if [[ gc = "true" ]]; then
  if [[ $caching = "true" ]]; then
    echo "Starting CHERRY Shuffle Service with look ahead caching..."
    if [[ -z "$cachingPort" ]]; then
      $cachingPort=7788
    fi

    if [[ $distributed = "true" ]]; then
     /bin/bash sbin/start-cherry-gc.sh --host $(hostname -i) --port 7777 --look.ahead.caching true --look.ahead.caching.host spark-cherry-shuffle-service --look.ahead.caching.size $size --look.ahead.caching.port $cachingPort --distributed true
    elif [[-z "$distributed" ]] || [[ $distributed = "false" ]]; then
      /bin/bash sbin/start-cherry-gc.sh --host spark-cherry-shuffle-service --port 7777 --look.ahead.caching true --look.ahead.caching.host spark-cherry-shuffle-service --look.ahead.caching.size $size --look.ahead.caching.port $cachingPort
    fi

  else
    echo "Starting CHERRY Shuffle Service..."
    if [[ $distributed = "true" ]]; then
     /bin/bash sbin/start-cherry-gc.sh --host $(hostname -i) --port 7777 --distributed true
    elif [[ -z "$distributed" ]] || [[ $distributed = "false" ]]; then
      /bin/bash sbin/start-cherry-gc.sh --host spark-cherry-shuffle-service --port 7777
    fi
  fi

else
  if [[ $caching = "true" ]]; then
    echo "Starting CHERRY Shuffle Service with look ahead caching..."
    if [[ -z "$cachingPort" ]]; then
      $cachingPort=7788
    fi

    if [[ $distributed = "true" ]]; then
     /bin/bash sbin/start-cherry.sh --host $(hostname -i) --port 7777 --look.ahead.caching true --look.ahead.caching.host spark-cherry-shuffle-service --look.ahead.caching.size $size --look.ahead.caching.port $cachingPort --distributed true
    elif [[-z "$distributed" ]] || [[ $distributed = "false" ]]; then
      /bin/bash sbin/start-cherry.sh --host spark-cherry-shuffle-service --port 7777 --look.ahead.caching true --look.ahead.caching.host spark-cherry-shuffle-service --look.ahead.caching.size $size --look.ahead.caching.port $cachingPort
    fi

  else
    echo "Starting CHERRY Shuffle Service..."
    if [[ $distributed = "true" ]]; then
     /bin/bash sbin/start-cherry.sh --host $(hostname -i) --port 7777 --distributed true
    elif [[ -z "$distributed" ]] || [[ $distributed = "false" ]]; then
      /bin/bash sbin/start-cherry.sh --host spark-cherry-shuffle-service --port 7777
    fi
  fi
fi

exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"

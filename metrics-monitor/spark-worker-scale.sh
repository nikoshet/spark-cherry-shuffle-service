#!/bin/sh

set -e

NUMBER_OF_REPLICAS="$1"
CURRENT_NAMESPACE="spark"
DEPLOYMENT_NAME="spark-worker"

KUBE_TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
KUBE_CACRT_PATH="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

PAYLOAD="{\"spec\":{\"replicas\":$NUMBER_OF_REPLICAS}}"

curl  --cacert $KUBE_CACRT_PATH \
     -X PATCH \
     -H "Content-Type: application/strategic-merge-patch+json" \
     -H "Authorization: Bearer $KUBE_TOKEN" \
     --data "$PAYLOAD" \
     https://$KUBERNETES_SERVICE_HOST:$KUBERNETES_PORT_443_TCP_PORT/apis/apps/v1/namespaces/$CURRENT_NAMESPACE/deployments/$DEPLOYMENT_NAME #/scale


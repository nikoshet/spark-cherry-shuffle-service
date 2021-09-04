#!/usr/bin/env bash

# chmod +x app.sh
# sudo ./app.sh

#sudo service docker restart
#echo "Building Docker Image for spark code"
#docker build -f spark-code/Dockerfile -t nikoshet/k8s-spark:spark-v1 . #/spark-code
#echo ""
#echo "Pushing images to DockerHub"
#docker push nikoshet/k8s-spark:spark-v1
#docker push nikoshet/k8s-spark:remote-shuffle-service-v1

#echo ""
#echo "Pulling images from DockerHub to other VMs available"
## must optimize it to get dynamically the ip for nodes
#ssh ubuntu@10.0.1.169 "docker pull nikoshet/k8s-spark:spark-v1"
#ssh ubuntu@10.0.1.145 "docker pull nikoshet/k8s-spark:spark-v1"
#ssh ubuntu@10.0.1.169 "docker pull nikoshet/k8s-spark:remote-shuffle-service-v1"
#ssh ubuntu@10.0.1.145 "docker pull nikoshet/k8s-spark:remote-shuffle-service-v1"


# To Remove the taints on the master so that you can schedule pods on it:
## kubectl taint nodes --all node-role.kubernetes.io/master- # not used
## kubectl label nodes nnik-1 type=master
# kubectl label nodes nnik-2 type=master-driver
# kubectl label nodes nnik-3, etc type=worker

#sudo docker build -f spark-code/Dockerfile -t nikoshet/k8s-spark:spark-v1 . && sudo docker push nikoshet/k8s-spark:spark-v1 && ssh ubuntu@10.0.1.169 "sudo docker pull nikoshet/k8s-spark:spark-v1" && ssh ubuntu@10.0.1.145 "sudo docker pull nikoshet/k8s-spark:spark-v1"

echo ""
echo "Starting Master Pod"
kubectl create namespace spark
#kubectl create -f ./kubernetes/spark-master-service.yaml --namespace=spark
#kubectl create -f ./kubernetes/spark-master-deployment.yaml --namespace=spark

echo ""
echo "Starting Remote Shuffle Service Pod"
#kubectl create -f ./kubernetes/spark-remote-cherry-service-service.yaml --namespace=spark
#kubectl create -f ./kubernetes/spark-remote-cherry-service-deployment.yaml --namespace=spark


sleep 10
echo ""
echo "Starting Worker Pod(s)"
# kubectl create -f ./kubernetes/spark-worker-service.yaml --namespace=spark ?? usw only for prometheus??
#kubectl create -f ./kubernetes/spark-worker-deployment.yaml --namespace=spark
#kubectl scale deployments/spark-worker --replicas=2 --namespace=spark

kubectl get pods --all-namespaces -o wide
kubectl get svc --all-namespaces -o wide

#sleep 10
#echo ""
#echo "Starting Driver Pod"
#kubectl create -f ./kubernetes/spark-driver-service.yaml --namespace=spark
#kubectl create -f ./kubernetes/spark-driver-deployment.yaml --namespace=spark
# OR ONLY kubectl create -f ./kubernetes/spark-driver-pod.yaml --namespace=spark
# OR ONLY kubectl create -f ./kubernetes/spark-driver-job.yaml --namespace=spark -> runs only one time as needed
#kubectl get pods



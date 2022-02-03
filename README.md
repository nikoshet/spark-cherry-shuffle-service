# Cherry: A Distributed Task-Aware Shuffle Service for Serverless Analytics

[![GitHub license](https://img.shields.io/badge/license-GNU-blue.svg)](https://raw.githubusercontent.com/nikoshet/spark-cherry-shuffle-service/master/LICENSE)

## Table of Contents

+ [About](#about)
+ [Getting Started](#getting_started)
    + [Prerequisites](#prerequisites)
    + [Available Ansible Playbooks](#playbooks)
    + [Configuration](#configuration)
+ [Execution Options](#execution_options)	
+ [Available Workloads](#available_workloads)	
+ [Deployment](#deployment)
+ [Built With](#built_with)
+ [License](#license)

## About <a name = "about"></a>
Code for the paper ["Cherry: A Distributed Task-Aware Shuffle Service for Serverless Analytics"](https://ieeexplore.ieee.org/xpl/conhome/9671263/proceeding).

While there has been a lot of effort in recent years in optimising Big Data systems like Apache Spark and Hadoop, the all-to-all transfer of data between a MapReduce computation step, i.e., the shuffle data mechanism between cluster nodes remains always a serious bottleneck. In this work, we present Cherry, an open-source distributed task-aware Caching sHuffle sErvice for seRveRless analYtics. Our thorough experiments on a cloud testbed using realistic and synthetic workloads showcase that Cherry can achieve an almost 23% to 39% reduction in completion of the reduce stage with small shuffle block sizes, a 10% reduction in execution time on real workloads, while it can efficiently handle Spark execution failures with a constant task time re-computation overhead compared to existing approaches.

The current implementation has been made with Python 3.7, Apache Spark 3.0.1, Kubernetes 1.20.1, Docker 20.10.1, Java 8, Scala 3.1.0 and Ansible 2.10. Also, Calico CNI has been used as a Network Plugin for Kubernetes, and Prometheus Operator 0.48.1 for monitoring the Kubernetes Cluster.

## Getting Started <a name = "getting_started"></a>

The following instructions will help you run this project on a Kubernetes cluster. The already implemented Ansible playbooks will help speed up this procedure.

### Prerequisites <a name = "prerequisites"></a>

As mentioned above, you will need to install the correct versions of Python, Apache Spark, Kubernetes (kubeadm, kubelet, kubectl), Docker, Java, Scala and Ansible to all the hosts in the available cluster.
Firstly, install Ansible to all nodes as follows:
```
sudo apt install software-properties-common
sudo apt-add-repository --yes --update ppa:ansible/ansible
sudo apt install ansible

```
In order to install the rest of the software required with Ansible, first configure the info of the `/ansible/inventory/hosts` file based on your cluster. Then execute the following:
```
cd ./ansible
ansible-playbook -i inventory prerequisites.yml
ansible-playbook -i inventory create_kubernetes_cluster.yml
ansible-playbook -i inventory additional_configuration.yml
ansible-playbook -i inventory playbooks/start_kubernetes_services.yml
```
The above commands will configure the Kubernetes cluster. More specifically:
- [X] Disable Swap on each node
- [X] Add IPs to path /etc/hosts on each node
- [X] Enable Passwordless SSH netween nodes
- [X] Install Java, Python, Scala, Docker, kubeadm, kubelet, kubectl and enable Kubernetes Services
- [X] Initialize Kubernetes Cluster from Master node
- [X] Install Calico CNI Network Add-on
- [X] Join Worker nodes with kubernetes master
- [X] Install Python Docker module and Log into Docker Hub to store and retrieve Docker images
- [X] Add monitoring packages for Kubernetes (i.e., prometheus-operator)
- [X] Label Kubernetes nodes accordingly, create namespace, start Kubernetes Services for Spark and Prometheus to scrape Spark metrics

### Available Ansible Playbooks <a name = "playbooks"></a>
There have also been implemented other Ansible Playbooks in the `/ansible/playbooks` folder that do the following:
- Build and Push Docker Images to Docker Hub
- Pull Docker Images from Docker Hub for each node
- Remove old Docker Images to free disk space
- Clear RAM
- Copy data to Worker nodes for Spark Workloads
- Destroy the Kubernetes Cluster
- Create an HDFS Cluster and Generate TPC-DS Data

### Configuration <a name = "configuration"></a>
In order to configure Spark, there are `/conf/spark-env.sh` and `/conf/spark-defaults.conf` files available for this role. 
In order to overwrite these configurations there is a specific Bash script (the `spark-driver.sh` file) to implement this procedure.


## Execution Options <a name = "execution_options"></a>
To simplify the deployment of a Spark Cluster and a Spark workload as a Kubernetes Job with different parameters, the aforementioned script is used, and the flags it accepts are the following:
```
## -s: service
# values: cherry, external, none
## -w: workload
# values: synthetic, skew, tpcds, file.py, file.jar
## -c: class
# values: java.class if workload==jar
# -d: dataset
# values: .cvs, null, etc
# -p: parallelism
# values: number
# -l: look-ahead caching enabled for Cherry
# values: boolean (default:false)
# -r: look-ahead caching port
# values: Port number (default: 7788)
# -g: gigabytes # size of data created in GBs between 2 stages
# values: Int (e.g. 1, 10, 100)
# -z: distributed # uses distributed Cherry service
# values: Int (e.g. 1, 10, 100)
# -k: skewness if workload==skew
# values: [0, 1]
```
## Available Workloads <a name = "available_workloads"></a>
The available workloads are the following:
- synthetic.workload.py
- skewed_synthetic_workload.py
- amazon_customer_reviews_workload.py
- TPCDS Benchmark (after using the Ansible playbook that creates an HDFS cluster and generates TPC-DS Benchmark Data)

More on how to deploy a Spark Workload with examples in a Kubernetes cluster in the next section.

## Deployment <a name = "deployment"></a>
To deploy a Spark Cluster with differently configured workloads, you need to deploy the implemented Spark Metadata Service, the Spark Master, Workers, Cherry shuffle services and finally the Driver job. Example command:
```
kubectl delete deploy spark-metadata-service spark-worker spark-cherry-shuffle-service -n spark \
&& kubectl delete job spark-driver -n spark \
&& sleep 1m \
&& kubectl create -f ./kubernetes/spark-metadata-service/spark-metadata-service-deployment.yaml --namespace=spark \
&& sleep 1m \
&& kubectl create -f ./kubernetes/spark-cherry-shuffle-service/spark-cherry-shuffle-service-deployment.yaml --namespace=spark \
&& kubectl create -f ./kubernetes/spark-worker/spark-worker-deployment.yaml --namespace=spark \
&& kubectl scale deployments/spark-worker --replicas=10 --namespace=spark \
&& kubectl scale deployments/spark-cherry-shuffle-service --replicas=10 --namespace=spark \
&& sleep 1m \
&& kubectl create -f ./kubernetes/spark-driver/spark-driver-job.yaml --namespace=spark
```

Example commands with flags for the `spark-driver.sh` script to execute different workloads (need to be modified in the `/kubernetes/spark-driver/spark-driver-job.yaml` file):
```
# executes the TPC-DS Q2 and Q5 queries with 100 mappers in first stage and Vanilla Spark is used
["/spark/spark-driver.sh", "-s", "none", "-w", "tpcds", "-p", "100", "-q", "q2,q5"] 
# executes the synthetic workload with 1500 mappers, creates 5GB of shuffle data, uses distributed Cherry shuffle service and the look-ahead caching policy 
["/spark/spark-driver.sh", "-s", "cherry", "-w", "synthetic", "-p", "1500", "-g", "5", "-l", "true", "-r", "7788", "-z", "true"]
# executes the skewed synthetic workload with distributed Cherry and caching, and skewness=0.8
command: [ "/spark/spark-driver.sh", "-s", "cherry", "-w", "skew", "-p", "2000", "-g", "20", "-l", "true", "-r", "7788", "-z", "true", "-k", "0.8"] 
```

## Built With <a name = "built_with"></a>
* [Apache Spark 3.0.1](https://spark.apache.org/docs/3.0.1/) 
* [Python 3.7](https://docs.python.org/3/whatsnew/3.7.html)
* [Kubernetes 1.20.1](https://kubernetes.io/blog/2020/12/08/kubernetes-1-20-release-announcement/)
* [Docker 20.10.1](https://www.docker.com/blog/introducing-docker-engine-20-10/)
* [Java 8](https://www.oracle.com/java/technologies/java8.html)
* [Scala 3.1.0](https://scala-lang.org/blog/2021/10/21/scala-3.1.0-released.html)
* [Ansible 2.10](https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html)
* [Calico CNI](https://projectcalico.docs.tigera.io/reference/cni-plugin/configuratio)
* [Prometheus Operator 0.48.1](https://github.com/prometheus-operator/prometheus-operator)


## License <a name = "license"></a>
This project is licensed under the GNU License - see the [LICENSE](LICENSE) file for details.

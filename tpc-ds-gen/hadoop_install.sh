#!/bin/bash

HADOOP_VERSION=3.2.0
HDFS_MASTER_IP=nnik2-1 #0.0.0.0 #localhost #nnik2-1 #10.0.1.167

download__hadoop () {
	## Go to Home Folder
	cd ~

	## Download the hadoop tar
	wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz

	## Extract hadoop tar
	tar -xzf hadoop-${HADOOP_VERSION}.tar.gz

	## Export environmental variables.
	#echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> ~/.bashrc
	echo 'export HADOOP_INSTALL=/home/ubuntu/hadoop-${HADOOP_VERSION}' >> ~/.bashrc
	echo 'export PATH=$PATH:$HADOOP_INSTALL/bin' >> ~/.bashrc
	echo 'export PATH=$PATH:$HADOOP_INSTALL/sbin' >> ~/.bashrc
	echo 'export HADOOP_HOME=$HADOOP_INSTALL' >> ~/.bashrc
	echo 'export HADOOP_COMMON_HOME=$HADOOP_INSTALL' >> ~/.bashrc
	echo 'export HADOOP_HDFS_HOME=$HADOOP_INSTALL' >> ~/.bashrc
	echo 'export HADOOP_CONF_DIR=$HADOOP_INSTALL/etc/hadoop' >> ~/.bashrc
	echo 'export HDFS_NAMENODE_USER="ubuntu"' >> ~/.bashrc
	echo 'export HDFS_DATANODE_USER="ubuntu"' >> ~/.bashrc
	echo 'export HDFS_SECONDARYNAMENODE_USER="ubuntu"' >> ~/.bashrc
	echo 'export YARN_RESOURCEMANAGER_USER="ubuntu"' >> ~/.bashrc
	echo 'export YARN_NODEMANAGER_USER="ubuntu"' >> ~/.bashrc
	source ~/.bashrc

	echo 'export HDFS_NAMENODE_USER="ubuntu"' >> ~/hadoop-3.2.0/etc/hadoop/hadoop-env.sh
	echo 'export HDFS_DATANODE_USER="ubuntu"' >> ~/hadoop-3.2.0/etc/hadoop/hadoop-env.sh
	echo 'export HDFS_SECONDARYNAMENODE_USER="ubuntu"' >> ~/hadoop-3.2.0/etc/hadoop/hadoop-env.sh
	echo 'export YARN_RESOURCEMANAGER_USER="ubuntu"' >> ~/hadoop-3.2.0/etc/hadoop/hadoop-env.sh
	echo 'export YARN_NODEMANAGER_USER="ubuntu"' >> ~/hadoop-3.2.0/etc/hadoop/hadoop-env.sh
}

configure_hadoop () {
## Edit core-site.xml to set hdfs default path to hdfs://master:6666
## Browser Port -> 9870
		CORE_SITE_CONTENT="\t<property>\n\t\t<name>fs.default.name</name>\n\t\t<value>hdfs://${HDFS_MASTER_IP}:6666</value>\n\t</property>"
		INPUT_CORE_SITE_CONTENT=$(echo $CORE_SITE_CONTENT | sed 's/\//\\\//g')
		sed -i "/<\/configuration>/ s/.*/${INPUT_CORE_SITE_CONTENT}\n&/" /home/ubuntu/hadoop-${HADOOP_VERSION}/etc/hadoop/core-site.xml


## Edit hdfs-site.xml to set hadoop file system parameters
		HDFS_SITE_CONTENT="\t<property>\n\t\t<name>dfs.replication</name>\n\t\t<value>1</value>\n\t\t<description>Default block replication.</description>\n\t</property>"
		HDFS_SITE_CONTENT="${HDFS_SITE_CONTENT}\n\t<property>\n\t\t<name>dfs.namenode.name.dir</name>\n\t\t<value>/home/ubuntu/hdfsname</value>\n\t</property>"
		HDFS_SITE_CONTENT="${HDFS_SITE_CONTENT}\n\t<property>\n\t\t<name>dfs.datanode.data.dir</name>\n\t\t<value>/home/ubuntu/hdfsdata</value>\n\t</property>"
		HDFS_SITE_CONTENT="${HDFS_SITE_CONTENT}\n\t<property>\n\t\t<name>dfs.blocksize</name>\n\t\t<value>64m</value>\n\t\t<description>Block size</description>\n\t</property>"
		HDFS_SITE_CONTENT="${HDFS_SITE_CONTENT}\n\t<property>\n\t\t<name>dfs.webhdfs.enabled</name>\n\t\t<value>true</value>\n\t</property>"
		HDFS_SITE_CONTENT="${HDFS_SITE_CONTENT}\n\t<property>\n\t\t<name>dfs.support.append</name>\n\t\t<value>true</value>\n\t</property>"
		INPUT_HDFS_SITE_CONTENT=$(echo $HDFS_SITE_CONTENT | sed 's/\//\\\//g')
		sed -i "/<\/configuration>/ s/.*/${INPUT_HDFS_SITE_CONTENT}\n&/" /home/ubuntu/hadoop-${HADOOP_VERSION}/etc/hadoop/hdfs-site.xml

### If you have specifically defined data node and namenode directory in hdfs-site.xml, then delete all the files under those directories.
#rm -rf hdfsdata  hdfsname

## Set the one datanode for the distributed filesystem
		echo "master" > /home/ubuntu/hadoop-${HADOOP_VERSION}/etc/hadoop/slaves
		echo "${HDFS_MASTER_IP}" > /home/ubuntu/hadoop-${HADOOP_VERSION}/etc/hadoop/slaves
		#echo "slave" >> /home/ubuntu/hadoop-${HADOOP_VERSION}/etc/hadoop/slaves
		echo "nnik2-17" >> /home/ubuntu/hadoop-${HADOOP_VERSION}/etc/hadoop/slaves
		echo "nnik2-16" >> /home/ubuntu/hadoop-${HADOOP_VERSION}/etc/hadoop/slaves

## Export JAVA_HOME variable for hadoop
		sed -i '/export JAVA\_HOME/c\export JAVA\_HOME=\/usr\/lib\/jvm\/java-8-openjdk-amd64' /home/ubuntu/hadoop-${HADOOP_VERSION}/etc/hadoop/hadoop-env.sh
}

echo "STARTING DOWNLOAD ON MASTER"
#download__hadoop

echo "STARTING DOWNLOAD ON SLAVES"
#ssh ubuntu@nnik2-17 "$(typeset -f download__hadoop); download__hadoop"
#ssh ubuntu@nnik2-16 "$(typeset -f download__hadoop); download__hadoop"


echo "STARTING HADOOP CONFIGURE ON MASTER"
source ~/.bashrc; configure_hadoop

echo "STARTING HADOOP CONFIGURE ON SLAVE"
#ssh user@nnik2-17 "$(typeset -f configure_hadoop); configure_hadoop"
#ssh user@nnik2-16 "$(typeset -f configure_hadoop); configure_hadoop"

echo "OK"
source ~/.bashrc
sudo apt-get install python3.7-gdbm

## Format hdfs
~/hadoop-3.2.0/bin/hdfs namenode -format
## Start hdfs
#~/hadoop-3.2.0/sbin/start-dfs.sh

cd ~/hdfsname/current
sudo chown ubuntu *
cd

$HADOOP_HOME/sbin/stop-all.sh
#$HADOOP_HOME/bin/hadoop namenode –format
sudo -u ubuntu hadoop-3.2.0/bin/hadoop namenode -format
$HADOOP_HOME/sbin/start-all.sh

## Check that hfds has started
#jps
jps && ssh nnik2-16 jps && ssh nnik2-17 jps
#hadoop dfsadmin -report
hadoop-3.2.0/bin/hadoop dfsadmin -report



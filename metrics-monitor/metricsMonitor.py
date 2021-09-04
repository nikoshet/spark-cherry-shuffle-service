from datetime import datetime
import time
import requests
import json


def get_metrics():
####----- General Metrics -----####
#--- Worker Pod Current CPU Usage
	spark_worker_pod_current_cpu_usage_response = requests.get(PROMETHEUS + MAIN_PATH,
	params={
		'query': """sum(
		node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate{namespace="spark"}
		* on(namespace,pod)
		group_left(workload, workload_type) namespace_workload_pod:kube_pod_owner:relabel{namespace="spark", workload="spark-worker"}
		) by (pod)"""
	})
	results = spark_worker_pod_current_cpu_usage_response.json()
	worker_cpu_usage = {}
	for result in results['data']['result']:
		#print(' {metric}: {value[1]}'.format(**result))
		worker_cpu_usage[result['metric']['pod']] = float(result['value'][1]) * 100
	print("worker_cpu_usage (%)", json.dumps(worker_cpu_usage))

#--- Worker Pod Current Memory Usage
	spark_worker_pod_current_mem_usage_response = requests.get(PROMETHEUS + MAIN_PATH,
	params={
		'query': """sum(
		container_memory_working_set_bytes{namespace="spark", container!="", image!=""}
		* on(namespace, pod)
		group_left(workload, workload_type) namespace_workload_pod:kube_pod_owner:relabel{namespace="spark", workload="spark-worker"}
		) by (pod)"""
	})
	results = spark_worker_pod_current_mem_usage_response.json()
	worker_mem_usage = {}
	for result in results['data']['result']:
		worker_mem_usage[result['metric']['pod']] = float(result['value'][1]) / (1024**2)
	print("worker_mem_usage (MB)", json.dumps(worker_mem_usage))

#--- Available Memory Per Node
	cluster_available_mem_response = requests.get(PROMETHEUS + MAIN_PATH,
	params={
		'query': """node_memory_MemFree_bytes{job="node-exporter"}"""
	})
	results = cluster_available_mem_response.json()
	available_mem = {}
	for result in results['data']['result']:
		if result['metric']['instance'] != "10.0.1.174:9100":
			available_mem[result['metric']['instance']] = float(result['value'][1]) / (1024**3)
	print("available_mem (GB)",  json.dumps(available_mem))

#--- CPU Usage Per Node
	cluster_cpu_usage_response = requests.get(PROMETHEUS + MAIN_PATH,
	params={
		'query': """(instance:node_cpu_utilisation:rate1m{job="node-exporter"}
		*instance:node_num_cpu:sum{job="node-exporter"})
		/ scalar(sum(instance:node_num_cpu:sum{job="node-exporter"}))"""
	})
	results = cluster_cpu_usage_response.json()
	cpu_usage = {}
	for result in results['data']['result']:
		if result['metric']['instance'] != "10.0.1.174:9100":
			cpu_usage[result['metric']['instance']] = float(result['value'][1]) * 100
	print("cpu_usage (%)", json.dumps(cpu_usage))


####----- Spark Metrics -----####
#--- Alive Workers
	spark_master_alive_workers_response = requests.get(PROMETHEUS + MAIN_PATH,
	params={
		'query': """metrics_master_aliveWorkers_Number{namespace="spark", service="spark-master"}"""
	})
	results = spark_master_alive_workers_response.json()
	alive_workers = 0
	#for result in results['data']['result']:
	# {'metric': {'__name__': 'metrics_master_aliveWorkers_Number', 'endpoint': 'spark-ui', 'instance': '192.168.243.78:8080',
	#'job': 'spark-master', 'namespace': 'spark', 'pod': 'spark-master-79fbdb6867-d2l4s', 'service': 'spark-master', 'type': 'gauges'}, 'value': [1611054664.489, '2']}
	alive_workers = int(results['data']['result'][0]['value'][1])
	print("alive_workers", alive_workers)

#--- Number Of Waiting Stages
	wating_stages_of_job_response = requests.get(PROMETHEUS + MAIN_PATH,
	params={
		'query': """spark_metrics_number_of_waiting_stages_of_job{}"""
	})
	results = wating_stages_of_job_response.json()
	#print(results)
	waiting_stages = int(results['data']['result'][0]['value'][1])
	print("waiting_stages", waiting_stages)

#--- Number Of Running Stages
	running_stages_of_job_response = requests.get(PROMETHEUS + MAIN_PATH,
	params={
		'query': """spark_metrics_number_of_running_stages_of_job{}"""
	})
	results = running_stages_of_job_response.json()
	#print(results)
	running_stages = int(results['data']['result'][0]['value'][1])
	print("running_stages", running_stages)

#--- Number Of Tasks For Running Stage
	num_tasks_of_running_stage_response = requests.get(PROMETHEUS + MAIN_PATH,
	params={
		'query': """spark_metrics_running_stage_numTasks{}"""
	})
	results = num_tasks_of_running_stage_response.json()
	#print(results)
	num_tasks = {}
	for result in results['data']['result']:
		num_tasks[result['metric']['running_stage_id']] = int(result['value'][1])
	print("num_tasks", num_tasks) # running_stage_id1: num_tasks1, running_stage_id2: num_task2

#--- Running Time Of Running Stage
	running_time_of_running_stage_response = requests.get(PROMETHEUS + MAIN_PATH,
	params={
		'query': """spark_metrics_running_stage_running_time_sec"""
	})
	results = running_time_of_running_stage_response.json()
	#print(results)
	running_time = {}
	for result in results['data']['result']:
		running_time[result['metric']['running_stage_id']] = float(result['value'][1])
	print("running_time (sec)", running_time)

#--- Complete Tasks Of Running Stage
	complete_tasks_of_running_stage_response = requests.get(PROMETHEUS + MAIN_PATH,
	params={
		'query': """spark_metrics_running_stage_stageData_complete_tasks"""
	})
	results = complete_tasks_of_running_stage_response.json()
	#print(results)
	complete_tasks = {}
	for result in results['data']['result']:
		complete_tasks[result['metric']['running_stage_id']] = int(result['value'][1])
	print("complete_tasks", complete_tasks)

#--- Total Number Of Tasks Of Waiting Stages
	number_of_tasks_of_waiting_stages = requests.get(PROMETHEUS + MAIN_PATH,
	params={
		'query': """spark_metrics_waiting_stage_numTasks"""
	})
	results = number_of_tasks_of_waiting_stages.json()
	#print(results)
	num_tasks_of_waiting_stages = {}
	for result in results['data']['result']:
		num_tasks_of_waiting_stages[result['metric']['waiting_stage_id']] = int(result['value'][1])
	print("num_tasks_of_waiting_stages", num_tasks_of_waiting_stages)


####----- Check If Scaling Is Needed -----####
	for cpu_usage in worker_cpu_usage.values():
		if cpu_usage > 90:
			import os
			worker_replicas = alive_workers + 1
			print("Scaling out to " + str(worker_replicas) + " spark worker replicas...")
			#command = '/usr/local/bin/kubectl scale deployments/spark-worker --replicas=' + str(worker_replicas) + ' --namespace=spark'
			#os.system(command) #p = os.system(command)
			os.system("/bin/bash spark-worker-scale.sh "+ str(worker_replicas))
			print("Scaling is successful.")
			time.sleep(60 - time.time() % 60)
		break


# Main Code
if __name__ == '__main__':

	# Main url
	PROMETHEUS = 'http://10.0.1.174:9090/'
	MAIN_PATH = 'api/v1/query'

	while True:
		now = datetime.now()
		dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
		print("Time: ", dt_string)
		try:
			# Check if spark job is submitted. If not, an IndexError will be thrown
			check_if_spark_job_is_submitted = False
			check_if_spark_job_is_submitted_response = requests.get(PROMETHEUS + MAIN_PATH,
			params={
		 	'query': """spark_info{}"""
			})
			results = check_if_spark_job_is_submitted_response.json()
			res = float(results['data']['result'][0]['value'][1])
			# Call main function to get metrics
			get_metrics()

		except IndexError:
			print("Waiting for a Spark job to be submitted...")

		# Run loop every 15 seconds
		print("\n")
		time.sleep(15 - time.time() % 15)


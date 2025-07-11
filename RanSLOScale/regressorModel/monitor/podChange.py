from codecs import lookup_error
import os
import time
from config.Config import Config
import pandas as pd
from util.PrometheusClient import PrometheusClient
import random

# Get the response time of the invocation edges
def collect_call_latency(config: Config, first:bool):
    call_df = pd.DataFrame()

    prom_util = PrometheusClient(config)
    # P50，P90，P99
    #prom_50_sql = 'histogram_quantile(0.50, sum(irate(istio_request_duration_milliseconds_bucket{reporter=\"destination\", destination_workload_namespace=\"%s\"}[1m])) by (destination_workload, destination_workload_namespace, source_workload, le))' % config.namespace
    prom_90_sql = 'histogram_quantile(0.90, sum(irate(istio_request_duration_milliseconds_bucket{reporter=\"destination\", destination_workload_namespace=\"%s\"}[1m])) by (destination_workload, destination_workload_namespace, source_workload, le))' % config.namespace
    #prom_99_sql = 'histogram_quantile(0.99, sum(irate(istio_request_duration_milliseconds_bucket{reporter=\"destination\", destination_workload_namespace=\"%s\"}[1m])) by (destination_workload, destination_workload_namespace, source_workload, le))' % config.namespace
    #responses_50 = prom_util.execute_prom(config.prom_range_url, prom_50_sql)
    responses_90 = prom_util.execute_prom(config.prom_range_url, prom_90_sql)
    #responses_99 = prom_util.execute_prom(config.prom_range_url, prom_99_sql)
    def handle(result, call_df, type):
        name = result['metric']['source_workload'] + '_' + result['metric']['destination_workload']
        values = result['values']
        values = list(zip(*values))
        if 'timestamp' not in call_df:
            timestamp = values[0]
            call_df['timestamp'] = timestamp
            call_df['timestamp'] = call_df['timestamp'].astype('datetime64[s]')
        metric = values[1]
        key = name + '&' + type
        call_df[key] = pd.Series(metric)
        call_df[key] = call_df[key].astype('float64')

    #[handle(result, call_df, 'p50') for result in responses_50]
    [handle(result, call_df, 'p90') for result in responses_90]
    #[handle(result, call_df, 'p99') for result in responses_99]

    responsetime=call_df['unknown_frontend&p90']

    print(responsetime)

    return responsetime





# Get the response time for the microservices
def collect_svc_latency(config: Config, _dir: str, first: bool):
    latency_df = pd.DataFrame()

    prom_util = PrometheusClient(config)
    # P50，P90，P99
    prom_50_sql = 'histogram_quantile(0.50, sum(irate(istio_request_duration_milliseconds_bucket{reporter=\"destination\", destination_workload_namespace=\"%s\"}[1m])) by (destination_workload, destination_workload_namespace, le))' % config.namespace
    prom_90_sql = 'histogram_quantile(0.90, sum(irate(istio_request_duration_milliseconds_bucket{reporter=\"destination\", destination_workload_namespace=\"%s\"}[1m])) by (destination_workload, destination_workload_namespace, le))' % config.namespace
    prom_99_sql = 'histogram_quantile(0.99, sum(irate(istio_request_duration_milliseconds_bucket{reporter=\"destination\", destination_workload_namespace=\"%s\"}[1m])) by (destination_workload, destination_workload_namespace, le))' % config.namespace
    responses_50 = prom_util.execute_prom(config.prom_range_url, prom_50_sql)
    responses_90 = prom_util.execute_prom(config.prom_range_url, prom_90_sql)
    responses_99 = prom_util.execute_prom(config.prom_range_url, prom_99_sql)

    def handle(result, latency_df, type):
        name = result['metric']['destination_workload']
        values = result['values']
        values = list(zip(*values))
        if 'timestamp' not in latency_df:
            timestamp = values[0]
            latency_df['timestamp'] = timestamp
            latency_df['timestamp'] = latency_df['timestamp'].astype('datetime64[s]')
        metric = values[1]
        key = name + '&' + type
        latency_df[key] = pd.Series(metric)
        latency_df[key] = latency_df[key].astype('float64')

    [handle(result, latency_df, 'p50') for result in responses_50]
    [handle(result, latency_df, 'p90') for result in responses_90]
    [handle(result, latency_df, 'p99') for result in responses_99]

    path = os.path.join(_dir, 'latency.csv')
    if (first):
        latency_df.to_csv(path, index=False)
    else:
        latency_df.to_csv(path, mode='a', header=False, index=False)

# 获取机器的vCPU和memory使用
def collect_resource_metric(config: Config, _dir: str, first: bool):
    metric_df = pd.DataFrame()
    vCPU_sql = 'sum(rate(container_cpu_usage_seconds_total{image!="",namespace="%s"}[1m]))' % config.namespace
    mem_sql = 'sum(rate(container_memory_usage_bytes{image!="",namespace="%s"}[1m])) / (1024*1024)' % config.namespace
    prom_util = PrometheusClient(config)
    vCPU = prom_util.execute_prom(config.prom_range_url, vCPU_sql)
    mem = prom_util.execute_prom(config.prom_range_url, mem_sql)

    def handle(result, metric_df, col):
        values = result['values']
        values = list(zip(*values))
        if 'timestamp' not in metric_df:
            timestamp = values[0]
            metric_df['timestamp'] = timestamp
            metric_df['timestamp'] = metric_df['timestamp'].astype('datetime64[s]')
        metric = values[1]
        metric_df[col] = pd.Series(metric)
        metric_df[col] = metric_df[col].fillna(0)
        metric_df[col] = metric_df[col].astype('float64')

    [handle(result, metric_df, 'vCPU') for result in vCPU]
    [handle(result, metric_df, 'memory') for result in mem]

    path = os.path.join(_dir, 'resource.csv')
    if (first):
        metric_df.to_csv(path, index=False)
    else:
        metric_df.to_csv(path, mode='a', header=False, index=False)

# Get the number of pods for all microservices
def collect_pod_num(config: Config, first: bool):
    instance_df = pd.DataFrame()
    prom_util = PrometheusClient(config)
    # qps_sql = 'count(container_cpu_usage_seconds_total{namespace="%s", container!~"POD|istio-proxy"}) by (container)' % (config.namespace)
    # def handle(result, instance_df):
    #     if 'container' in result['metric']:
    #         name = result['metric']['container'] + '&count'
    #         values = result['values']
    #         values = list(zip(*values))
    #         if 'timestamp' not in instance_df:
    #             timestamp = values[0]
    #             instance_df['timestamp'] = timestamp
    #             instance_df['timestamp'] = instance_df['timestamp'].astype('datetime64[s]')
    #         metric = values[1]
    #         instance_df[name] = pd.Series(metric)
    #         instance_df[name] = instance_df[name].astype('float64')
    qps_sql = 'count(kube_pod_info{namespace="%s"}) by (created_by_name)' % config.namespace
    response = prom_util.execute_prom(config.prom_range_url, qps_sql)
    def handle(result, instance_df):
        if 'created_by_name' in result['metric']:
            name = result['metric']['created_by_name'].split('-')[0] + '&count'
            values = result['values']
            values = list(zip(*values))
            if 'timestamp' not in instance_df:
                timestamp = values[0]
                instance_df['timestamp'] = timestamp
                instance_df['timestamp'] = instance_df['timestamp'].astype('datetime64[s]')
            metric = values[1]
            instance_df[name] = pd.Series(metric)
            instance_df[name] = instance_df[name].astype('float64')

    [handle(result, instance_df) for result in response]
    return instance_df

# Get the number of pods for all microservices
def collect_pod_num_current(config: Config, first: bool):
    instance_df = pd.DataFrame()
    prom_util = PrometheusClient(config)
    # qps_sql = 'count(container_cpu_usage_seconds_total{namespace="%s", container!~"POD|istio-proxy"}) by (container)' % (config.namespace)
    # def handle(result, instance_df):
    #     if 'container' in result['metric']:
    #         name = result['metric']['container'] + '&count'
    #         values = result['values']
    #         values = list(zip(*values))
    #         if 'timestamp' not in instance_df:
    #             timestamp = values[0]
    #             instance_df['timestamp'] = timestamp
    #             instance_df['timestamp'] = instance_df['timestamp'].astype('datetime64[s]')
    #         metric = values[1]
    #         instance_df[name] = pd.Series(metric)
    #         instance_df[name] = instance_df[name].astype('float64')
    qps_sql = 'count(kube_pod_container_status_ready{namespace="%s"}) by (created_by_name)' % config.namespace
    response = prom_util.execute_prom(config.prom_range_url, qps_sql)
    def handle(result, instance_df):
        if 'created_by_name' in result['metric']:
            name = result['metric']['created_by_name'].split('-')[0] + '&count'
            values = result['values']
            values = list(zip(*values))
            if 'timestamp' not in instance_df:
                timestamp = values[0]
                instance_df['timestamp'] = timestamp
                instance_df['timestamp'] = instance_df['timestamp'].astype('datetime64[s]')
            metric = values[1]
            instance_df[name] = pd.Series(metric)
            instance_df[name] = instance_df[name].astype('float64')

    [handle(result, instance_df) for result in response]
    return instance_df

# get qps for microservice
def collect_svc_qps(config: Config, _dir: str, first:bool):
    qps_df = pd.DataFrame()
    prom_util = PrometheusClient(config)
    qps_sql = 'sum(rate(istio_requests_total{reporter="destination",namespace="%s"}[1m])) by (destination_workload)' % config.namespace
    response = prom_util.execute_prom(config.prom_range_url, qps_sql)

    def handle(result, qps_df):
        name = result['metric']['destination_workload']
        values = result['values']
        values = list(zip(*values))
        if 'timestamp' not in qps_df:
            timestamp = values[0]
            qps_df['timestamp'] = timestamp
            qps_df['timestamp'] = qps_df['timestamp'].astype('datetime64[s]')
        metric = values[1]
        qps_df[name] = pd.Series(metric)
        qps_df[name] = qps_df[name].astype('float64')

    [handle(result, qps_df) for result in response]
    
    path = os.path.join(_dir, 'svc_qps.csv')
    if (first):
        qps_df.to_csv(path, index=False)
    else:
        qps_df.to_csv(path, mode='a', header=False, index=False)

# Get metric for microservices
def collect_svc_metric(config: Config, _dir: str, first: bool):
    prom_util = PrometheusClient(config)
    final_df = prom_util.get_svc_metric_range()
    path = os.path.join(_dir,'svc_metric.csv')
    if (first):
        final_df.to_csv(path, index=False)
    else:
        final_df.to_csv(path, mode='a', header=False, index=False)

# Get the success rate for microservices
def collect_succeess_rate(config: Config, first:bool):
    success_df = pd.DataFrame()
    prom_util = PrometheusClient(config)
    success_rate_sql = '(sum(rate(istio_requests_total{reporter="destination", response_code!~"5.*",namespace="%s"}[1m])) by (destination_workload, destination_workload_namespace) / sum(rate(istio_requests_total{reporter="destination",namespace="%s"}[1m])) by (destination_workload, destination_workload_namespace))' % (
    config.namespace, config.namespace)
    response = prom_util.execute_prom(config.prom_range_url, success_rate_sql)

    def handle(result, success_df):
        name = result['metric']['destination_workload']
        values = result['values']
        values = list(zip(*values))
        if 'timestamp' not in success_df:
            timestamp = values[0]
            success_df['timestamp'] = timestamp
            success_df['timestamp'] = success_df['timestamp'].astype('datetime64[s]')
        metric = values[1]
        success_df[name] = pd.Series(metric)
        success_df[name] = success_df[name].astype('float64')

    [handle(result, success_df) for result in response]
    return success_df


def collect(config: Config):
    print('collect metrics')

    interval=config.step
    first_run=True
    iterations=37000

    for i in range(iterations):
        current=pd.DataFrame()
        responsetime=collect_call_latency(config, first_run)[0]
        currentPods=collect_pod_num(config, first_run)
        success=collect_succeess_rate(config, first_run)
        successRate= (success['frontend'][0])

        #current['adservice']=currentPods['adservice&count']
        #current['cartservice']=currentPods['cartservice&count']
        #current['checkoutservice']=currentPods['checkoutservice&count']
        #current['currencyservice']=currentPods['currencyservice&count']
        #current['emailservice']=currentPods['emailservice&count']
        #current['chatbot']=currentPods['chatbot&count']
        #current['paymentservice']=currentPods['paymentservice&count']
        current['productcatalogservice']=currentPods['productcatalogservice&count']
        #current['recommendationservice']=currentPods['recommendationservice&count']
        #current['redis-cart']=currentPods['redis&count']
        #current['shippingservice']=currentPods['shippingservice&count']
        current['frontend']=currentPods['frontend&count']
        for index, row in current.items():
            deploymentName=index
            currentNum=int(row[0])
            inOrDe=random.randint(-1,2)
            print('p90:'+str(responsetime))
            if (responsetime>500) or (successRate<0.95):
                num=min(currentNum+inOrDe, 10)
                if (num<1):
                    num=1
                os.system("kubectl scale deployment --replicas="+str(num)+" "+deploymentName)
                print("scale to: "+str(num))
                
            else:
                
                num=max(1,currentNum-inOrDe)
                if (num>10):
                    num=10
                
                os.system("kubectl scale deployment --replicas="+str(num)+" "+deploymentName)
                print("scale to: "+str(num))
                    

        first_run=False
       # time.sleep(120)
        time.sleep(180)



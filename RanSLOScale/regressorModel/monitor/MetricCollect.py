from codecs import lookup_error
import os
import time
from config.Config import Config
import pandas as pd
from util.PrometheusClient import PrometheusClient

def collect_call_latency_90(config: Config, _dir: str, first:bool):
    call_df = pd.DataFrame()
    newdf=pd.DataFrame()

    prom_util = PrometheusClient(config)
    # P50，P90，P99
    prom_90_sql = 'histogram_quantile(0.90, sum(irate(istio_request_duration_milliseconds_bucket{reporter=\"destination\", destination_workload_namespace=\"%s\"}[1m])) by (destination_workload, destination_workload_namespace, source_workload, le))' % config.namespace
    responses_90 = prom_util.execute_prom(config.prom_range_url, prom_90_sql)
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

    [handle(result, call_df, 'p90') for result in responses_90]

    newdf['p90']=call_df['unknown_frontend&p90']
    #print(call_df['unknown_chatbot&p90'])

    path = os.path.join(_dir, 'callp90.csv')
    if (first):
        newdf.to_csv(path, index=False)
    else:
        newdf.to_csv(path, mode='a', header=False, index=False)


# Get the response time of the invocation edges
def collect_call_latency(config: Config, _dir: str, first:bool):
    call_df = pd.DataFrame()

    prom_util = PrometheusClient(config)
    # P50，P90，P99
    prom_50_sql = 'histogram_quantile(0.50, sum(irate(istio_request_duration_milliseconds_bucket{reporter=\"destination\", destination_workload_namespace=\"%s\"}[1m])) by (destination_workload, destination_workload_namespace, source_workload, le))' % config.namespace
    prom_90_sql = 'histogram_quantile(0.90, sum(irate(istio_request_duration_milliseconds_bucket{reporter=\"destination\", destination_workload_namespace=\"%s\"}[1m])) by (destination_workload, destination_workload_namespace, source_workload, le))' % config.namespace
    prom_99_sql = 'histogram_quantile(0.99, sum(irate(istio_request_duration_milliseconds_bucket{reporter=\"destination\", destination_workload_namespace=\"%s\"}[1m])) by (destination_workload, destination_workload_namespace, source_workload, le))' % config.namespace
    responses_50 = prom_util.execute_prom(config.prom_range_url, prom_50_sql)
    responses_90 = prom_util.execute_prom(config.prom_range_url, prom_90_sql)
    responses_99 = prom_util.execute_prom(config.prom_range_url, prom_99_sql)
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

    [handle(result, call_df, 'p50') for result in responses_50]
    [handle(result, call_df, 'p90') for result in responses_90]
    [handle(result, call_df, 'p99') for result in responses_99]

    print(call_df['unknown_frontend&p90'])
    #print(call_df['unknown_chatbot&p90'])

    path = os.path.join(_dir, 'call.csv')
    if (first):
        call_df.to_csv(path, index=False)
    else:
        call_df.to_csv(path, mode='a', header=False, index=False)


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
def collect_pod_num(config: Config, _dir: str, first: bool):
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

    path = os.path.join(_dir, 'instances.csv')
    if (first):
        instance_df.to_csv(path, index=False)
    else:
        instance_df.to_csv(path, mode='a', header=False, index=False)

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
# Get the number of pods for all microservices
# Get the number of pods for all microservices
def collect_pod_num_current(config: Config, _dir: str, first: bool):
    instance_df = pd.DataFrame()
    prom_util = PrometheusClient(config)
    '''
    name0='chatbot&count&current'
    qps_sql0 = 'kube_deployment_status_replicas_available{namespace="jia", deployment="chatbot"}'
    response0 = prom_util.execute_prom_current(config.prom_range_url, qps_sql0)
    instance_df[name0]=pd.Series(response0[1]).astype('float64')
    '''

    
    name1='adservice&count&current'
    qps_sql1 = 'kube_deployment_status_replicas_available{namespace="boutique", deployment="adservice"}'
    response1 = prom_util.execute_prom_current(config.prom_range_url, qps_sql1)
    instance_df[name1]=pd.Series(response1[1]).astype('float64')

    if 'timestamp' not in instance_df:
        timestamp=response1[0]
        instance_df['timestamp']=timestamp
        instance_df['timestamp']=instance_df['timestamp'].astype('datetime64[s]')

    name2='cartservice&count&current'
    qps_sql2 = 'kube_deployment_status_replicas_available{namespace="boutique", deployment="cartservice"}'
    response2 = prom_util.execute_prom_current(config.prom_range_url, qps_sql2)
    instance_df[name2]=pd.Series(response2[1]).astype('float64')

    name3='checkoutservice&count&current'
    qps_sql3 = 'kube_deployment_status_replicas_available{namespace="boutique", deployment="checkoutservice"}'
    response3 = prom_util.execute_prom_current(config.prom_range_url, qps_sql3)
    instance_df[name3]=pd.Series(response3[1]).astype('float64')

    name4='currencyservice&count&current'
    qps_sql4 = 'kube_deployment_status_replicas_available{namespace="boutique", deployment="currencyservice"}'
    response4 = prom_util.execute_prom_current(config.prom_range_url, qps_sql4)
    instance_df[name4]=pd.Series(response4[1]).astype('float64')

    name5='emailservice&count&current'
    qps_sql5 = 'kube_deployment_status_replicas_available{namespace="boutique", deployment="emailservice"}'
    response5 = prom_util.execute_prom_current(config.prom_range_url, qps_sql5)
    instance_df[name5]=pd.Series(response5[1]).astype('float64')

    name6='frontend&count&current'
    qps_sql6 = 'kube_deployment_status_replicas_available{namespace="boutique", deployment="frontend"}'
    response6 = prom_util.execute_prom_current(config.prom_range_url, qps_sql6)
    instance_df[name6]=pd.Series(response6[1]).astype('float64')

    name7='paymentservice&count&current'
    qps_sql7 = 'kube_deployment_status_replicas_available{namespace="boutique", deployment="paymentservice"}'
    response7 = prom_util.execute_prom_current(config.prom_range_url, qps_sql7)
    instance_df[name7]=pd.Series(response7[1]).astype('float64')

    name8='productcatalogservice&count&current'
    qps_sql8 = 'kube_deployment_status_replicas_available{namespace="boutique", deployment="productcatalogservice"}'
    response8 = prom_util.execute_prom_current(config.prom_range_url, qps_sql8)
    instance_df[name8]=pd.Series(response8[1]).astype('float64')

    name9='recommendationservice&count&current'
    qps_sql9 = 'kube_deployment_status_replicas_available{namespace="boutique", deployment="recommendationservice"}'
    response9 = prom_util.execute_prom_current(config.prom_range_url, qps_sql9)
    instance_df[name9]=pd.Series(response9[1]).astype('float64')

    name10='redis-cart&count&current'
    qps_sql10 = 'kube_deployment_status_replicas_available{namespace="boutique", deployment="redis-cart"}'
    response10 = prom_util.execute_prom_current(config.prom_range_url, qps_sql10)
    instance_df[name10]=pd.Series(response10[1]).astype('float64')

    name11='shippingservice&count&current'
    qps_sql11 = 'kube_deployment_status_replicas_available{namespace="boutique", deployment="shippingservice"}'
    response11 = prom_util.execute_prom_current(config.prom_range_url, qps_sql11)
    instance_df[name11]=pd.Series(response11[1]).astype('float64')
    

    path = os.path.join(_dir, 'instances_current.csv')
    if (first):
        instance_df.to_csv(path, index=False)
    else:
        instance_df.to_csv(path, mode='a', header=False, index=False)

# Get the success rate for microservices
def collect_succeess_rate(config: Config, _dir: str, first:bool):
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

    path = os.path.join(_dir, 'success_rate.csv')
    if (first):
        success_df.to_csv(path, index=False)
    else:
        success_df.to_csv(path, mode='a', header=False, index=False)

def collect(config: Config, _dir: str):
    print('collect metrics')
    if not os.path.exists(_dir):
        os.mkdir(_dir)
    interval=60
    first_run=True
    iterations=37000
    for i in range(iterations):
        collect_call_latency(config, _dir, first_run)
        collect_call_latency_90(config, _dir, first_run)
        collect_svc_latency(config, _dir, first_run)
        collect_resource_metric(config, _dir, first_run)
        collect_succeess_rate(config, _dir, first_run)
        collect_svc_qps(config, _dir, first_run)
        collect_svc_metric(config, _dir, first_run)
        collect_pod_num(config, _dir, first_run)
        collect_pod_num_current(config, _dir, first_run)
        first_run=False
        time.sleep(interval)



import math
import time
import numpy as np
import schedule
from copy import deepcopy
import networkx as nx
import scipy.stats
import joblib
from config.Config import Config
import warnings
from util.KubernetesClient import KubernetesClient
from util.PrometheusClient import PrometheusClient
import random
import os
import threading
import csv

warnings.filterwarnings("ignore")
checkList=[0,0,0,0]
checkList120=[0,0,0,0,0,0,0,0]

def run_threaded(job_func):
    job_thread=threading.Thread(target=job_func)
    job_thread.start()


def coast_time(func):
    def fun(*args, **kwargs):
        t = time.perf_counter()
        result = func(*args, **kwargs)
        print(f'func {func.__name__} coast time:{time.perf_counter() - t:.8f} s')
        return result
    return fun

# anomaly detection window -- 15 seconds
# waste detection window -- 120 seconds
#WASTE_CHECK_INTERVAL = 120
config=Config()
mae=0.0084


def collect_pod_num_current(config: Config):
    instance_df = pd.DataFrame()
    prom_util = PrometheusClient(config)

    name0='chatbot'
    qps_sql0 = 'kube_deployment_status_replicas_available{namespace="jia", deployment="chatbot"}'
    response0 = prom_util.execute_prom_current(config.prom_range_url, qps_sql0)
    instance_df[name0]=pd.Series(response0[1]).astype('float64')

    return float(instance_df['chatbot'][0])

def collect_svc_qps(config: Config):
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
    return qps_df['chatbot'][0]

def get_svc(config: Config):
    prom_util = PrometheusClient(config)
    final_df = prom_util.get_svc_metric()
    return final_df

def get_success(config: Config):
    prom_util = PrometheusClient(config)
    success_rate_sql = '(sum(rate(istio_requests_total{reporter="destination", response_code!~"5.*",namespace="%s"}[1m])) by (destination_workload, destination_workload_namespace) / sum(rate(istio_requests_total{reporter="destination",namespace="%s"}[1m])) by (destination_workload, destination_workload_namespace))' % (
    config.namespace, config.namespace)
    response = prom_util.execute_prom(config.prom_range_url, success_rate_sql)
    values=response[0]['values']
    values=list(zip(*values))
    return float(values[1][0])



def fitness(svc_df,sum):
    model=joblib.load('/home/jia/PBScaler/simulation/chatbot_model_RandomForestRegressor.model')
    svc_df=np.array(svc_df).reshape(1, -1)
    predictResponse= (model.predict(svc_df)[0])
    predictResponse=predictResponse
    #count= 'count(kube_pod_container_status_ready{namespace="%s"})' % Config().namespace
    #response = PrometheusClient(Config()).execute_prom_current(config.prom_range_url, count)
    #podCounts=pd.Series(response[1]).astype('float64')  
    if (predictResponse<0.95+mae):
        return (1-predictResponse)/(1-0.95)
    else:
        return sum/3
        #return podCounts[0]


class selfAdapt:
    def __init__(self, config: Config):
        # the prometheus client && k8s client
        self.config = config
        self.prom_util = PrometheusClient(config)
        self.k8s_util = KubernetesClient(config)
        # simulation environment
        #self.predictor = joblib.load(simulation_model_path)
        # args
        self.SLO = config.SLO
        self.max_num = config.max_pod
        self.min_num = config.min_pod
        # microservices
        #self.mss = self.k8s_util.get_svcs()
        self.population=config.population
        

    @coast_time
    def anomaly_detect(self):
        
        """ SLO check
        """
        sucRate=get_success(self.config)
        print ("success rate: "+str(sucRate))
        global checkList
        upFlag=sum(checkList)
        count= 'count(kube_pod_container_status_ready{namespace="%s"})' % Config().namespace
        response = PrometheusClient(Config()).execute_prom_current(config.prom_range_url, count)
        podCounts=pd.Series(response[1]).astype('float64') 
        if (sucRate < self.SLO+mae) or (upFlag!=0):
        #if True:
            if (int(podCounts[0])<6):
                result, fitness=self.changeUp()
                predict=self.predict(result)
                print('UP   result'+str(result)+', fitness'+str(fitness)+', predict: '+str(predict))
                os.system("kubectl scale deployment --replicas="+str(result)+" chatbot")

        global checkList120
        downFlag=sum(checkList120)
        if sucRate == 1 and (int(podCounts[0])>2) and (downFlag==0) :
        #if True:
            result,fitness=self.changeDown()
            predict=self.predict(result)
            if (predict>self.SLO+mae):
                print('Down   result'+str(result)+', fitness'+str(fitness)+', predict'+str(predict))
                os.system("kubectl scale deployment --replicas="+str(result)+" chatbot")



    def predict(self, inputValue):
        currentNums=int(inputValue)
        qps=float(collect_svc_qps(self.config))

        metrics=get_svc(self.config)
        cpu=float(metrics['chatbot&cpu_usage'][0])/currentNums
        mem=float(metrics['chatbot&mem_usage'][0])/currentNums
        input=[]
        input.append(qps)
        input.append(currentNums)
        input.append(cpu)
        input.append(mem)
        model=joblib.load('/home/jia/PBScaler/simulation/chatbot_model_RandomForestRegressor.model')
        svc_df=np.array(input).reshape(1, -1)
        predictResponse= (model.predict(svc_df)[0])
        return predictResponse
    

    
    def changeUp(self):
        currentPods=collect_pod_num_current(self.config)
        currentNums=int(currentPods)
        upNums=self.config.max_pod-currentNums

        qps=float(collect_svc_qps(self.config))

        metrics=get_svc(self.config)
        cpu=float(metrics['chatbot&cpu_usage'][0])/currentNums
        mem=float(metrics['chatbot&mem_usage'][0])/currentNums
        result=currentNums
        input=[]
        input.append(qps)
        input.append(currentNums)
        input.append(cpu)
        input.append(mem)
        fitnessValue=fitness(input,currentNums)

        population=self.config.population

        for i in range (0, population-1):
            tempInput=[]
            tempResult=0
            tempSum=0

            tempInput.append(qps)
            randomNumber=int(random.randint(1,upNums)+currentNums)
            tempSum=randomNumber
            tempInput.append(randomNumber)
            tempInput.append(cpu)
            tempInput.append(mem)
            tempResult=randomNumber

            tempFitness=fitness(tempInput, tempSum)

            if (fitnessValue>tempFitness):
                fitnessValue=tempFitness
                result=tempResult

            if (result==currentNums):
                result=result+1
                fitnessValue=fitnessValue+1

        return result, fitnessValue
    
    def changeDown(self):
        currentPods=collect_pod_num_current(self.config)

        currentNums=int(currentPods)
        downNums=currentNums
        population=self.population

        qps=float(collect_svc_qps(config))
        metrics=get_svc(self.config)
        cpu=float(metrics['chatbot&cpu_usage'][0])/currentNums
        mem=float(metrics['chatbot&mem_usage'][0])/currentNums
        result=currentNums-1
        input=[]
        input.append(qps)
        input.append(currentNums-1)
        input.append(cpu)
        input.append(mem)
        fitnessValue=fitness(input,currentNums-1)

        for i in range (0, population-1):
            tempInput=[]
            tempResult=0
            tempSum=0

            tempInput.append(qps)
            randomNumber=currentNums-random.randint(1,downNums-1)
            tempSum=randomNumber
            tempInput.append(randomNumber)
            tempInput.append(cpu)
            tempInput.append(mem)
            tempResult=randomNumber

            tempFitness=fitness(tempInput, tempSum)
            #print(tempResult)
            #print(tempFitness)
            if (fitnessValue>tempFitness):
                fitnessValue=tempFitness
                result=tempResult
                
        return result, fitnessValue
                    


    def start(self):
        print("PBScaler is running...")
        schedule.clear()
        schedule.every(60).seconds.do(run_threaded,self.anomaly_detect)
        schedule.every(15).seconds.do(run_threaded,self.check)
        while True:
            schedule.run_pending()
            time.sleep(1)


    def check(self):
        global checkList

        i=3
        while(i>0):
            checkList[i]=checkList[i-1]
            i=i-1

        global checkList120
        i=7
        while(i>0):
            checkList120[i]=checkList120[i-1]
            i=i-1

        sucRate=get_success(self.config)

        if (sucRate<self.SLO+mae):
            checkList[0]=1
            checkList120[0]=1
        else:
            checkList[0]=0
            checkList120[0]=0
        print (str(sum(checkList))+", "+str(sum(checkList120)))
        


if __name__ == '__main__':
     config = Config()

     scaler = selfAdapt(config=config)
     scaler.start()
     config.end = int(round(time.time()))
     config.start = config.end - config.duration
     

import time
import numpy as np
import schedule
from copy import deepcopy
import networkx as nx
import joblib
from config.Config import Config
import warnings
from util.KubernetesClient import KubernetesClient
from util.PrometheusClient import PrometheusClient
import random
import os
import threading
import pandas as pd

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

config=Config()
mae=127

def collect_pod_num_set(config: Config):
    instance_df = pd.DataFrame()
    prom_util = PrometheusClient(config)

    name1='frontend'
    qps_sql1 = 'count(kube_pod_container_status_ready{namespace="%s", pod=~"frontend.+"})' % config.namespace
    response1 = prom_util.execute_prom_current(config.prom_range_url, qps_sql1)
    instance_df[name1]=pd.Series(response1[1]).astype('float64')

    name2='productcatalogservice'
    qps_sql2 = 'count(kube_pod_container_status_ready{namespace="%s", pod=~"productcatalogservice.+"})' % config.namespace
    response2 = prom_util.execute_prom_current(config.prom_range_url, qps_sql2)
    instance_df[name2]=pd.Series(response2[1]).astype('float64')

    return instance_df

def collect_pod_num_current(config: Config):
    instance_df = pd.DataFrame()
    prom_util = PrometheusClient(config)

    name1='frontend'
    qps_sql1 = 'kube_deployment_status_replicas_available{namespace="%s", deployment=~"frontend"}' % config.namespace
    response1 = prom_util.execute_prom_current(config.prom_range_url, qps_sql1)
    instance_df[name1]=pd.Series(response1[1]).astype('float64')

    name2='productcatalogservice'
    qps_sql2 = 'kube_deployment_status_replicas_available{namespace="%s", deployment=~"productcatalogservice"}' % config.namespace
    response2 = prom_util.execute_prom_current(config.prom_range_url, qps_sql2)
    instance_df[name2]=pd.Series(response2[1]).astype('float64')

    return instance_df

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
    return qps_df


def fitness(svc_df,sum):
    model=joblib.load('/home/jia/PBScaler/simulation/model_RandomForestRegressor_bottleneck.model')
    svc_df=np.array(svc_df).reshape(1, -1)
    predictResponse= (model.predict(svc_df)[0])

    if (predictResponse>(500-mae)):
        return predictResponse/(500-mae)
    else:
        return sum/(10+10)


class selfAdapt:
    def __init__(self, config: Config):
        # the prometheus client && k8s client
        self.config = config
        self.prom_util = PrometheusClient(config)
        self.k8s_util = KubernetesClient(config)

        self.SLO = 500
        self.max_num = 10
        self.min_num = 1

        self.svcs=['cartservice', 'checkoutservice', 'currencyservice', 'emailservice', 'frontend', 'paymentservice', 
                     'productcatalogservice', 'recommendationservice', 'shippingservice', 'adservice', 'redis-cart']
        self.qpssvcs=['cartservice', 'checkoutservice', 'currencyservice', 'emailservice', 'frontend', 'paymentservice', 
                     'productcatalogservice', 'recommendationservice', 'shippingservice']
        self.svcsLeft=['adservice', 'redis-cart']
        self.svcBN=['frontend','productcatalogservice']

        self.population=config.population
        
    def collectQPS(self):
        qpsResult=collect_svc_qps(config)
        qps=[]
        svcs=self.qpssvcs
        for i in range(len(svcs)):
            qpsValue=qpsResult[svcs[i]][0]
            qps.append(qpsValue)
        return qps
    
    def predict(self, inputValue):
        input=[]
        qpsResult=collect_svc_qps(config)
        for i in range(len(self.qpssvcs)):
            qpsValue=qpsResult[self.qpssvcs[i]][0]
            input.append(qpsValue)
        for i in range(len(self.svcBN)):
            input.append(int(inputValue[i]))

        model=joblib.load('/home/jia/PBScaler/simulation/model_RandomForestRegressor_bottleneck.model')
        svc_df=np.array(input).reshape(1, -1)
        predictResponse= (model.predict(svc_df)[0])
        return predictResponse

    @coast_time
    def anomaly_detect(self):

        p90=self.get_p90()
        print ('p90:'+str(p90))

        global checkList
        upFlag=sum(checkList)
        podsNums=collect_pod_num_set(self.config)

        podsSum=0
        for i in self.svcBN:
            podsSum=podsSum+int(podsNums[str(i)][0])
        podsSum=int(podsSum/2)

        if(p90>self.SLO+mae) or (upFlag!=0):
        #if (True):
            if (podsSum<20):
                result, fitness=self.changeUp()
                predict=self.predict(result)
                print('UP   result',result, ', fitness',str(fitness),', predict: ', predict)
                for i in range(len(result)):
                    os.system("kubectl scale deployment --replicas="+str(result[i])+" "+self.svcBN[i])

        global checkList120
        downFlag=sum(checkList120)
        if p90<(self.SLO-mae)*0.8 and (podsSum>2) and (downFlag==0):
            result,fitness=self.changeDown()
            predict=self.predict(result)
            if (predict<self.SLO-mae):
                print('Down   result',result,', fitness',str(fitness),', predict:', predict)
                for i in range(len(result)):
                    os.system("kubectl scale deployment --replicas="+str(result[i])+" "+self.svcBN[i])
            
    def changeUp(self):
        currentPods=collect_pod_num_current(self.config)
        
        result=[]
        input=[]
        svcs=self.qpssvcs
        svcsBN=self.svcBN

        currentNums=[]

        for i in svcsBN:
            if (str(i)=='redis-cart'):
                i='redis'
            currentPodNum=int(currentPods[str(i)][0])
            currentNums.append(currentPodNum)

        population=self.population

        qpsResult=collect_svc_qps(config)

        for i in range(len(svcs)):
            qpsValue=qpsResult[svcs[i]][0]
            input.append(qpsValue)

        for i in range(len(svcsBN)):
            currentNum=int(currentNums[i])
            input.append(currentNum)
            result.append(currentNum)
       
        fitnessValue=fitness(input, sum(currentNums))

        for i in range (0, population-1):
            tempInput=[]
            tempResult=[]
            tempSum=0

            for i in svcs:
                qpsValue=qpsResult[i][0]
                tempInput.append(qpsValue)

            for i in range(len(svcsBN)):
                if (10-currentNums[i]>1):
                    randomNumber=int(random.randint(1,10-currentNums[i])+currentNums[i])
                else:
                    randomNumber=int(1+currentNums[i])
                if (randomNumber>10):
                    randomNumber=10
                tempSum=tempSum+randomNumber
                tempInput.append(randomNumber)
                tempResult.append(randomNumber)
            
            tempFitness=fitness(tempInput, tempSum)

            if (fitnessValue>tempFitness):
                fitnessValue=tempFitness
                result=tempResult

        return result, fitnessValue
    
    def changeDown(self):
        currentPods=collect_pod_num_current(self.config)

        downNums=[]
        currentNums=[]

        result=[]
        input=[]
        svcs=self.qpssvcs
        svcsBN=self.svcBN

        for i in svcsBN:
            if (str(i)=='redis-cart'):
                i='redis'
            currentPodNum=int(currentPods[str(i)][0])
            currentNums.append(currentPodNum)
            downNums.append(currentPodNum)

        population=self.population

        qpsResult=collect_svc_qps(config)

        for i in range(len(svcs)):
            qpsValue=qpsResult[svcs[i]][0]
            input.append(qpsValue)
        for i in range(len(svcsBN)):
            currentNum=int(currentNums[i])
            input.append(currentNum)
            result.append(currentNum)
        fitnessValue=fitness(input, sum(currentNums))

        for i in range (0, population-1):
            tempInput=[]
            tempResult=[]
            tempSum=0

            for i in svcs:
                tempInput.append(qpsResult[i][0])
            for i in range(len(svcsBN)):
                if (downNums[i]-1>1):
                    randomNumber=currentNums[i]-random.randint(1,downNums[i]-1)
                else:
                    randomNumber=currentNums[i]-1
                if(randomNumber<1):
                    randomNumber=1
                tempSum=tempSum+randomNumber
                tempInput.append(randomNumber)
                tempResult.append(randomNumber)
            tempFitness=fitness(tempInput, tempSum)
            #print(tempResult)
            #print(tempFitness)
            if (fitnessValue>tempFitness):
                fitnessValue=tempFitness
                result=tempResult
        return result, fitnessValue
                    

    def get_p90(self):
        # slo Hypothesis testing
        call_latency = self.prom_util.get_call_latency()
        return call_latency['unknown_frontend']


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

        p90=self.get_p90()

        if (p90>self.SLO-mae):
            checkList[0]=1
            checkList120[0]=1
        else:
            checkList[0]=0
            checkList120[0]=0
        print (str(sum(checkList))+", "+str(sum(checkList120)))
        


    def start(self):
        print("PBScaler is running...")
        schedule.clear()
        schedule.every(60).seconds.do(run_threaded,self.anomaly_detect)
        schedule.every(15).seconds.do(run_threaded,self.check)
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == '__main__':
     config = Config()
     scaler = selfAdapt(config=config)
     scaler.start()
     config.end = int(round(time.time()))
     config.start = config.end - config.duration

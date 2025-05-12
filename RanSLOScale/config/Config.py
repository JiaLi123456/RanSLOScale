import time

def getNowTime():
    return int(round(time.time()))

class Config():
    def __init__(self):
        self.namespace = 'boutique'
        # self.nodes = {
        #     'ubuntu-Precision-Tower-7810': '192.168.31.202:9100',
        #     'dell': '192.168.31.201:9100',
        #     'node1': '192.168.31.136:9100'
        # }

        self.SLO = 500
        # maximum and minimum number of pods for a microservice
        self.max_pod = 10
        self.min_pod = 1
        self.population = 100

        # k8s config path
        self.k8s_config = '/home/jia/.kube/config'
        self.k8s_yaml = '/home/ubuntu/xsy/microservices-demo/release/kubernetes-manifests.yaml'
        
        # set the duration of experiment
        self.duration = 0  # 20 min
        self.start = getNowTime()
        self.end = self.start + self.duration

        # prometheus
        self.prom_range_url = "http://10.40.33.41:31200/api/v1/query_range"
        self.prom_no_range_url = "http://10.40.33.41:31200/api/v1/query"
        self.step = 5

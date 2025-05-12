import requests
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from threading import Thread
import csv
import datetime
from datetime import datetime
from faker import Faker
fake=Faker()

# URL of the API endpoint or website that handles questions
URL = "http://10.106.185.78"
current_year = datetime.now().year+1

def action1():
    return requests.get(URL)
def action2():
    return requests.post(URL+"/setCurrency", {"currency_code":"CAD"})
def action3():
    return requests.get(URL+"/cart")
def action4():
    return requests.post(URL+"/cart", {"product_id":"OLJCESPC7Z", "quantity":random.randint(1,10)})
def action5():
    action4()
    return requests.post(URL+"/cart/checkout", {"email":fake.email(),"street_address": fake.street_address(), "zip_code": fake.zipcode(), "city": fake.city(),"state": fake.state_abbr(),"country": fake.country(),"credit_card_number": fake.credit_card_number(card_type="visa"),"credit_card_expiration_month": random.randint(1, 12),"credit_card_expiration_year": random.randint(current_year, current_year + 70),"credit_card_cvv": f"{random.randint(100, 999)}",})
def action6():
    return requests.get(URL+"/product/"+"OLJCESPC7Z")
action_weights={
    'action1()':1,
    'action2()':2,
    'action3()':3,
    'action4()':2,
    'action5()':1,
    'action6()':10
}
def weighted_actions(weights):
    total_weight=sum(weights.values())
    probabilities={k: v / total_weight for k, v in weights.items()}
    action=random.choices(list(probabilities.keys()), weights=list(probabilities.values()))[0]
    return action


# Function to send a question and receive an answer
def send_requests():
    try:
        response = eval(weighted_actions(action_weights))

    except requests.exceptions.RequestException as e:
        return 0

# Function to simulate multiple questions being asked in parallel, with RPS control


if __name__ == "__main__":

    #total_duration = int(input("Enter the total duration of the test (in seconds): "))
    interval=20
    minInterval=2
    maxInterval=80


    currentTime=time.time()

    changeTime=currentTime
    count=0 

    while(count<70):
    #while (time.time()<currentTime+total_duration):
        thread=threading.Thread(target=send_requests)
        thread.start()
        realInterval=float(1/interval)
        time.sleep(realInterval)
        if (time.time()-changeTime>10*60):
            count=count+1
            changeTime=time.time()
            interval=random.randint(minInterval,maxInterval)
            print (interval)
        
    time.sleep(10)

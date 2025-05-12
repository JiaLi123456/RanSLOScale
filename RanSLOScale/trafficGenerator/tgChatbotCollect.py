import requests
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from threading import Thread
import csv
import datetime
from datetime import datetime

# URL of the API endpoint or website that handles questions
URL = "http://localhost:32747"

# Example questions to be sent
questions = [
    #{"question": "What is Cheetah Networks?"},
    #{"question": "Who are you?"},
    {"question": "Hello."},
    # Add more sample questions here
]

# Function to send a question and receive an answer
def send_question(filename):
    filename=filename
    question=random.choice(questions)
    content=[]
    currentTime=time.time()
    content.append(currentTime)
    content.append(datetime.fromtimestamp(currentTime))
    content.append(question)
    try:
        start_time = time.time()
        response = requests.post(URL, data=question)
        response_time = time.time() - start_time
        # Check if the response is successful
        if response.status_code == 200:
            answer = response.json()  # Assuming the response is in JSON format
            content.append(answer['result'])
            content.append(response_time)
            #print(answer)
            
        else:
            content.append('0')
            content.append(response_time)
        out = csv.writer(open(filename,"a"), delimiter=',',quoting=csv.QUOTE_ALL)
        out.writerow(content)
    except requests.exceptions.RequestException as e:
        print("question"+question["question"]+ " error"+str(e))

# Function to simulate multiple questions being asked in parallel, with RPS control

# Perform load test
if __name__ == "__main__":

    #total_duration = int(input("Enter the total duration of the test (in seconds): "))
    interval=8
    minInterval=5
    maxInterval=10


    currentTime=time.time()

    changeTime=currentTime
    count=0 
    filename="abc.csv"
    while(count<70):
    #while (time.time()<currentTime+total_duration):
        thread=threading.Thread(target=send_question, args=(filename,))
        thread.start()
        time.sleep(interval)
        if (time.time()-changeTime>10*60):
            count=count+1
            changeTime=time.time()
            interval=random.randint(minInterval,maxInterval)
            print (interval)
        
    time.sleep(10)

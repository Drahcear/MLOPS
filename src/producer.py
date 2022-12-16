from datetime import datetime
import json
from kafka import KafkaProducer
import pandas as pd
from sender import Card, Sender
import uuid
import glob

def createUID():
    return uuid.uuid1()
    

files = glob.glob("../DB/*.csv")
df = pd.DataFrame() #since it is just simulation of creating info pandas is fine 7000 lines of info only
for f in files:
    csv = pd.read_csv(f)
    df = pd.concat([csv, df], ignore_index=True)
df = df.astype(str)

machine_id = createUID()
for i in range (100):
    row = df.sample().values.flatten().tolist()
    sender = Sender(machine_id, "bank", "localhost", Card(row[0], row[1], row[2], row[3]))
    sender.send()

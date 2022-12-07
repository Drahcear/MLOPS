import streamlit as st
from sender import Card, Sender
from kafka import KafkaProducer
from kafka import KafkaConsumer
import uuid
import json
import time

def createUID():
    return uuid.uuid1()


with st.form("my_form"):
   name = st.text_input("Fullname")
   number = st.text_input("Card Number")   
   CVC = st.text_input("Security code")
   expire = st.text_input("Expiry date")    

   # Every form must have a submit button.
   submitted = st.form_submit_button("Submit")
   if submitted:
       sender = Sender(createUID(), "t3", "localhost:9092", Card(name, number, CVC, expire, "France"))
       consumer = KafkaConsumer('t4', bootstrap_servers=['localhost:9092'])
       sender.send()       
       for msg in consumer:            
            val = json.loads(msg.value)
            st.write(val)
            break
       
 
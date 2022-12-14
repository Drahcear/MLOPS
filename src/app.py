import streamlit as st
from utils import *
from datetime import date
import plotly.express as px
from sender import Card, Sender
from kafka import KafkaProducer
from kafka import KafkaConsumer
import uuid
import json
import time
from datetime import datetime, timedelta

def createUID():
    return uuid.uuid1()

#sender = Sender(createUID(), "t3", "localhost:9092", Card("", "", "", "", "France"))
#consumer = KafkaConsumer('t4', bootstrap_servers=['localhost:9092'])
       
f = open('../Analytics.txt')
data = json.load(f)
f.close()


def main_page():
    st.title("Analytics")

def total_transaction():
    start = st.date_input(
    "Start date",
    date(2022,12,13))

    end = st.date_input(
    "End date",
    date(2022,12,13))
    if start > end:
        end = start
            

    delta = end - start   # returns timedelta
    
    total_count, true_count, false_count = 0, 0, 0
    for i in range(delta.days + 1):
        day = start + timedelta(days=i)
        if not str(day) in data:
            continue
        dico = data[str(day)]
        total_count += dico["count"]    
    

    
    st.write('Total transaction: ', total_count)


def transaction_day():
    start = st.date_input(
    "Start date",
    date(2022,12,13))

    end = st.date_input(
    "End date",
    date(2022,12,13))
    if start > end:
        end = start
    dates = []
    delta = end - start   # returns timedelta
    total_count, true_count, false_count = 0, [], []
    for i in range(delta.days + 1):        
        day = start + timedelta(days=i)
        if not str(day) in data:
            continue
        dates.append(day)
        dico = data[str(day)]
        total_count += dico["count"]
        true_count.append( dico["true"])
        false_count.append(dico["false"])
    
    fig = px.bar(x=dates, y=[true_count, false_count], barmode="group", labels={'x':'Date', 'value':'Count'}, title="Transaction by day")
    newnames = {"wide_variable_0":"True", "wide_variable_1":"False"}
    fig.for_each_trace(lambda t: t.update(name = newnames[t.name]))
    st.plotly_chart(fig)

    
def geo():
    start = st.date_input(
    "Start date",
    date(2022,12,13))
    agree = st.checkbox('All Time')
    if agree:
        start = ""        
    st.plotly_chart(plot_per_coutry(df, start))

def most_used_card():
    start = st.date_input(
    "Start date",
    date(2022,12,13))
    if str(start) in data:
        dico = data[str(start)]
        name = dico["name"]
        number = dico["number"]
        CVC = dico["CVC"]
        expire = dico["expire"]
        st.write(name, number, CVC, expire)

def live_test():
    with st.form("my_form"):
       name = st.text_input("Fullname")
       number = st.text_input("Card Number")   
       CVC = st.text_input("Security code")
       expire = st.text_input("Expiry date")    

       # Every form must have a submit button.
       submitted = st.form_submit_button("Submit")
       if submitted:
           card = Card(name, number, CVC, expire, "France")
           sender.card = card           
           sender.send()       
           for msg in consumer:            
                val = json.loads(msg.value)
                st.write(val)
                break
    

page_names_to_funcs = {
    "Main Page": main_page,
    "Number of Transaction": total_transaction,
    "Transaction per Day": transaction_day,
    #"Transaction per Country": geo,
    "Most used card": most_used_card,
    #"Card Test": live_test
}

selected_page = st.sidebar.selectbox("Select a page", page_names_to_funcs.keys())
page_names_to_funcs[selected_page]()

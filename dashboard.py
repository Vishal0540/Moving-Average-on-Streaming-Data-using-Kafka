import time  

import numpy as np 
import pandas as pd  
import plotly.express as px  
import streamlit as st  
import json


# streamlit run dashboard.py
st.set_page_config(
    page_title="Real-Time Risk Score Moving Avergae",
    page_icon="✅",
    layout="wide",
)

st.title("Real-Time Risk Score Moving Avergae")


with open('config.json','r') as f:
    config_data = json.load(f)

num = config_data['MA_NUMBER']
try:
    df = pd.read_csv("Database.csv")

except:
    df = pd.DataFrame()
# kpi1, kpi2, kpi3 = st.columns(3)

# fill in those three columns with respective metrics or KPIs



placeholder = st.empty()
for seconds in range(1000):

    
    try:
        df = pd.read_csv("Database.csv")
    except:
        df = pd.DataFrame()
        
    try:
        lst_ma = df.tail(1)[f'MovingAverage{num}']
    except:
        lst_ma = "N/A"
        
    with placeholder.container():

        # create three columns
        kpi1 , kpi2 = st.columns(2)

        # fill in those three columns with respective metrics or KPIs
        kpi1.metric(
        label="Number of Records",
        value=df.shape[0]
        )
        
        kpi2.metric(
            label="Current MA5",
            value=lst_ma,
        )
        try:
            chart_data = pd.DataFrame(df[f'MovingAverage{num}'])

            st.line_chart(chart_data)
        except:
            pass

        

        st.markdown("### Detailed Data View")
        st.dataframe(df)
        time.sleep(1)


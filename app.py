import streamlit as st 
import time as t
# tittle -
st.title("Welcome to my Youtube project")

# Header

st.header("Mechine Learning")

# Sub header

st.subheader(" Linear Regression")

# To give information 

st.info("Information details of a user")

# Warning message

st.warning("come on time else you will get absent")

# Error message 

st.error("Wrong password")

# success message

st.success("Congratz you get A grade")

# Write 

st.write("Student name")
st.write(range(50))

# Markdown

st.markdown("# You tube ")
st.markdown("## You tube")
st.markdown(":moon:")

# Wedget

# check box

st.checkbox('Login')

# button

st.button("Click")

# Radio wedget

st.radio("Pick your gender",["Male","Female","Other"])

# Select box

st.selectbox("Pick your course",["ML","Jave","Cyber security"])

# slider

st.slider("Enter your number", 0,100)

# number input

st.number_input("Pick a number",0,100)

# Text input 

st.text_input("Enter your email")

# date input

st.date_input("Opening ceremony")

# time input

st.time_input("What is time now")

# text area

st.text_area("Welcome to Youtube")

st.file_uploader("Upload your file")

# spinner 

with st.spinner("Just wait"):
    t.sleep(5)

# ballons

st.balloons()

# sidebar
st.sidebar.title("Youtube")
st.sidebar.text_input("Mail address")
st.sidebar.text_input("Password")
st.sidebar.button("Submit")
st.sidebar.radio("Professional expert",["Student","Working","Others"])

# Data Visualization 
import pandas as pd
import numpy as np
st.title("Bar chart")
data=pd.DataFrame(np.random.randn(50,2),columns=["x","y"])
st.bar_chart(data)
st.title("Line chart")
st.line_chart(data)
st.title("Area chart")
st.area_chart(data)

from datetime import datetime

def convert_to_mysql_datetime(iso_datetime):
    if iso_datetime:
        return datetime.strptime(iso_datetime, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
    return None

from sqlalchemy import create_engine, text

# Function to insert date
def insert_date(engine, date_str):
    mysql_datetime = convert_to_mysql_datetime(date_str)
    if mysql_datetime:
        with engine.connect() as connection:
            insert_query = text("INSERT INTO COMMENT (comment_published_date) VALUES (:datetime)")
            connection.execute(insert_query, {"datetime": mysql_datetime})
        print("Date inserted successfully")
    else:
        print("Invalid date format")


from datetime import datetime

def convert_to_mysql_datetime(iso_datetime):
    if iso_datetime:
        return datetime.strptime(iso_datetime, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
    return None

# Example usage
iso_date = "2020-08-23T20:55:00Z"
mysql_datetime = convert_to_mysql_datetime(iso_date)
print(mysql_datetime)  # Output: 2020-08-23 20:55:00

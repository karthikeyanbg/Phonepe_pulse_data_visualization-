import pandas as pd
import mysql.connector
import streamlit as st
import plotly.express as px
import os
import json
from streamlit_option_menu import option_menu
from PIL import Image
os.environ["GIT_PYTHON_REFRESH"] = "quiet"
import git
from git.repo.base import Repo
# Repo.clone_from("https://github.com/PhonePe/pulse.git/", "D:/phonepe_pulse_project/")
#Data transformation Dataframe of aggregated Transactions
path1 = "D:/phonepe_pulse_project/data/aggregated/transaction/country/india/state/"
agg_trans_list = os.listdir(path1)

columns1 = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [],
            'Transaction_amount': []}
for state in agg_trans_list:
    cur_state = path1 + state + "/"
    agg_year_list = os.listdir(cur_state)

    for year in agg_year_list:
        cur_year = cur_state + year + "/"
        agg_file_list = os.listdir(cur_year)

        for file in agg_file_list:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            A = json.load(data)

            for i in A['data']['transactionData']:
                name = i['name']
                count = i['paymentInstruments'][0]['count']
                amount = i['paymentInstruments'][0]['amount']
                columns1['Transaction_type'].append(name)
                columns1['Transaction_count'].append(count)
                columns1['Transaction_amount'].append(amount)
                columns1['State'].append(state)
                columns1['Year'].append(year)
                columns1['Quarter'].append(int(file.strip('.json')))

df_agg_trans = pd.DataFrame(columns1)
df_agg_trans.shape

path2 = "D:/phonepe_pulse_project/data/aggregated/user/country/india/state/"

agg_user_list = os.listdir(path2)

columns2 = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'Count': [],
            'Percentage': []}
for state in agg_user_list:
    cur_state = path2 + state + "/"
    agg_year_list = os.listdir(cur_state)

    for year in agg_year_list:
        cur_year = cur_state + year + "/"
        agg_file_list = os.listdir(cur_year)

        for file in agg_file_list:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            B = json.load(data)
            try:
                for i in B["data"]["usersByDevice"]:
                    brand_name = i["brand"]
                    counts = i["count"]
                    percents = i["percentage"]
                    columns2["Brands"].append(brand_name)
                    columns2["Count"].append(counts)
                    columns2["Percentage"].append(percents)
                    columns2["State"].append(state)
                    columns2["Year"].append(year)
                    columns2["Quarter"].append(int(file.strip('.json')))
            except:
                pass
df_agg_user = pd.DataFrame(columns2)
df_agg_user.tail()
df_agg_user.shape

path3 = "D:/phonepe_pulse_project/data/map/transaction/hover/country/india/state/"

map_trans_list = os.listdir(path3)

columns3 = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Count': [],
            'Amount': []}

for state in map_trans_list:
    cur_state = path3 + state + "/"
    map_year_list = os.listdir(cur_state)

    for year in map_year_list:
        cur_year = cur_state + year + "/"
        map_file_list = os.listdir(cur_year)

        for file in map_file_list:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            C = json.load(data)

            for i in C["data"]["hoverDataList"]:
                district = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                columns3["District"].append(district)
                columns3["Count"].append(count)
                columns3["Amount"].append(amount)
                columns3['State'].append(state)
                columns3['Year'].append(year)
                columns3['Quarter'].append(int(file.strip('.json')))

df_map_trans = pd.DataFrame(columns3)
df_map_trans.shape

path4 = "D:/phonepe_pulse_project/data/map/user/hover/country/india/state/"

map_user_list = os.listdir(path4)

columns4 = {"State": [], "Year": [], "Quarter": [], "District": [],
            "RegisteredUser": [], "AppOpens": []}

for state in map_user_list:
    cur_state = path4 + state + "/"
    map_year_list = os.listdir(cur_state)

    for year in map_year_list:
        cur_year = cur_state + year + "/"
        map_file_list = os.listdir(cur_year)

        for file in map_file_list:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            D = json.load(data)

            for i in D["data"]["hoverData"].items():
                district = i[0]
                registereduser = i[1]["registeredUsers"]
                appOpens = i[1]['appOpens']
                columns4["District"].append(district)
                columns4["RegisteredUser"].append(registereduser)
                columns4["AppOpens"].append(appOpens)
                columns4['State'].append(state)
                columns4['Year'].append(year)
                columns4['Quarter'].append(int(file.strip('.json')))

df_map_user = pd.DataFrame(columns4)
df_map_user.shape

path5 = "D:/phonepe_pulse_project/data/top/transaction/country/india/state/"

top_trans_list = os.listdir(path5)
columns5 = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Transaction_count': [],
            'Transaction_amount': []}

for state in top_trans_list:
    cur_state = path5 + state + "/"
    top_year_list = os.listdir(cur_state)

    for year in top_year_list:
        cur_year = cur_state + year + "/"
        top_file_list = os.listdir(cur_year)

        for file in top_file_list:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            E = json.load(data)

            for i in E['data']['pincodes']:
                name = i['entityName']
                count = i['metric']['count']
                amount = i['metric']['amount']
                columns5['Pincode'].append(name)
                columns5['Transaction_count'].append(count)
                columns5['Transaction_amount'].append(amount)
                columns5['State'].append(state)
                columns5['Year'].append(year)
                columns5['Quarter'].append(int(file.strip('.json')))
df_top_trans = pd.DataFrame(columns5)

df_top_trans.shape

path6 = "D:/phonepe_pulse_project/data/top/user/country/india/state/"
top_user_list = os.listdir(path6)
columns6 = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [],
            'RegisteredUsers': []}

for state in top_user_list:
    cur_state = path6 + state + "/"
    top_year_list = os.listdir(cur_state)

    for year in top_year_list:
        cur_year = cur_state + year + "/"
        top_file_list = os.listdir(cur_year)

        for file in top_file_list:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            F = json.load(data)

            for i in F['data']['pincodes']:
                name = i['name']
                registeredUsers = i['registeredUsers']
                columns6['Pincode'].append(name)
                columns6['RegisteredUsers'].append(registeredUsers)
                columns6['State'].append(state)
                columns6['Year'].append(year)
                columns6['Quarter'].append(int(file.strip('.json')))
df_top_user = pd.DataFrame(columns6)
df_top_user.shape


df_agg_trans.to_csv('agg_trans.csv',index=False)
df_agg_user.to_csv('agg_user.csv',index=False)
df_map_trans.to_csv('map_trans.csv',index=False)
df_map_user.to_csv('map_user.csv',index=False)
df_top_trans.to_csv('top_trans.csv',index=False)
df_top_user.to_csv('top_user.csv',index=False)


import mysql.connector
import pandas as pd

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Karthikeyan",
    database="phonepe_pulse"
)

mycursor = mydb.cursor()
# mycursor.execute("CREATE DATABASE IF NOT EXIST phonepe_pulse")

table_ = "CREATE DATABASE IF NOT EXIST phonepe_pulse"
# import mysql.connector
# import pandas as pd

# Assuming you have the CSV file paths and table names defined
csv_file_path = 'agg_trans.csv'
table_name = "agg_trans"

# Read the CSV file into a DataFrame
df_agg_trans = pd.read_csv('agg_trans.csv')

# Establish a connection to the MySQL server
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Karthikeyan",
    database="phonepe_pulse"
)

mycursor = mydb.cursor()

# Insert data into the MySQL table
for i, row in df_agg_trans.iterrows():
    sql = f"INSERT INTO {table_name} (State, Year, Quarter, Transaction_type,Transaction_count,Transaction_amount) VALUES (%s, %s, %s, %s, %s, %s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()

mycursor.close()
mydb.close()


import mysql.connector
import pandas as pd

# Assuming you have the CSV file paths and table names defined
csv_file_path = 'agg_user.csv'
table_name = "agg_user"

# Read the CSV file into a DataFrame
df_agg_user = pd.read_csv('agg_user.csv')

# Establish a connection to the MySQL server
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Karthikeyan",
    database="phonepe_pulse"
)

mycursor = mydb.cursor()

# Insert data into the MySQL table
for i, row in df_agg_user.iterrows():
    sql = f"INSERT INTO {table_name} (State, Year, Quarter, Brands, Count, Percentage) VALUES (%s, %s, %s, %s, %s, %s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()

mycursor.close()
mydb.close()

import mysql.connector
import pandas as pd

# Assuming you have the CSV file paths and table names defined
csv_file_path = 'map_trans.csv'
table_name = "map_trans"

# Read the CSV file into a DataFrame
df_map_trans = pd.read_csv('map_trans.csv')

# Establish a connection to the MySQL server
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Karthikeyan",
    database="phonepe_pulse"
)

mycursor = mydb.cursor()

# Insert data into the MySQL table
for i, row in df_map_trans.iterrows():
    sql = f"INSERT INTO {table_name} (State, Year, Quarter, District, Count, Amount) VALUES (%s, %s, %s, %s, %s, %s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()

mycursor.close()
mydb.close()


import mysql.connector
import pandas as pd

# Assuming you have the CSV file paths and table names defined
csv_file_path = 'map_user.csv'
table_name = "map_user"

# Read the CSV file into a DataFrame
df_map_user = pd.read_csv('map_user.csv')

# Establish a connection to the MySQL server
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Karthikeyan",
    database="phonepe_pulse"
)

mycursor = mydb.cursor()

# Insert data into the MySQL table
for i, row in df_map_user.iterrows():
    sql = f"INSERT INTO {table_name} (State, Year, Quarter, District, Registered_user, App_opens) VALUES (%s, %s, %s, %s, %s, %s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()

mycursor.close()
mydb.close()




import pandas as pd

# To find out Nan values are in which column
columns_with_nan = df_top_trans.columns[df_top_trans.isna().any()].tolist()

print("Columns with NaN values:")
print(columns_with_nan)

import pandas as pd

# to count how many Nan values in a column
nan_count = df_top_trans["Pincode"].isna().sum()

print("Number of NaN values in the Pincode column:", nan_count)

import mysql.connector
import pandas as pd
import numpy as np

# Assuming you have the CSV file paths and table names defined
csv_file_path = 'top_trans.csv'
table_name = "top_trans"

# Read the CSV file into a DataFrame
df_top_trans = pd.read_csv('top_trans.csv')

# Drop rows with NaN values in the "Pincode" column
df_top_trans = df_top_trans.dropna(subset=["Pincode"])

# Establish a connection to the MySQL server
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Karthikeyan",
    database="phonepe_pulse"
)

mycursor = mydb.cursor()

# Insert data into the MySQL table
for i, row in df_top_trans.iterrows():
    sql = f"INSERT INTO {table_name} (State, Year, Quarter, Pincode, Transaction_count, Transaction_amount) VALUES (%s, %s, %s, %s, %s, %s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()

mycursor.close()
mydb.close()

import pandas as pd

# To find out Nan values are in which column
columns_with_nan = df_top_user.columns[df_top_user.isna().any()].tolist()

print("Columns with NaN values:")
print(columns_with_nan)


import mysql.connector
import pandas as pd
import numpy as np

# Assuming you have the CSV file paths and table names defined
csv_file_path = 'top_user.csv'
table_name = "top_user"

# Read the CSV file into a DataFrame
df_top_trans = pd.read_csv('top_user.csv')


# Establish a connection to the MySQL server
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Karthikeyan",
    database="phonepe_pulse"
)

mycursor = mydb.cursor()

# Insert data into the MySQL table
for i, row in df_top_user.iterrows():
    sql = f"INSERT INTO {table_name} (State, Year, Quarter, Pincode, Registered_users) VALUES (%s, %s, %s, %s, %s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()

mycursor.close()
mydb.close()


import mysql.connector

# Establish a connection to the MySQL server
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Karthikeyan",
    database="phonepe_pulse"
)

# Create a cursor object
mycursor = mydb.cursor()

mycursor.execute("show tables")
mycursor.fetchall()



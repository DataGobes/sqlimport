import pandas as pd
import pyodbc

server = 's060a0408\\dwh'
username = 'Python_User'
password = 'Unity_2018'
database = 'Datamart'
driver = '{ODBC Driver 13 for SQL Server}'

con = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)

with open('input.sql') as file:
    qry = file.read()

a = pd.read_sql(qry, con)
print(a.describe())
print(a.info())

a.to_csv('output.csv')

# Connecting to snowflake using AZURE AD login  and Using the Python Connector to Executing query
# remove authenticator to connect using username and password without  AZURE AD

# !pip install snowflake-connector-python

import snowflake.connector
import pandas as pd

ctx = snowflake.connector.connect(
    user='daneshwar@company.com.in',
    password='Password',
    account='company.ap-southeast-2',
    authenticator='externalbrowser',
    role='KROLE',
    warehouse='KWAREHOUSE',
    database='KDATABASE',
    schema='KSCHEMA'
    )
cs = ctx.cursor()

query='''
SELECT * FROM "KWAREHOUSE"."ABC"."CUSTOMERPRODUCTS" LIMIT 2
'''

cs.execute(query)

df=pd.DataFrame.from_records(iter(cs),columns=[x[0]for x in cs.description])
print(df)


# Closing this will close the connection and will 
# intialize again to authenticator so keep them open until all operations
cs.close()
ctx.close()
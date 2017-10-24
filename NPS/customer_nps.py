import pandas as pd
import datetime
import numpy as np
import time
import requests
import json

## Get Credentials for oauth_token


with open('credentials.json') as data_file:    
    cred = json.load(data_file)
    
token = cred['API']['promoter']['token']

# Customer Campaign
url = 'https://app.promoter.io/api/feedback/?posted_date_0=2017-01-01&survey__campaign=14594&'

headers = {
  'Authorization': "Token " + token,
  'Content-Type': 'application/json'
}

full_output = pd.DataFrame()
page_num = 0

while True:
    page_num +=1
    page = 'page=' + str(page_num)   
    # Get Data
    r = requests.get(url + page, headers = (headers))
    #Check for problems getting data
    detail_check = 'detail' in [key for key in r.__dict__]
    status_check = r.__dict__.get('status_code')
    # Check if no rows returned
    if (detail_check):
        print('Reached last Page')
        break
    elif (status_check != 200):
        print('Had Status Code != 200 on page ' + str(page_num))
        break
    else:
        # JSON format
        r_json = r.json()

        # Results Dict
        feedback = r_json['results'] 

        # Loop to format
        feedback_tmp = pd.DataFrame.from_dict(feedback)
        new_df = pd.DataFrame()
        for ix, row in feedback_tmp.iterrows(): 
            contact_tmp = pd.DataFrame(feedback_tmp.contact[ix])
            attribute_cols = pd.DataFrame([contact_tmp.attributes])
            attribute_cols['email'] = feedback_tmp.contact[ix]['email']
            attribute_cols['first_name'] = feedback_tmp.contact[ix]['first_name']
            attribute_cols['last_name'] = feedback_tmp.contact[ix]['last_name']
            attribute_cols['id'] = feedback_tmp.contact[ix]['id']
            attribute_cols['campaign'] = feedback_tmp['campaign'][ix]
            attribute_cols['comment'] = feedback_tmp['comment'][ix]
            attribute_cols['comment_updated_date'] = feedback_tmp['comment_updated_date'][ix]
            attribute_cols['followup_href'] = feedback_tmp['followup_href'][ix]
            attribute_cols['href'] = feedback_tmp['href'][ix]
            attribute_cols['id'] = feedback_tmp['id'][ix]
            attribute_cols['posted_date'] = feedback_tmp['posted_date'][ix]
            attribute_cols['id'] = feedback_tmp['id'][ix]
            attribute_cols['score'] = feedback_tmp['score'][ix]
            attribute_cols['score_type'] = feedback_tmp['score_type'][ix]
            new_df = new_df.append(attribute_cols)
        new_df = new_df.reset_index()
    full_output = full_output.append(new_df)

# Get customer scores
full_output['score_class'] = 'passive'
full_output.loc[full_output.score <= 6, 'score_class'] = 'detractor'
full_output.loc[full_output.score >= 9, 'score_class'] = 'promoter'
full_output = full_output.reset_index()


check = full_output.Number.isnull()
z = zip(full_output.Number, full_output.descriptive_order_number, check)
full_output['order_number'] = [y if check else x for x ,y, check in z]

# Get customer scores
cols = ['posted_date', 'email', 'first_name', 'last_name', 'score',
        'score_type', 'comment', 'order_number', 'order_id']
customer_nps = full_output[cols]


## Getting Credential Data
import json
with open('credentials.json') as data_file:    
    cred = json.load(data_file)

from sqlalchemy import create_engine

# DB Engine
DB_TYPE = 'postgresql'
DB_DRIVER = 'psycopg2'
DB_USER = cred['database']['cube']['user']
DB_PASS = cred['database']['cube']['password']
DB_HOST = cred['database']['cube']['host']
DB_PORT = '5432'
DB_NAME = cred['database']['cube']['dbname']
POOL_SIZE = 50
SQLALCHEMY_DATABASE_URI = '%s+%s://%s:%s@%s:%s/%s' % (DB_TYPE, DB_DRIVER, DB_USER,
                                                          DB_PASS, DB_HOST, DB_PORT, DB_NAME)

ENGINE = create_engine(
        SQLALCHEMY_DATABASE_URI, pool_size=POOL_SIZE, max_overflow=0)


# Get order data
orders_query = """
select 
id::text as order_id,
number,
reservation_start
from reservations_order
"""

orders = pd.read_sql_query(orders_query, ENGINE)

# Merge to get missing order numbers
merged_output = pd.merge(customer_nps, orders, how='left', on= 'order_id')

# Merge the order number columns
check = merged_output.order_number.isnull()
z = zip(merged_output.order_number, merged_output.number, check)
merged_output['order_number'] = [y if check else x for x ,y, check in z]

# Keep Selected Columns
cols = ['posted_date', 'email', 'first_name', 'last_name', 'score',
        'score_type', 'order_number', 'reservation_start','comment',]
full_customer_nps = merged_output[cols]


# Functions for cleaning the data
def bulk_null_replace(df):   
    for col in df.columns:
        nulls = df[col].isnull()
        df.loc[nulls, col] = 'none'

def text_replace(comment): 
    return ''.join([i if ord(i) < 128 else ' ' for i in comment])

def blank_replace(comment):
    return ''.join(['none' if i == '' else i for i in comment])

def null_character_replace(comment):
    return comment.replace('\x00', '')



# bulk_null_replace(full_customer_nps)
full_customer_nps['comment'] = [text_replace(comment) for comment in full_customer_nps.comment]
# full_customer_nps['comment'] = [blank_replace(comment) for comment in full_customer_nps.comment]
full_customer_nps['comment'] = [null_character_replace(comment) for comment in full_customer_nps.comment]


# inserts the data to the table.
try: 
    full_customer_nps.to_sql('nps_customer', ENGINE, if_exists='replace')
except Exception as e:
    print(e)

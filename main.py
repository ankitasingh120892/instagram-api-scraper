
'''
This script fetches list of target accounts to be scraped in parallel from source database.
Fetches all users a target account follows in parallel batches.
Inserts output of users a target follows into a target table in database

All environment varibales are defined in .env file
'''

import os
import requests
import json
import random
import mysql.connector
import threading
from time import sleep
from dotenv import load_dotenv

#Load environment variables from .env file
#load_dotenv()
load_dotenv(override=True)
db_host = os.getenv('DB_HOST')
db_port = int(os.getenv('DB_PORT'))
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
database = os.getenv('DATABASE')
proxy = os.getenv('PROXY')
input_table = os.getenv('INPUT_TABLE')
output_table = os.getenv('OUTPUT_TABLE')
max_following = int(os.getenv('MAX_FOLLOWING'))
accounts_number = int(os.getenv('ACCOUNTS_NUMBER'))
max_retry = int(os.getenv('MAX_RETRY'))
sleep_time = int(os.getenv('SLEEP_TIME'))
request_sleep_time = int(os.getenv('REQUEST_SLEEP_TIME'))
proxy_ip_change_wait_time = int(os.getenv('PROXY_IP_CHANGE_WAIT_TIME'))
sessions = json.loads(os.environ['SESSIONS'])

print(f'\n db_host\t\t\t=\t{db_host}'
    f'\n db_port\t\t\t=\t{db_port}'
    f'\n db_user\t\t\t=\t{db_user}'
    #f'\n db_password\t\t\t=\t{db_password}'
    f'\n db_password\t\t\t=\t**********'
    f'\n database\t\t\t=\t{database}'
    #f'\n proxy\t\t\t\t=\t{proxy}'
    f'\n proxy\t\t\t\t=\t******'
    f'\n sleep_time\t\t\t=\t{sleep_time}'
    f'\n input_table\t\t\t=\t{input_table}'
    f'\n output_table\t\t\t=\t{output_table}'
    f'\n max_following\t\t\t=\t{max_following}'
    f'\n accounts_number\t\t=\t{accounts_number}'
    f'\n max_retry\t\t\t=\t{max_retry}'
    f'\n request_sleep_time\t\t=\t{request_sleep_time}'
    f'\n proxy_ip_change_wait_time\t=\t{proxy_ip_change_wait_time}'
    f'\n sessions\t\t\t=\t{sessions}')


def get_accountIDs_from_DB(accounts_number, default=5):
    connection = mysql.connector.connect(host=db_host, port=db_port, user=db_user, password=db_password, database=database)
    cursor = connection.cursor()
    query = f'select user_id from {input_table} where is_processed = 7 limit {accounts_number};'
    print(f'\nFetching accounts from source table,\n\tQuery= "{query}"')
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    account_ids = [ userid[0] for userid in result ]
    print(f'\tAccounts returned from source table: {account_ids}')
    return account_ids


def insert_followings_data_outputTable(followings, account_id):
    print(f'\t\t\t\t{account_id}: Starting function "insert_followings_data_outputTable"')
    connection = mysql.connector.connect(host=db_host, port=db_port, user=db_user, password=db_password, database=database)
    cursor = connection.cursor()
    query = f'INSERT INTO {output_table}(user_id,username,title,isprivate,isverified,followedby) VALUES(%s, %s, %s, %s, %s, %s)'   
    print(f'\t\t\t\t{account_id}: inserting followings data into target table, query: {query}')
    cursor.executemany(query, followings)
    connection.commit()
    cursor.close()
    print(f'\t\t\t\t{account_id}: Function insert_followings_data_outputTable completed')


def update_isProcessed_inputTable(account_id, is_processed):
    #global is_processed
    print(f'\t\t\t\t\t{account_id}: Starting function "update_isProcessed_inputTable", is_processed: {is_processed}')
    connection = mysql.connector.connect(host=db_host, port=db_port, user=db_user, password=db_password, database=database)
    cursor = connection.cursor()
    query = f'update {input_table} set is_processed={is_processed} where user_id={account_id}' 
    cursor.execute(query)
    connection.commit()
    cursor.close()
    print(f'\t\t\t\t\t{account_id}: Completed function "update_isProcessed_inputTable", is_processed: {is_processed}')


def get_followings(account_id):
    print(f'\n\t\tStarting "get_followings" thread for account_id: {account_id}')  
    next_max_id = 0
    retry = 0
    
    #run loop until next_max_id not reached last & next_max_id is less than max_following
    while next_max_id != 'Last' and next_max_id <= max_following and retry<max_retry:
    
        print(f'\t\t\t{account_id}: Entered While loop')
        print(f'\t\t\t\t{account_id}: next_max_id = {next_max_id}')
        print(f'\t\t\t\t{account_id}: retry = {retry+1}/{max_retry}')
        print(f'\t\t\t\t{account_id}: max_following = {max_following}')
        followings = []
        session_ID = random.choice(sessions)
        headers = {
            "Accept": "*/*",
            "cookie": session_ID,
            "x-ig-app-id": "936619743392459",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
            "Connection": "keep-alive",
            "Accept-Encoding": "gzip, deflate, br",
            "proxy-connection":proxy
        }
        print(f'\t\t\t\t{account_id}: headers = {headers["cookie"]}')
        if next_max_id == 0:
            url = f'https://i.instagram.com/api/v1/friendships/{account_id}/following/?count=200'
        else:
            url = f'https://i.instagram.com/api/v1/friendships/{account_id}/following/?count=200&max_id={next_max_id}'
        print(f"\t\t\t\t{account_id}: Starting Downloading data")
        print(f'\t\t\t\t{account_id}: url = {url}')
        response = requests.request("GET", url, headers=headers)
        #print(response.text)
        data_text = response.text
        print(f"\t\t\t\t{account_id}: Completed Downloading data")
        response_statusCode = response.status_code
        print(f'\t\t\t\t{account_id}: Response.status_code = {response.status_code}')
        
        # testing data
        #data_text = '{"message":"checkpoint_required","checkpoint_url":"https://i.instagram.com/challenge/?next=/api/v1/friendships/47712304401/following/%253Fcount%253D200","lock":true,"flow_render_type":0,"status":"fail"}'
        
        if response.status_code == 200:
            try:
                print(f'\t\t\t\t\t{account_id}: Entered Try Block')
                data = json.loads(data_text)
                if json.loads(response.text)['status'] == 'ok':
                    next_max_id = data.get('next_max_id', 'Last')
                    if next_max_id != 'Last':
                        next_max_id = int(next_max_id)
                    for user in data['users']:
                        user_id = user['pk_id']
                        username = user['username']
                        full_name = user['full_name']
                        is_private = user['is_private']
                        is_verified = user['is_verified']
                        followings.append(tuple([user_id, username, full_name, is_private, is_verified, account_id])) 
                    print(f'\t\t\t\t\t\t{account_id}: calling function insert_followings_data_outputTable')
                    insert_followings_data_outputTable(followings, account_id)
                    is_processed = 8
                    print(f'\t\t\t\t\t\t{account_id} is_processed: {is_processed}, retry:{retry}')
            except Exception as e:                
                print(f"\t\t\t\t\t{account_id}: Try Block failed")
                print(f"\t\t\t\t{account_id}: Entered Except Block")
                print(f"\t\t\t\t\t{account_id}: Error: {e}")
                print(f"\t\t\t\t\t{account_id}: Response data last 100 chars = {data_text[-100:]}")
                
                #Delete failed session ID
                if len(sessions) > 0:
                    print(f"\t\t\t\t\t{account_id}: Deleted last failed session id :{session_ID}")
                    sessions.remove(session_ID)
                else:
                    print(f'\t\t\t\t\t{account_id}: All sessions IDs blocked and removed, please provide new session IDs')                
                retry = max_retry
                is_processed = -7
                print(f'\t\t\t\t\t{account_id}: Program sleeping for {proxy_ip_change_wait_time} seconds, waiting for proxy IP to rotate')
                sleep(proxy_ip_change_wait_time)                
            finally:
                print(f'\t\t\t\t{account_id}: Entered finally block')
                print(f'\t\t\t\t\t{account_id}: calling function update_isProcessed_inputTable, is_processed= {is_processed}')
                update_isProcessed_inputTable(account_id, is_processed)
                print(f'\t\t\t\t\t{account_id}: finally block completed')
        else:
            print(f'\t\t\t\t\t{account_id}: Request Failed')            
            print(f'\t\t\t\t\t{account_id}: Request text = {response.text}')
            retry += 1
            is_processed = -7
            print(f'\t\t\t\t\t{account_id}: calling function update_isProcessed_inputTable, is_processed= {is_processed}')
            update_isProcessed_inputTable(account_id, is_processed)

        print(f'\t\t\t\t{account_id}: Request Sleeping for {request_sleep_time} seconds\n')
        sleep(request_sleep_time)    
    print(f'\t\t{account_id}: Completed "get_followings" thread \n\t\t{account_id}: retries = {retry}/{max_retry}\t next_max_id: {next_max_id}\t max_following: {max_following}')


#calling the main fuction
if __name__ == "__main__":
    while True:
        #fetch user_ids from DB
        account_ids = get_accountIDs_from_DB(accounts_number)
        #account_ids = [268891661]
        if len(account_ids) == 0:
            print(f'\nNo accounts returned from source Table, with flag is_processed=7')
            print(f'Program sleeping for {sleep_time} seconds\n')
            sleep(sleep_time)
        else:
            print(f'\tFetching Followings of accounts: {account_ids}')
            thread_list = []
            for account_id in account_ids:
                thread = threading.Thread(target=get_followings, args=(account_id,))
                thread_list.append(thread)
                thread.start()
            
            for thread in thread_list:
                thread.join()


#-----------------------------------------------
#Fetch Followings data
if response status code == 200
    try:
        if response text status == 'ok'
            insert data into Output Table
    except:
        Session id is blocked, remove session id from array.
        cancel request for current users, move to next user.
else:
    retry again
    
  

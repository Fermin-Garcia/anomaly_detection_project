import pandas as pd
import env
import os



# First we need to use our env file to access our mysql data base
def get_connection():
    
    '''
    This function will return the link to access mysql database. 
    '''
    user=env.username
    password=env.password
    host=env.host
    db=env.db
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

    
# next we define the query that we are calling from sql 
query = '''select * 
from cohorts
left join logs as l on l.cohort_id = cohorts.id
'''


# then we get this and use it to make a fucntion that 
# will check to see if a csv file containing the telco file named 
# telco.csv is it doesn't then the code will then run the query through 
# pymysql and cache the file locally so it is fastly accessiable 


def get_log_data():
    if os.path.exists('log_data.csv') == True:
        return pd.read_csv('log_data.csv')
    else:
        df = pd.read_sql(query, get_connection())
        df.to_csv('log_data.csv')
        return pd.read_csv('log_data.csv')



def acquire_data():
    df = pd.read_csv('anonymized-curriculum-access.txt', sep= ' ', names=['date', 'time', 'path', 'user_id', 'cohort_id', 'ip' ])
    df['timestamp'] = df['date'] + ' '+ df['time']
    df.drop(columns=['date', 'time'], inplace= True)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp').sort_index()
    
    return df
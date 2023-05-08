


def prepare():
    df = pd.read_csv('anonymized-curriculum-access.txt', sep= ' ', names=['date', 'time', 'path', 'user_id', 'cohort_id', 'ip' ])
    df['timestamp'] = df['date'] + ' '+ df['time']
    df.drop(columns=['date', 'time'], inplace= True)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp').sort_index()
    
    return df
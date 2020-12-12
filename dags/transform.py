import pandas as pd
import numpy as np

def transformm_df(df):
    print('Entramos 1')
    df = pd.melt(df, df.iloc[:,0:4])
    df['Province/State'] = df['Province/State'].fillna('')
    df['variable'] = pd.to_datetime(df['variable']) 
    df.rename(columns = {'variable':'event_date'}, inplace = True)
    print('Entramos 2')
    df_sorted = df.sort_values(by = ['Country/Region', 'Province/State', 'event_date'])
    df_sorted['ID'] = df_sorted['Country/Region'] + df_sorted['Province/State']
    df_sorted['cases_per_day'] = df_sorted.value - df_sorted.groupby(['ID'])['value'].shift(1) 
    df_sorted['cases_per_day'] = df_sorted['cases_per_day'].fillna(0)
    df_sorted = df_sorted.sort_values(by = ['Country/Region', 'event_date'])
    print('Entramos 3')
    print(pd.__version__)
    #df_sorted['cases_per_day_per_country'] = df_sorted.value - df_sorted.groupby(['Country/Region'])['value'].shift(1)
    #df_sorted['cases_per_day_per_country'] = df_sorted['cases_per_day_per_country'].fillna(0)
    #df_final = df_sorted.drop(columns = ['ID']) 
    df_sorted['cum_sum_by_country'] = df_sorted.groupby(['Country/Region', 'event_date'])['value'].cumsum()
    print('Entramos 3 1')
    df_sorted.cum_sum_by_country[df_sorted['Province/State'] == ''] = 0
    print('Entramos 3 2')
    df_sorted = df_sorted.sort_values(['Country/Region', 'event_date','cum_sum_by_country'], ascending=[True, True, False])
    print('Entramos 3 3')
    print(df_sorted.columns)
    print(df_sorted.groupby(['Country/Region', 'event_date']))
    df_maxes = df_sorted.groupby(['Country/Region', 'event_date'])['Country/Region','event_date','cum_sum_by_country'].max()
    print(df_maxes)  
    print('Entramos 4')
    print(df_maxes.columns)
    print(df_maxes.info())
    df_maxes.reset_index(inplace=True)
    print(df_maxes.columns)
    print(df_maxes.iloc[:,0])
    keys = np.array(df_maxes.iloc[:,0] + df_maxes['event_date'].astype('str'))
    print(keys)
    print('Entramos 4 1')
    print(df_maxes['cum_sum_by_country'])
    dictionary = dict()
    for i in range(len(df_maxes.cum_sum_by_country)):
        dictionary[keys[i]] = df_maxes.cum_sum_by_country[i]

    print('Entramos 5')
    max_list = []
    temp_country = ''
    temp_date = '12-10-1492'
    temp_max = -1
    key = ''

    df_sorted.event_date = df_sorted.event_date.astype('str')
    print('Entramos 6')

    print(df_sorted.iloc[:,0])
    print(df_sorted.iloc[:,1])
    print(df_sorted.iloc[:,5])

    for i in range(len(df_sorted)):
        if temp_country != df_sorted.iloc[i,1] or temp_date != df_sorted.iloc[i,4]:
            temp_country = df_sorted.iloc[i,1]
            temp_date = df_sorted.iloc[i,4]
            key = str(temp_country) + str(temp_date)
        if df_sorted.iloc[i,0] == '':
            max_list.append(df_sorted.iloc[i,5])
        else:    
            max_list.append(dictionary.get(key))
    print('Entramos 7')
    df_sorted['cum_max'] = np.array(max_list)
    df_maxes.reset_index(drop = True, inplace = True)
    df_maxes['cases_by_day_by_country'] = df_maxes.cum_sum_by_country - df_maxes.groupby(['Country/Region'])['cum_sum_by_country'].shift(1) 
    df_maxes['cases_by_day_by_country'] = df_maxes['cases_by_day_by_country'].fillna(0)
    keys = np.array(df_maxes.iloc[:,0] + df_maxes.event_date.astype('str'))
    print('Entramos 8')

    dictionary = dict()
    for i in range(len(df_maxes.cum_sum_by_country)):
        dictionary[keys[i]] = df_maxes.cases_by_day_by_country[i]
    per_day_list = []
    temp_country = ''
    temp_date = '12-10-1492'
    key = ''

    df_sorted.event_date = df_sorted.event_date.astype('str')
    print('Entramos 9')

    for i in range(len(df_sorted)):
        if temp_country != df_sorted.iloc[i,1] or temp_date != df_sorted.iloc[i,4]:
            temp_country = df_sorted.iloc[i,1]
            temp_date = df_sorted.iloc[i,4]
            key = str(temp_country) + str(temp_date)
        if df_sorted.iloc[i,0] == '':
            per_day_list.append(df_sorted.iloc[i,7])
        else:    
            per_day_list.append(dictionary.get(key))
    print('Entramos 10')
    df_sorted['cases_per_day_per_country'] = np.array(per_day_list)
    df_final = df_sorted.drop(columns = ['ID']) 
    return df_final
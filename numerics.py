import time
import json
from math import ceil
import pandas as pd
import numpy as np
from datetime import datetime

from query import run_query

## DATA Definition
months = {'Jan': 1,
# 'Feb': 2,
# 'Mar': 3,
# 'Apr': 4,
# 'May': 5,
# 'Jun': 6,
# 'Jul': 7,
# 'Aug': 8,
# 'Sep': 9,
# 'Oct': 10,
# 'Nov': 11,
# 'Dec': 12
}

# {"name": "MEA", "id": "1632"},
with open('mea_countries.json') as file:
    countries = json.load(file)['relevant']

programs = {
    'GTe': 9,
    # 'GTa': 8,
    # 'GV': 7
}

incoming = {True: 'opportunity_home_mc', False: 'person_home_mc'}


kpis = {
    'APP': 'created_at',
    'ACC': 'date_an_signed',
    'APD': 'date_approved',
    'RE': 'date_realized'
}

## Building Query (by forming sub-queries)
def get_request_clause(date_field:str):
    basic_clause = """
        {
            paging {
                total_items
            }
    """ 
    date_fields = ['created_at', 'an_signed_at', 'date_approved', 'date_realized']
    if date_field == "created_at":
        return basic_clause + "\n}"
    if date_field == "date_an_signed":
        date_field = "an_signed_at"
    
    cutoff_index = date_fields.index(date_field)
    request_dates = "\n".join(date_fields[cutoff_index-1:cutoff_index+1])

    return """
        %s
        data {
            %s
        }
    }
    """ % (basic_clause, request_dates)
    
def _dateform(date_str):
    """ Date2 - Date1 in days"""
    date_str = date_str[:-1] # Remove Timezone code
    return  datetime.fromisoformat(date_str)

def _avgdays_btwn(dates_li: list):
    if dates_li:
        total_days, count = 0,0
        for obj in dates_li:
            dates = list(obj.values())
            total_days += (_dateform(dates[1]) - _dateform(dates[0])).days
            count += 1

        if total_days: return ceil(total_days/count)
        else: return None
    else:
        return None

def form_subqueries():
    sub_queries = []
    index = 0
    for month_num in months.values():
        start_month = str(month_num).rjust(2, '0')
        next_month = (month_num + 1) % 12 or 12
        end_month = str(next_month).rjust(2, '0')

        for mc in countries:
            for p_id in programs.values():
                for mc_type in incoming.values():
                    for kpi_query in kpis.values():
                        sub_query = """
                            %s: allOpportunityApplication(
                                filters: {%s: {from: "2022-%s-01", to: "2022-%s-01"}

                                # Function
                                programmes: [%i]

                                # incoming or outgoing function
                                %s:[%s]
                                }) 

                            # {data_clause}
                            %s
                        """ % ('i'+str(index), kpi_query, start_month, end_month, p_id , mc_type, mc['id'], get_request_clause(kpi_query))
                        sub_queries.append(sub_query)
                        index += 1
    return sub_queries
                    

def execute_queries(sub_queries, limit=400):
    query_top = """
    query getApplicationList {
    """
    query_bottom = "}"
    result = []
    process_times = []
    print('Executing query...')
    start = time.perf_counter()
    for batch in range(0, len(sub_queries), limit):
        print(f'BATCH : {batch+1} - {batch+limit}')
        query = query_top + ",\n".join(sub_queries[batch: batch+limit]) + query_bottom
        raw_data = run_query(query)
        data = [d['paging']['total_items'] for d in raw_data.values()]

        for d in raw_data.values():
            dates = d.get('data') # list of application-level data of datetimes
            if dates is None:
                continue
            # Process Times
            avg_days = _avgdays_btwn(dates)
            process_times.append(avg_days)
        result += data
    print(f'Time Taken for query: {time.perf_counter()-start:0.4f} seconds')
    return result, process_times

def get():
    queries = form_subqueries()
    data, proc_times = execute_queries(queries)
    data_np, proc_times_np = np.array(data), np.array(proc_times)
    data_np = data_np.reshape((-1, len(kpis.keys())))

    proc_times_np = proc_times_np.reshape((-1, 3))



    cols = ['month', 'mc', 'department']
    res_df = pd.DataFrame(columns=cols)

    for month_str in months.keys():
        for mc in countries:
            for p_name in programs.keys():
                for in_ in incoming.keys():
                    dept = 'i' if in_ else 'o'
                    dept += p_name

                    # Build Row
                    row = [month_str, mc['name'], dept]
                    try:
                        res_df.loc[len(res_df.index)] = row
                    except ValueError as e:
                        print(e, f'These are the headers:{cols}\n and values that caused it: {row}')
    res_df.loc[:, [*kpis.keys()]] = data_np
    res_df.loc[:, ['APP-ACC Days', 'ACC-APD Days', 'APD-RE Days']] = proc_times_np
    res_df.fillna('', inplace=True)
    return res_df

def getcrs(df):
    kpi_cols =  list(kpis.keys())
    for index in range(len(kpi_cols[:-1])):
        current = kpi_cols[index]
        nxt = kpi_cols[index+1]
        df[f'{current}-{nxt} %'] = df[nxt]/df[current]
    df.drop(kpi_cols + ['APP-ACC Days', 'ACC-APD Days', 'APD-RE Days'], axis=1, inplace=True)
    df.fillna(0, inplace=True)
    df.replace([np.inf, -np.inf], 0, inplace=True)
    return df

if __name__ == '__main__':
    df = get()
    print(df)

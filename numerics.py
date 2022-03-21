import time
import json
import pandas as pd

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
    'GTa': 8,
    'GV': 7
}

incoming = {True: 'opportunity_home_mc', False: 'person_home_mc'}


kpis = {
    'APP': 'created_at',
    'ACC': 'date_an_signed',
    'APD': 'date_approved',
    'RE': 'date_realized'
}

## Building Query (by forming sub-queries)
paging_part = """
    {
        paging {
            total_items
        }
    }
""" 
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

                            # {paging_part}
                            %s
                        """ % ('i'+str(index), kpi_query, start_month, end_month, p_id , mc_type, mc['id'], paging_part)
                        sub_queries.append(sub_query)
                        index += 1
    return sub_queries
                    

def execute_queries(sub_queries, limit=400):
    query_top = """
    query getApplicationList {
    """
    query_bottom = "}"
    result = []
    print('Executing query...')
    start = time.perf_counter()
    for batch in range(0, len(sub_queries), limit):
        print(f'BATCH : {batch+1} - {batch+limit}')
        query = query_top + ",\n".join(sub_queries[batch: batch+limit]) + query_bottom
        raw_data = run_query(query)
        data = [d['paging']['total_items'] for d in raw_data.values()]
        result += data
    print(f'Time Taken for query: {time.perf_counter()-start:0.4f} seconds')
    return result

def get():
    queries = form_subqueries()
    data = execute_queries(queries)

    def get_kpis_values():
        num_kpis = len(kpis.keys())
        for x in range(0, len(data), num_kpis):
            yield data[x: x+num_kpis]
    kpi_data = get_kpis_values()

    cols = ['month', 'mc', 'department', *kpis.keys()]
    res_df = pd.DataFrame(columns=cols)

    for month_str in months.keys():
        for mc in countries:
            for p_name in programs.keys():
                for in_ in incoming.keys():
                    dept = 'i' if in_ else 'o'
                    dept += p_name

                    # Build Row
                    row = [month_str, mc['name'], dept]
                    row += next(kpi_data) # Add numerics
                    try:
                        res_df.loc[len(res_df.index)] = row
                    except ValueError as e:
                        print(e, f'These are the headers:{cols}\n and values that caused it: {row}')
    return res_df

if __name__ == '__main__':
    df = get()
    print(df)

import json
import pandas as pd
from query import run_query

# {"name": "MEA", "id": "1632"},
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

with open('mea_countries.json') as file:
    countries = json.load(file)['data']

programs = {
    'GTe': 9,
    # 'GTa': 8,
    # 'GV': 7
}

incoming = {True: 'opportunity_home_mc', False: 'person_home_mc'}


kpis = {
    'APP': 'created_at',
    'ACC': 'date_matched',
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
sub_queries = []
index = 0
for month_str, month_num in months.items():
    start_month = str(month_num).rjust(2, '0')
    next_month = (month_num + 1) % 12 or 12
    end_month = str(next_month).rjust(2, '0')

    for mc in countries:
        for p_name, p_id in programs.items():
            for in_, mc_type in incoming.items():
                for kpi, kpi_query in kpis.items():
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
                    



query_top = """
query getApplicationList {
"""


query_bottom = "}"
query = query_top + ",\n".join(sub_queries) + query_bottom
# print(query)
data = run_query(query)
# data = {'i0': {'paging': {'total_items': 0}}, 'i1': {'paging': {'total_items': 279}}, 'i2': {'paging': {'total_items': 0}}, 'i3': {'paging': {'total_items': 1}}, 'i4': {'paging': {'total_items': 0}}, 'i5': {'paging': {'total_items': 0}}, 'i6': {'paging': {'total_items': 0}}, 'i7': {'paging': {'total_items': 0}}, 'i8': {'paging': {'total_items': 0}}, 'i9': {'paging': {'total_items': 0}}, 'i10': {'paging': {'total_items': 0}}, 'i11': {'paging': {'total_items': 0}}, 'i12': {'paging': {'total_items': 0}}, 'i13': {'paging': {'total_items': 20}}, 'i14': {'paging': {'total_items': 0}}, 'i15': {'paging': {'total_items': 1}}, 'i16': {'paging': {'total_items': 209}}, 'i17': {'paging': {'total_items': 108}}, 'i18': {'paging': {'total_items': 0}}, 'i19': {'paging': {'total_items': 8}}}

def get_kpis_values():
    num_kpis = len(kpis.keys())
    raw_data = [d['paging']['total_items'] for d in data.values()]
    for x in range(0, len(raw_data), num_kpis):
        yield raw_data[x: x+num_kpis]
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
                        
print(res_df)
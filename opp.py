import time
import json
from math import ceil
import pandas as pd
import numpy as np
from datetime import datetime

# {"name": "MEA", "id": "1632"},
with open('mea_countries.json') as file:
    countries = json.load(file)['relevant']


mc_ids = ",".join([mc['id'] for mc in countries])

from query import run_query
query = """
query getOpportunitiesList {
	opportunities(
		pagination: { per_page: 1000 }
		filters: {
			status: "open"
			date_opened: { from: "2021-11-01" }
			sort: date_opened
			sort_direction: asc
            home_mcs: [
                %s
            ]
		}
	) {
		data {
			id
            title
			# sub_product {
			# 	name
		
			# 	sub_product_group
			# }
			
			backgrounds {
				constant_name
			}

			date_opened
			sdg_info {
				sdg_target {
					Goal: goal_index
					Target: target_index
				}
			}

			host_lc {
				name
			}
			host_mc: home_mc {
				name
			}
            location
			programme {
				short_name_display
			}
			applicants_count
			view_count
			all_slots {
				nodes {
					available_openings
					start_date
					end_date
				}
			}
		}
	}
}
""" % (mc_ids)
def get():
    res = run_query(query)
    res = res['opportunities']['data']
    res_df = pd.json_normalize(res, sep='_')
    # Reduce Backgrounds
    backgrounds = set({})
    def reduce_bakgrounds(x):
        collect = []
        for obj in x:
            backgrnd = obj['constant_name']
            backgrounds.add(backgrnd)
            collect.append(backgrnd)
        return ', '.join(collect)

    def splitup_slots(slots):
        return [s['start_date'] for s in slots]
    
            


    res_df['backgrounds'] = res_df['backgrounds'].apply(reduce_bakgrounds)
    back_df = pd.DataFrame(backgrounds, columns=['Background']).sort_values('Background')

    # Split up all_slots column into Slot 1, Slot 2, ....
    col_to_split = 'all_slots_nodes'
    split_up_df = pd.DataFrame(res_df[col_to_split].apply(splitup_slots).tolist())
    res_df.drop(col_to_split, axis=1, inplace=True)
    cols_to_add = [f'Slot #{i+1}' for i in range(split_up_df.shape[1])]
    res_df[cols_to_add] = split_up_df

    res_df.fillna('', inplace=True)

    
    
    
    


    return res_df, back_df

if __name__ == '__main__':
    data = get()
    print(data)
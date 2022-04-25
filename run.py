#!/usr/bin/env python3

import os
import json
import tempfile
from pprint import pprint
from pytest import param
import requests
import asyncio
import aiohttp
import time
import numerics
from numerics import months
import opp
import pygsheets
import numpy as np
import pandas as pd
from functools import partial
from utils import write_base64str_obj_to_file
from datetime import datetime

# numbers, covid, opps
RUN_THESE = ['opps', 'covid', 'numbers', 'nps']
def main():
    # Get credentials from service-account-file to access Google Sheets
    print("Creating temporary file for service account credentials...")
    temp = tempfile.NamedTemporaryFile()
    try:
        access_creds = os.environ['GOOGLE_CREDS']
        write_base64str_obj_to_file(access_creds, temp.name)
    finally:
        gc = pygsheets.authorize(service_file=temp.name)
        temp.close()

    workbook = gc.open_by_key(os.environ["SPREADSHEET_ID"])
    # Create handy function to write to sheets
    set_worksheet_todf = partial(pygsheets.Worksheet.set_dataframe, start="A1", copy_head=True)
    set_opps_worksheet_todf = partial(pygsheets.Worksheet.set_dataframe, start="B5", copy_head=True)
    set_list_of_backgrnds_todf = partial(pygsheets.Worksheet.set_dataframe, start="E2", copy_head=False)

    if 'covid' in RUN_THESE:
        print('# Updating COVID Sheet #')
        ## Get COVID Data
        owid = pd.read_csv('https://github.com/owid/covid-19-data/raw/master/public/data/latest/owid-covid-latest.csv', low_memory=False)
        text_cols = ['location']
        int_cols = ['people_vaccinated', 'people_fully_vaccinated', 'population']
        cols = text_cols + int_cols

        owid[int_cols] = owid[int_cols].astype('object')
        owid = owid[cols]

        # Get %
        owid['One Dose %'] = owid['people_vaccinated']/owid['population']
        owid['Both Doses %'] = owid['people_fully_vaccinated']/owid['population']

        owid_covid_sheet = workbook.worksheet_by_title(os.environ["CovidSheet"])
        owid_covid_sheet.clear()

        ### Push it to Google Sheets
        set_worksheet_todf(owid_covid_sheet, owid)

    if 'opps' in RUN_THESE:
        print('# Updating Opp Portal Sheet #')
        opp_df, backgrnds_df = opp.get()
        opp_sheet = workbook.worksheet_by_title(os.environ["OppsSheet"])
        backgrounds_sheet = workbook.worksheet_by_title(os.environ["ListofThings"])
        ### Push it to Google Sheets
        set_opps_worksheet_todf(opp_sheet, opp_df)
        set_list_of_backgrnds_todf(backgrounds_sheet, backgrnds_df)

    if 'numbers' in RUN_THESE:
        print('# Updating Performance Analytics Sheet #')
        perf_df = numerics.get()
        crs_df = numerics.getcrs(perf_df.copy())

        # Combine both dataframes
        cols_to_join_on = ['month', 'mc', 'department']
        whole_df = perf_df.merge(crs_df,
                        on=cols_to_join_on,
                        copy=False)

        # To order the columns and filter them, add this column
        def month_to_num(m):
            date_month = str(months[m]).rjust(2, '0')
            return date_month
        whole_df['month_as_num'] = whole_df['month'].apply(month_to_num)

        perf_sheet = workbook.worksheet_by_title(os.environ["PerformanceSheet"])
        perf_sheet.clear()
        ### Push it to Google Sheets
        set_worksheet_todf(perf_sheet, whole_df)
    if 'nps' in RUN_THESE:

        dates = [{'name':'Fruit', 'value': ('2022-01-01', '2022-06-30')},
                {'name': 'Fish', 'value': ('2022-07-01', '2022-12-31')}
            ]
        
        programs = [
            {'name': 'GTe', 'id': 9},
            {'name': 'GTa', 'id': 8},
            {'name': 'GV', 'id': 7},
        ]
        entity_types = [
            {'dept_prefix': 'i', 'value': 'opportunity'},
            {'dept_prefix': 'o', 'value': 'person'},
            ]

        with open('mea_countries.json') as file:
            countries = json.load(file)['relevant']
        # params = {
        #     'access_token': os.environ['ACCESS_TOKEN'],
        #     'start_date': '2020-12-01',
        #     'end_date': '2022-03-31',
        #     'entity_type': 'opportunity',
        #     'programmes[]': 7,
        #     'entity_id': 1609
        # }
        async def _get_nps(params):
            async with aiohttp.ClientSession() as session:
                async with session.get('https://analytics.api.aiesec.org/v2/nps/analytics.json', params=params) as resp:
                    res = await resp.json()
            res = res['analytics']
            try:
                nps = (res['total_promoters']['doc_count'] - res['total_detractors']['doc_count'])/res['total_responses']['doc_count']*100
            except ZeroDivisionError:
                nps = 0

                # NPS (float), NPS (Int), # of Responses
            return nps, int(round(nps)), res['total_responses']['doc_count']

        async def _get_response_asrow(params, peak, mc, dept_prefix, program):
            nps_ft, nps_int, num_responses = await _get_nps(params)
            return [
                peak, mc, dept_prefix+program, nps_ft, nps_int, num_responses
            ]


        async def get_nps():
            count = 0
            funcs = []
            for peak in dates:
                for e_typ in entity_types:
                    for prog in programs:
                        for mc in countries:
                            params = {
                                'access_token': os.environ['ACCESS_TOKEN'],
                                'start_date': peak['value'][0],
                                'end_date': peak['value'][1],
                                'entity_type': e_typ['value'],
                                'programmes[]': prog['id'],
                                'entity_id': mc['id']
                            }
                            count+=1
                            # Peak 2022, MC, dept, NPS Score
                            funcs.append(_get_response_asrow(params,
                                                        peak['name'],
                                                        mc['name'],
                                                        e_typ['dept_prefix'],
                                                        prog['name']))
            print(f"Processing requests...")
            nps_data = await asyncio.gather(
                *funcs
            )
            nps_data_array = np.array(nps_data)
            nps_df = pd.DataFrame(nps_data_array, columns=['Peak',
                                                    'MC',
                                                    'Program',
                                                    'NPS Score',
                                                    'NPS Score (Rounded)',
                                                    '# of Responses'])
            return nps_df
        start = time.perf_counter()
        nps_df = asyncio.run(get_nps())
        print(nps_df)
        nps_sheet = workbook.worksheet_by_title(os.environ["NPSSheet"])
        nps_sheet.clear()
        ### Push it to Google Sheets
        set_worksheet_todf(nps_sheet, nps_df)
        print(f'Time Taken for all NPS requests: {time.perf_counter()-start:0.4f} seconds')


    status_sheet = workbook.worksheet_by_title(os.environ["StatusUpdateSheet"])
    status_sheet.clear()
    status_df = pd.DataFrame(data={
            'Last Update': [datetime.now().isoformat() + ' UTC']
            })
    set_worksheet_todf(status_sheet, status_df)
    print("Done!")

if __name__ == "__main__":
    main()

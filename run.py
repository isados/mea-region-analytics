#!/usr/bin/env python3

import os
import tempfile
import numerics
from numerics import months
import opp
import pygsheets
import pandas as pd
from functools import partial
from utils import write_base64str_obj_to_file
from datetime import datetime

# numbers, covid, opps
RUN_THESE = ['opps', 'covid', 'numbers']
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
        opp_sheet.clear()
        ### Push it to Google Sheets
        set_worksheet_todf(opp_sheet, opp_df)
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

    status_sheet = workbook.worksheet_by_title(os.environ["StatusUpdateSheet"])
    status_sheet.clear()
    status_df = pd.DataFrame(data={
            'Last Update': [datetime.now().isoformat() + ' UTC']
            })
    set_worksheet_todf(status_sheet, status_df)
    print("Done!")

if __name__ == "__main__":
    main()

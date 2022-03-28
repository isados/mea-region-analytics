#!/usr/bin/env python3

import os
import tempfile
import numerics
import opp
import pygsheets
import pandas as pd
from functools import partial
from utils import write_base64str_obj_to_file

# numbers, covid, opps
RUN_THESE = ['opps']
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
        perf_sheet = workbook.worksheet_by_title(os.environ["PerformanceSheet"])
        crs_sheet = workbook.worksheet_by_title(os.environ["ConversionsSheet"])
        perf_sheet.clear()
        crs_sheet.clear()
        ### Push it to Google Sheets
        set_worksheet_todf(perf_sheet, perf_df)
        set_worksheet_todf(crs_sheet, crs_df)

    print("Done!")

if __name__ == "__main__":
    main()

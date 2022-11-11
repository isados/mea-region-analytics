#!/usr/bin/env python3

import os
import tempfile
from pprint import pprint
from pytest import param
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
    set_worksheet_todf = partial(pygsheets.Worksheet.set_dataframe, start="A1", copy_head=True, extend=True)


    status_sheet = workbook.worksheet_by_title(os.environ["StatusUpdateSheet"])
    status_sheet.clear()
    status_df = pd.DataFrame(data={
            'Last Update': [datetime.now().isoformat() + ' UTC']
            })
    set_worksheet_todf(status_sheet, status_df)
    print("Done!")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3

import os
import tempfile
import pygsheets
import pandas as pd
import asyncio
from functools import partial
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from utils import write_base64str_obj_to_file


def main():
    # Select your transport with a defined url endpoint
    # access_token = os.environ['access_token']
    # transport = aiohttptransport(url=f"https://gis-api.aiesec.org/graphql/?access_token={access_token}")

    # async def getData():
    #     # Create a GraphQL client using the defined transport
    #     async with Client(transport=transport, fetch_schema_from_transport=True) as session:

    #         # Provide a GraphQL query
    #         applications_query = gql(
    #             """
    #             query getApplicationList ($limit: Int, $start_date: DateTime, $end_date: DateTime){
    #             allOpportunityApplication(per_page: $limit, filters: {created_at: {from: $start_date, to: $end_date}}) {
    #                 data {
    #                 id
    #                 status
    #                 created_at
    #                 date_matched
    #                 date_pay_by_cash
    #                 date_approved
    #                 date_realized
    #                 experience_start_date
    #                 experience_end_date
    #                 date_approval_broken
    #                 nps_response_completed_at
    #                 updated_at
    #                 person {
    #                     id
    #                     full_name
    #                     contact_detail {
    #                         email
    #                         phone
    #                     }
    #                     home_mc {
    #                     name
    #                     }
    #                     home_lc {
    #                     name
    #                     }
    #                 }
    #                 host_lc {
    #                     name
    #                 }
    #                 host_mc: home_mc {
    #                     name
    #                 }
    #                 opportunity {
    #                     id
    #                     created_at
    #                     title
    #                     duration
    #                     sub_product {
    #                     name
    #                     }
    #                     programme {
    #                     short_name_display
    #                     }
    #                 }
    #                 standards {
    #                     option
    #                 }
    #                 }
    #             }
    #             }
    #         """
    #         )

    #         opps_query = gql(
    #             """
    #             query getOpportunitiesList ($limit: Int){
    #                 opportunities(pagination: {per_page: $limit},
    #                                             filters: {status: "open",
    #                                                 date_opened:{from: "2021-01-01"} ,
    #                                                 sort: date_opened,
    #                                                 sort_direction:asc}) {
    #                         data {
    #                         ID: id
    #                         Opening_Date: date_opened
    #                         sdg_info {
    #                             sdg_target {
    #                                 goal_index
    #                                 target_index
    #                             }
    #                         }
    #                         description
    #                         is_global_project
    #                         programme {
    #                                 short_name_display
    #                                 }
    #                         all_slots {
    #                             nodes {
    #                                 available_openings
    #                                 start_date
    #                                 end_date
    #                             }
    #                         }
    #                         applicants_count
    #                         view_count
    #                         host_lc {name}
    #                         host_mc: home_mc {name}
    #                 }
    #             }
    #             }
    #             """
    #         )
    #         opps_query_params = {'limit': 1001}
    #         params = {	
    #                     "start_date": "2021-01-01",
    #                     "end_date": "",
    #                     "limit": 1000 # Could be any large enough number
    #                 }

    #         # Execute the query on the transport
    #         results = await session.execute(opps_query, variable_values=opps_query_params)
    #         return results

    print("Executing query off of EXPA ...")
    # apps_data = asyncio.run(getData()) 
    # opps_data = asyncio.run(getData()) 

    print("Started preprocessing...")
    # Reduce the dict by 3 Levels
    # opps_data = opps_data['opportunities']['data']

    #  Flatten dictionary and compress keys
    # opps_df = pd.json_normalize(opps_data, sep='_')

    """
    Create new columns for easy comprehension
        * LC
        * Department
        * Partner_MC
        * Partner_LC
    """

    # new_fields = ['department', 'lc', 'partner_mc', 'partner_lc']
    # def generate_new_fields(row):
    #     if row['person_home_mc_name'] == 'Bahrain':
    #         values = ['o' + row['opportunity_programme_short_name_display'],
    #                    row['person_home_lc_name'],
    #                    row['host_mc_name'], 
    #                    row['host_lc_name']
    #                  ]
    #     else:
    #         values = ['i' + row['opportunity_programme_short_name_display'],
    #                   row['host_lc_name'],
    #                   row['person_home_mc_name'],
    #                   row['person_home_lc_name']
    #                  ]
    #     return dict(zip(new_fields, values))

    # print("Generating new fields and tables ...")
    # apps_df[new_fields] = apps_df.apply(lambda row: generate_new_fields(row), axis=1, result_type='expand')

    # pointless_cols = ['opportunity_programme_short_name_display', 'host_mc_name', 'host_lc_name', 'person_home_mc_name', 'person_home_lc_name']
    # apps_df.drop(pointless_cols, inplace=True, axis=1)

    """
    Produce Performance Analytics Table
        * First convert dates from longform to YYYY-MM-DD
        * Retain Date, LC, Dept, PartnerMC, PartnerLC, and the Status Column like # of Applications, Accepted etc.. will be the aggregation
    """

    # date_cols = ['created_at', 'date_matched', 'date_approved', 'date_realized', 'updated_at']
    # multi_indices = ['lc', 'department', 'partner_mc', 'partner_lc']
    # aggregration_fields = ['id', 'person_id']

    # Generate table with these columns only
    # perf_table = apps_df[aggregration_fields + date_cols + multi_indices].copy()

    # Ensure that dates are uniform and shortened
    # perf_table.loc[:,date_cols] = apps_df[date_cols].applymap(lambda x: x[:-10], na_action='ignore')

    # def get_timeseries_formetric(table: pd.DataFrame, other_fields: list, selected_date_col: str, metric_name: str) -> pd.DataFrame: table = table[[selected_date_col, *other_fields, *aggregration_fields]]
    #     _ = table.sort_values([selected_date_col, *other_fields])
    #     _[metric_name] = 1
    #     _.rename(columns={selected_date_col: "date", 
    #                     "id": "AppID", 
    #                     "person_id": "PersonID"}, inplace=True)
    #     return _.dropna(axis=0)

    # apps_per_day = get_timeseries_formetric(perf_table, multi_indices, "created_at", "Applied")
    # acc_per_day = get_timeseries_formetric(perf_table, multi_indices, "date_matched", "Accepted")
    # apd_per_day = get_timeseries_formetric(perf_table, multi_indices, "date_approved", "Approved")

    # perf_analysis_df = pd.concat([apps_per_day, acc_per_day, apd_per_day])
    # perf_analysis_df.fillna(0, inplace=True, axis=0)

    # ### Push it to Google Sheets

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

    # Credentials from service account file for Google Sheets
    print("Creating temporary file for service account credentials...")
    # opps_df = opps_df[~opps_df.all_slots_nodes.isin([[]])]

    temp = tempfile.NamedTemporaryFile()
    try:
        access_creds = os.environ['GOOGLE_CREDS']
        write_base64str_obj_to_file(access_creds, temp.name)
    finally:
        gc = pygsheets.authorize(service_file=temp.name)
        temp.close()


    # print("Writing to Google Sheets...")
    workbook = gc.open_by_key(os.environ["SPREADSHEET_ID"])

    # # perf_worksheet = workbook.worksheet_by_title(os.environ["PerformanceSheet"])
    # # applications_worksheet = workbook.worksheet_by_title(os.environ["ApplicationsSheet"])
    owid_covid_sheet = workbook.worksheet_by_title(os.environ["CovidSheet"])
    owid_covid_sheet.clear()
    # opps_worksheet = workbook.worksheet_by_title(os.environ["OppsSheet"])
    # opps_worksheet.clear()

    # # Create handy function to write to sheets
    set_worksheet_todf = partial(pygsheets.Worksheet.set_dataframe, start="A1", copy_head=True)

    # set_worksheet_todf(perf_worksheet, perf_analysis_df)
    # set_worksheet_todf(applications_worksheet, apps_df)
    # set_worksheet_todf(opps_worksheet, opps_df)
    set_worksheet_todf(owid_covid_sheet, owid)
    print("Done!")

if __name__ == "__main__":
    main()

import os
import asyncio
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

def read_gql_fromfile(name, folder="query"):
    """Return GraphQL query from file"""
    path = os.path.join(folder, name)
    with open(path, 'r') as file:
        query = gql(file.read())
    return query

async def _run_query(query, params):
    # Create a GraphQL client 
    access_token = '00afef60b26308fa2c58de8fa0831984db710a8302a336c293bbd6202e6cf0ac' 
    transport = AIOHTTPTransport(url=f"https://gis-api.aiesec.org/graphql/?access_token={access_token}")
    async with Client(transport=transport, fetch_schema_from_transport=True) as session:
        # Execute query
        # results = await session.execute(query, variable_values=params)
        try:
            results = await session.execute(query)
        except asyncio.exceptions.TimeoutError:
            print('Oops! Timeout with the server. Will try again...')
            results = await session.execute(query)
        return results

def run_query(query, params=None):
    return asyncio.run(_run_query(gql(query), params))

async def pull_data(query_file, params, ):
    # access_token = os.environ['ACCESS_TOKEN']
    requested_data = []
    query = read_gql_fromfile(query_file)

    try:
        data = await _run_query(query, params)
        print('Done with first query')
    except asyncio.exceptions.TimeoutError as e:
        print('Reduce the limit parameter!')
        raise(e)

    print("Started preprocessing...")
    # Reduce the dict by 3 Levels
    for value in data.values():
        result_dfs = [value['data']]
        pages = value['paging']['total_pages']
        print(pages)
        if pages > 1:
            n_page_requests = pages - 1
            requests = []
            for page in range(2, n_page_requests+2,):
                params['page'] = page
                requests.append(_run_query(query, params))
            result_dfs += await asyncio.gather(*requests)
        break

    #  Flatten dictionary and compress keys
    # result_df = pd.json_normalize(result_dfs, sep='_')
    return result_dfs


def test():

    apps_params = {	
                "start_date": "2022-01-01",
                "end_date": "",
                "limit": 200, # Could be any large enough number
                "page": 1, # Always do first page, increase it later
	"mc_ids": [
		1632,
		457,
		518,
		459,
		1584,
		180,
		29,
		1581,
		1537,
		1609,
		476,
		499,
		1568,
		530,
		1617,
		2106,
		56,
		182,
		219,
		2428,
		1709,
		2442,
		489,
		1552,
		1574,
		78,
		2117,
		1578,
		529,
		506,
		1605,
		1840,
		2420,
		1545,
		477,
		2418,
		567,
		1543,
		1559,
		1602,
		1625,
		2122,
		2417
	]
}

    asyncio.run(pull_data('applications.gql', apps_params))
    print("Done!")

if __name__ == '__main__':
    test()
import requests
import dlt


# a source is a container that logically groups resources
# Example sources: REST api with many endpoints, database with many tables or google sheet with many tabs
@dlt.source
def twitter(api_url=dlt.config.value, twitter_bearer_token=dlt.secrets.value, last_id=0):
    # example of Bearer Authentication
    # create authorization headers
    return twitter_resource(twitter_bearer_token)


def _headers(twitter_bearer_token):
    headers = {
        "Authorization": f"Bearer {twitter_bearer_token}"
    }
    return headers


# A resource is a function that produces data and yields it
# Example resources: API endpoint, a database table, a file, or a sheet from a gsheets workbook...
@dlt.resource(write_disposition="append")
def twitter_resource(api_url=dlt.config.value, twitter_bearer_token=dlt.secrets.value):
    headers = _headers(twitter_bearer_token)

    # check if authentication headers look fine
    print(headers)

    # make an api call here
    # response = requests.get(url, headers=headers, params=params)
    # response.raise_for_status()
    # yield response.json()

    # test data for loading validation, delete it once you yield actual data
    test_data = [{'id': 0}, {'id': 1}]
    yield test_data


if __name__=='__main__':
    # print credentials by running the resource
    data = list(twitter_resource())

    # print responses from resource
    print(data)

    # run pipeline
    # configure the pipeline with your destination details
    # p = dlt.pipeline(destination="bigquery", dataset_name="twitter")
    # run the pipeline with your parameters
    # load_info = p.run(twitter(dlt.config.value, dlt.secrets.value, last_id=819273998))

    # pretty print the information on data that was loaded
    # print(load_info)



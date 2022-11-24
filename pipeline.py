import requests
import dlt


def _create_auth_headers(api_key):
    # example of Bearer Authentication
    # create authorization headers
    headers = {
        "Authorization": f"Bearer {api_key}"
    }
    return headers

# explain the `dlt.resource` and the default table naming, write disposition etc.
@dlt.resource
def example_resource(api_url=dlt.config.value, api_key=dlt.secrets.value, last_id = 0):
    headers = _create_auth_headers(api_key)

    # uncomment line below to see if your headers are correct (ie. include valid api_key)
    # print(headers)
    # print(api_url)

    # make a call to the endpoint with request library
    resp = requests.get("%s?last_id=%i" % (api_url, last_id), headers=headers)
    resp.raise_for_status()
    # yield the data from the resource
    data = resp.json()

    # explain that data["items"] contains a list of items
    yield data["items"]


# explain `dlt.source` a little here and last_id and api_key parameters
@dlt.source
def example_source(api_url=dlt.config.value, api_key=dlt.secrets.value, last_id = 0):
    # return all the resources to be loaded
    return example_resource(api_url, api_key, last_id)

if __name__ == '__main__':
    # specify the pipeline name, destination and dataset name when configuring pipeline, otherwise the defaults will be used that are derived from the current script name
    p = dlt.pipeline(pipeline_name="twitter", destination="bigquery", dataset_name="twitter_data", full_refresh=False)

    # uncomment line below to execute the resource function and see the returned data
    # print(list(example_data()))

    # explain that api_key will be automatically loaded from secrets.toml or environment variable below
    load_info = p.run(
        example_source(last_id=819273998)
    )
    #pretty print the information on data that was loaded
    print(load_info)

import requests
import dlt


# explain `dlt.source` a little here and last_id and api_key parameters
@dlt.source
def twitter_data(api_url, api_key, last_id = 0):
    # example of Bearer Authentication
    # create authorization headers
    headers = {
        "Authorization": f"Bearer {api_key}"
    }

    # explain the `dlt.resource` and the default table naming, write disposition etc.
    @dlt.resource
    def example_data():
        # make a call to the endpoint with request library
        resp = requests.get("%s?last_id=%i" % (api_url, last_id), headers=headers)
        resp.raise_for_status()
        # yield the data from the resource
        data = resp.json()
        # you may process the data here
        # example transformation?
        # return resource to be loaded into `example_data` table
        # explain that data["items"] contains a list of items
        yield data["items"]

    # return all the resources to be loaded
    return example_data

if __name__ == '__main__':
    # configure the pipeline
    dlt.pipeline(pipeline_name="twitter", destination="bigquery", dataset="twitter_data")
    # explain that api_key will be automatically loaded from secrets.toml or environment variable below
    load_info = dlt.run(twitter_data(dlt.config.value, dlt.secrets.value, last_id=819273998))
    #pretty print the information on data that was loaded
    print(load_info)

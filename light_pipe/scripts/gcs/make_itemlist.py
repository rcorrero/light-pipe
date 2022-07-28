import os
import requests
import json

PLANET_API_KEY = os.getenv('PL_API_KEY')
GEO_FILTER_PATH = os.getenv('GEO_FILTER_PATH')
ITEM_ID_PATH= os.getenv('ITEM_ID_PATH')
MIN_DATE = os.getenv('MIN_DATE')
MAX_DATE = os.getenv('MAX_DATE')

# Setup Planet Data API base URL
URL = "https://api.planet.com/data/v1"

# Setup the session
session = requests.Session()

# Authenticate
session.auth = (PLANET_API_KEY, "")

# Setup the stats URL
stats_url = "{}/stats".format(URL)

# Specify the sensors/satellites or "item types" to include in our results
item_types = ["PSScene4Band"]

cloud_filter = {
  "type": "RangeFilter",
  "field_name": "cloud_cover",
  "config": {
    "lte": 0.1
  }
}

date_filter = {
    "type": "DateRangeFilter", 
    "field_name": "acquired", 
    "config": {
        "gte": MIN_DATE,
        "lte": MAX_DATE 
    }
}

permission_filter = {
  "type": "PermissionFilter",
  "config": ["assets.analytic:download"]
}


if __name__ == '__main__':
    # Make a GET request to the Planet Data API
    test_res = session.get(URL)
    # Response status code
    print("Test response ok:", test_res.ok)
    
    with open(GEO_FILTER_PATH) as f:
        aoi = json.load(f)['features'][0]['geometry']

    geo_filter = {
        "type": "GeometryFilter",
        "field_name": "geometry",
        "config": aoi
    }

    # Setup an "AND" logical filter
    and_filter = {
        "type": "AndFilter",
        "config": [cloud_filter, geo_filter, date_filter, permission_filter]
    }

    # Setup the quick search endpoint url
    quick_url = "{}/quick-search".format(URL)
    

    # Setup the request
    request = {
        "item_types" : item_types,
        "filter" : and_filter
    }

    # Send the POST request to the API stats endpoint
    res = session.post(quick_url, json=request)
    response = res.json()
    features = response["features"]

    # Get the number of features present in the response
    print("Number of items matching criteria:", len(features))

    print("Creating file containing ids...")
    with open(ITEM_ID_PATH, 'w') as f:
      # Loop over all the features from the response
      for item in features:
          f.write(item["id"] + '\n')
      # Iterate over response pages
      idx = 2
      total_num_items = len(features)
      while response["_links"]["_next"] is not None:
        response = session.get(response["_links"]["_next"]).json()
        features = response["features"]

        # Get the number of features present in the response
        print('On page number %s...' % idx)
        print("Number of items matching criteria:", len(features))
        # Loop over all the features from the response
        for item in features:  
            f.write(item["id"] + '\n')
        total_num_items += len(features)
        idx += 1
    print('Total number of items matching criteria: ', total_num_items)
    print('Done')

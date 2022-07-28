import json
import time

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth


def make_param_dict(param_csv_path: str) -> dict:
    param_df_list = pd.read_csv(param_csv_path).to_dict('records')
    param_dict = {}
    for row_dict in param_df_list:
        param_name = row_dict['param_name']
        value = row_dict['value']
        param_dict[param_name] = value

    param_dict['single_archive'] = param_dict['single_archive'] == 'TRUE'
    param_dict['email'] = param_dict['email'] == 'TRUE'
    param_dict['max_num_samples'] = int(param_dict['max_num_samples'])

    param_dict['orders_url'] = 'https://api.planet.com/compute/ops/orders/v2'
    param_dict['auth'] = HTTPBasicAuth(param_dict['planet_api_key'], '')
    param_dict['headers'] = {'content-type': 'application/json'}
    param_dict['subscription_id'] = 0

    with open(param_dict['item_id_path']) as f:
        item_ids = f.read().splitlines()
        if len(item_ids) > param_dict['max_num_samples']:
            item_ids = item_ids[:param_dict['max_num_samples']]

    param_dict['item_ids'] = item_ids

    with open(param_dict['credentials']) as f:
        param_dict['credentials_str'] = f.read()

    return param_dict


def make_request(param_dict: dict) -> dict:
    request = {
        "name": param_dict['order_name'],
        "subscription_id": param_dict['subscription_id'],
        "products": [
        {
            "item_ids": param_dict['item_ids'],
            "item_type": param_dict['item_type'],
            "product_bundle": param_dict['product_bundle']
        }
        ],
        "delivery": {
        "single_archive": param_dict['single_archive'],
        #"archive_filename": archive_filename,
        "google_cloud_storage": {
            "bucket": param_dict['bucket'],
            "credentials": param_dict['credentials_str'],
            "path_prefix": param_dict['path_prefix']
        }
        },
        "notifications": {
        "email": param_dict['email']
        },
        "order_type": "full"
    }
    return request


def place_order(request, param_dict):
    response = requests.post(param_dict['orders_url'], 
        data=json.dumps(request), auth=param_dict['auth'], headers=param_dict['headers'])
    print("Response ok? ", response.ok)
    assert response.ok, 'Request failed. Response: ' + str(response.json())
    order_id = response.json()['id']
    print("Order id: ", order_id)
    order_url = param_dict['orders_url'] + '/' + order_id
    return order_url


def poll_for_success(order_url, param_dict, num_loops=250):
    print('Order status: ')
    count = 0
    while(count < num_loops):
        count += 1
        r = requests.get(order_url, auth=param_dict['auth'])
        response = r.json()
        state = response['state']
        print(state)
        end_states = ['success', 'failed', 'partial']
        if state in end_states:
            assert state == 'success', 'Request failed. Response: ' + str(response)
            break
        time.sleep(25)


def download_items(param_csv_path: str) -> None:
    param_dict = make_param_dict(param_csv_path)
    request = make_request(param_dict)
    order_url = place_order(request, param_dict)
    poll_for_success(order_url, param_dict)


if __name__ == '__main__':
    param_csv_path = input('Path to csv containing order params: ')
    #param_csv_path = '../../data/planet_scope_2/private/order_params.csv'
    download_items(param_csv_path)
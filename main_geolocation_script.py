import binascii
import requests
import json
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from datetime import date
from itertools import islice
import time
from get_here_token import get_token
from config import db_uri, keys


db = create_engine(db_uri)
urls = {'google': 'https://www.googleapis.com/geolocation/v1/geolocate', 'combain': 'https://apiv2.combain.com',
        'here_old': 'https://positioning.hereapi.com', 'here': 'https://pos.api.here.com/positioning',
        'opencell': 'http://opencellid.org'}

def decode_cellid(data: list) -> list:
    if len(data) > 0:
        full_list: list = []
        for uli in data:
            cell_id = uli['cell_id']
            if len(cell_id.strip()) >= 16: ##length of cell id has to be equal or greater than 16 chars
                location: dict = {}
                binary_string = binascii.unhexlify(cell_id) ##parse the cell id
                location['mcc'] = uli['mcc']
                location['mnc'] = uli['mnc']

                first_byte = binary_string[0]
                if first_byte == 130 or len(cell_id) == 26:
                    ci_hex = cell_id[19:]
                    location['lac'] = None
                    location['radiotype'] = "lte"
                elif first_byte == 1 or len(cell_id) == 16:
                    lac_hex = cell_id[8:12]
                    location['lac'] = int(lac_hex, base=16)
                    ci_hex = cell_id[12:]
                    location['radiotype'] = 'gsm'
                location['ci'] = int(ci_hex, base=16)
                location['cell_id'] = cell_id
                full_list.append(location)
        return full_list

def get_coordinates(api: str, details: list, path: str = 'cell_id.csv'):
    from token_file import token
    location_api: str = urls.get(api)
    if api == 'here':
        # url: str = f'{location_api}/v2/locate'
        url: str = f'{location_api}/v1/locate'
    else:
        url: str = f'{location_api}?{keys.get(api)}'
    all_data: list = []
    total: int = 0
    success: int = 0
    here_token = token

    for info in details:
        data = get_data_structure(api, info)
        headers = {'Content-Type': 'application/json'}
        if api == 'here':
            headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {here_token}'}

        if api == 'opencell':
           opencell_return = opencell(location_api, data)
           if opencell_return is not None:
               info['latitude'] = opencell_return['lat']
               info['longitude'] = opencell_return['lon']
               info['location_api'] = location_api
               success += 1

        else:
            response = requests.post(url, data=data, headers=headers)
            if response.status_code == 401:
                response, here_token = generate_here_token(url, data, headers)

            if response.status_code == 200:
                coordinates = json.loads(response.text)
                info['latitude'] = coordinates['location']['lat']
                info['longitude'] = coordinates['location']['lng']
                info['accuracy'] = coordinates.get('accuracy') or coordinates['location']['accuracy']
                info['location_api'] = location_api
                success += 1

            else:
                info['latitude'] = '0.0'
                info['longitude'] = '0.0'
                info['accuracy'] = '0.0'
        info['location_api'] = location_api
        info['date_updated'] = date.today()
        all_data.append(info)
        total += 1
    print(f'{success}/{total} results successfully retrieved')
    df = pd.DataFrame(all_data, columns=['mnc', 'mcc', 'lac', 'ci', 'cell_id', 'radiotype', 'latitude', 'longitude',
                                         'location_api', 'accuracy', 'date_updated'])
    df.to_csv(path, index=False)
    return all_data

def opencell(location_api, data):
    url: str = f'{location_api}/cell/get'
    headers = {'Content-Type': 'application/json;charset=UTF-8'}
    response = requests.get(url, params=data, headers=headers)
    coordinates = json.loads(response.text)
    if 'error' in coordinates.keys():
        return None
    else:
        return coordinates


def generate_here_token(url, data, headers):
    get_token()
    from token_file import token
    headers['Authorization'] = f'Bearer {token}'
    response = requests.post(url, data=data, headers=headers)
    return response, token


def get_data_structure(api: str, info:dict) -> json:
    data = None
    if api == 'here':
        if info.get('radiotype') == 'gsm':
            data = json.dumps(
                {
                    "gsm": [
                        {
                            "mcc": info.get('mcc'),
                            "mnc": info.get('mnc'),
                            "lac": info.get('lac'),
                            "cid": info.get('ci')
                        }
                    ]
                }
            )
        elif info.get('radiotype') == 'lte':
            data = json.dumps(
                {
                    "lte": [
                        {
                            "mcc": info.get('mcc'),
                            "mnc": info.get('mnc'),
                            "cid": info.get('ci')
                        }
                    ]
                }
            )
    elif api == 'opencell':
        data = {
                "key": keys.get(api),
                "mcc": info.get('mcc'),
                "mnc": info.get('mnc'),
                "lac": info.get('lac'),
                "cellid": info.get('ci'),
                "radio": info.get('radiotype').upper(),
                "format": "json",
            }
    else:
        data = json.dumps(
            {
                "considerIp": "false",
                "radioType": info.get('radiotype'),
                "cellTowers": [
                    {
                        "cellId": info.get('ci'),
                        "locationAreaCode": info.get('lac'),
                        "mobileCountryCode": info.get('mcc'),
                        "mobileNetworkCode": info.get('mnc')
                    }
                ]
            }
        )
    return data

def to_db(data: list, db=db, db_name='cellid_location_opencell'):
    df = pd.DataFrame(data, columns=['mnc', 'mcc', 'lac', 'ci', 'cell_id', 'radiotype', 'latitude', 'longitude',
                                     'location_api', 'accuracy', 'date_updated'])
    df.to_sql(db_name, db, if_exists='append', index=False)
    return None

def from_csv(path, delimiter=None, *args):
    header = ['type', 'mcc', 'mnc', 'cell_id', 'country']
    df = pd.read_csv(path, names=header, delimiter=delimiter, *args)
    new_df = df[['mcc', 'mnc', 'cell_id']]
    cell_id_list = new_df.to_dict('records')
    return cell_id_list

def from_db(query, batch_size, start, db=db):
    session = Session(db)
    results = session.execute(query).all()
    results_dict = iter([{'mnc': d[0], 'mcc': d[1], 'cell_id': d[2]} for d in results[start:]])
    while batch := list(islice(results_dict, batch_size)):
        yield batch
    return None

def run_batch(api:str, limit = None, start:int = 0, step: int = 100, query:str = None):
    #here.mnc, here.mcc, here.cell_id, here.radiotype,
    """query = f
        SELECT  google.mnc, google.mcc, google.cell_id
        FROM cellid_location_google google
        WHERE google.radiotype = 'lte'
        LIMIT {limit}
        """
    # query_default = f"""
    # SELECT mnc, mcc, cell_id FROM geo.cellid_location where location_api != 'https://apiv2.combain.com'
    # """
    query_default = f"""
     SELECT DISTINCT aff.mnc, aff.mcc, aff.cell_id, open.cell_id as open_cell_id FROM geolocation.agg_cdr_affluences aff
    LEFT JOIN geolocation.cellid_location_opencell open ON aff.cell_id = open.cell_id
    AND aff.mnc = open.mnc AND aff.mcc = open.mcc
    WHERE open.cell_id = ''
    """

    final_query = query_default if query is None else query
    start_time = time.time()
    lap_time = start_time
    results = from_db(final_query, batch_size=step, start=start)
    counter = start
    for data in results:
        decoded = decode_cellid(data)
        coordinates_db = get_coordinates(api, decoded, 'csvs/result_opencell_main.csv')
        to_db(coordinates_db)
        counter += len(data)
        if not limit:
            print(f'Processed {counter} - took {(time.time()-lap_time): .2f}secs')
        else:
            print(f'Processed {counter}/{limit} - {float(counter/limit)}% done - took {(time.time()-lap_time): .2f}secs')
        lap_time = time.time()
    print(f'Total time to process {counter} records = {(time.time()-start_time): .2f} secs')
    return True


if __name__ == '__main__':
    run_batch(api='opencell', start=0, step=20)

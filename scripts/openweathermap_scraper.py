
import pandas as pd, os, requests, json, time
from datetime import datetime

API_key = os.getenv('weather-api')
lat, lon = [47.61191234063561, -122.33851982527909]
units = 'imperial'
parent_f_path = 'Transport-Weather/scraped_data'
csv_path = parent_f_path + '/weather.csv'

###loop to get data from start time to end time, not the most efficient but it works for now 
weather_data = pd.DataFrame()
CHECKPOINT = parent_f_path + '/weather_checkpoint_full.pkl'

###check if there is a more recent checkpoint to start off from 
if os.path.exists(CHECKPOINT):
    records = pd.read_pickle(CHECKPOINT).to_dict('records')
    start = (max(rec['dt'] for rec in records) + 1 ).__int__()
    print(f'Resuming from {datetime.fromtimestamp(start)} - {len(records)} hours of data already saved.')
else:
    records = []
    start = datetime(year=1980, month=1, day=1, hour=1).timestamp().__int__()

###set end to start of already scraped csv so we can create the full data 
end = (pd.read_csv(csv_path).dt.astype(int)).min()



###initialization
weather_url = f'https://api.openweathermap.org/data/4.0/onecall/timeline/1h?lat={lat}&lon={lon}&units={units}&start={start}&appid={API_key}'
calls = 0
max_calls = 2200
session = requests.Session()

try:
    while weather_url and start < end:

        print('Data is within range. Requesting...')
        resp = session.get(weather_url, timeout=30)
        resp.raise_for_status()
        r = resp.json()

    ##if no response, break 
        batch = r.get('data', [])
        if not batch:   
            break
        
        records.extend(batch)
        calls += 1

        ###ends when date end is reached 
        if batch[-1]['dt'] >= end:
            break 

        ###save every 20 calls just in case 
        if calls % 20 == 0:
            pd.DataFrame(records).to_pickle(CHECKPOINT)
        data_time = datetime.fromtimestamp(r['data'][0]['dt'])

        ###break at 1000 calls - free daily limit  
        if calls == max_calls:
            break

        print(f'Data Saved for {data_time} - Call: {calls}')

        weather_url = r.get('next')
        time.sleep(1)
except requests.HTTPError:
    print('\nReached max calls for the day! Try again tomorrow.\n')
finally:
    pd.DataFrame(records).to_pickle(CHECKPOINT)
    with open(parent_f_path + '/calls.txt','wb') as f:
        f.write('Date Scraped: ', datetime.today().date, 'Total Calls: ', calls )
    print(f'{len(records)} hourly records saved this run.')

    
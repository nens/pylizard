import pyproj
import pandas as pd
from .tools import lizard_timeseries, lizard_api_collection


def create_headers(api_key):
    if api_key!=None:
        headers = {
                "username": "{}".format("__key__"),
                "password": "{key}",
                "Content-Type": "application/json",
            }    
    else:
        headers = None
    
    return(headers)

def get_meting(filter_code, timeseries, observation_type_code):
    divermeting = timeseries[(timeseries['location_code']==filter_code)&
                             (timeseries['observation_type_code']==observation_type_code)]
    if len(divermeting['uuid']) == 0:
        return('')
    else:
        return(divermeting['uuid'].values[0])

def format_groundwaterstation_data(groundwaterstation_data, meta, index, timeseries_list):
    
    p_rd =  pyproj.Proj("+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +towgs84=565.237,50.0087,465.658,-0.406857,0.350733,-1.87035,4.0812 +units=m +no_defs")
    p_wgs = pyproj.Proj(proj='latlong',datum='WGS84')
    
    for i in range(len(groundwaterstation_data)):
        
        #break
        buis = groundwaterstation_data['code'][i]
        coordinates = groundwaterstation_data['geometry'][i]['coordinates']
        lon, lat = coordinates[:2]
        surface_level = groundwaterstation_data['surface_level'][i]
        x, y = pyproj.transform(p_wgs, p_rd, lon, lat)
        for filter in groundwaterstation_data['filters'][i]:
            #break
            filter_code = filter['code']
            filter_code = filter_code.replace('-','')
            filter_number = int(filter_code[-3:])
            bkf = filter['filter_top_level']
            okf = filter['filter_bottom_level']
            
            if filter['timeseries']!=[]:
                for TIMESERIE_URL in filter['timeseries']:
                    timeseries_list.append({'filter':filter_code, 'uuid':TIMESERIE_URL.split('/')[-2]})


            meta.append([buis, filter_number, x, y, lat, lon, surface_level, bkf, okf]) #uuid_hand, uuid_diver
            index.append(filter_code)    
            
    return(meta, index, timeseries_list)

def get_groundwaterstation(code, api_key=None, report=False, LIZARD_URL='https://vitens.lizard.net/api/v4/', proxydict={}):
    headers = create_headers(api_key)

    LIZARD_GW_ENDPOINT = f'{LIZARD_URL}groundwaterstations/'
    LIZARD_TS_ENDPOINT = f'{LIZARD_URL}timeseries/'

    meta = []
    index = []
    timeseries_list = []

    GROUNDWATERSTATIONS_URL = f'{LIZARD_GW_ENDPOINT}?code={code}'

    if GROUNDWATERSTATIONS_URL!=None:
        if report:
            print('GET', GROUNDWATERSTATIONS_URL)
        
        groundwaterstation_data = lizard_api_collection(GROUNDWATERSTATIONS_URL, headers, page_size=10)
        groundwaterstation_data.get()
        groundwaterstation_data = groundwaterstation_data.results
        
        
        meta, index, timeseries_list = format_groundwaterstation_data(groundwaterstation_data, meta, index, timeseries_list)
        
    df = pd.DataFrame(meta,
                            columns=['buis', 'filter_number', 'x', 'y', 'lat', 'lon', 'surface_level', 'bkf', 'okf'],
                            index=index)
    
    timeseries_list = pd.DataFrame(timeseries_list)
    timeseries_list_str = ''.join([f'{ts},' for ts in list(timeseries_list['uuid'].values)])[:-1]
    TIMESERIES_URL = f'{LIZARD_TS_ENDPOINT}?uuid__in={timeseries_list_str}&observation_type__code__startswith=WNS9040'
    
    timeseries = lizard_api_collection(TIMESERIES_URL, headers, page_size=100)
    timeseries.get()
    timeseries = timeseries.results    
    timeseries['location_code']=timeseries['location'].apply(lambda x: x['code'])
    timeseries['observation_type_code']=timeseries['observation_type'].apply(lambda x: x['code'])
    
    df['code']=df.index
    df['uuid_hand']=df['code'].apply(lambda x: get_meting(x, timeseries, 'WNS9040.hand'))
    df['uuid_diver']=df['code'].apply(lambda x: get_meting(x, timeseries, 'WNS9040'))
    df = df.drop(['code'],axis=1)
    df['bkf']=df['surface_level']-df['bkf']
    df['okf']=df['surface_level']-df['okf']
    
    return df    

def polygon_to_groundwaterstations(polygon, api_key=None, report=False, LIZARD_URL='https://vitens.lizard.net/api/v4/', proxydict={}):
        
    headers = create_headers(api_key)

    LIZARD_GW_ENDPOINT = f'{LIZARD_URL}groundwaterstations/'
    LIZARD_TS_ENDPOINT = f'{LIZARD_URL}timeseries/'

    meta = []
    index = []
    timeseries_list = []
    GROUNDWATERSTATIONS_URL = f'{LIZARD_GW_ENDPOINT}?geometry__within={polygon}'

    if GROUNDWATERSTATIONS_URL!=None:
        if report:
            print('GET', GROUNDWATERSTATIONS_URL)
        
        groundwaterstation_data = lizard_api_collection(GROUNDWATERSTATIONS_URL, headers, page_size=10)
        groundwaterstation_data.get()
        groundwaterstation_data = groundwaterstation_data.results
        
        
        meta, index, timeseries_list = format_groundwaterstation_data(groundwaterstation_data, meta, index, timeseries_list)

        
    df = pd.DataFrame(meta,
                            columns=['buis', 'filter_number', 'x', 'y', 'lat', 'lon', 'surface_level', 'bkf', 'okf'],
                            index=index)
    
    timeseries_list = pd.DataFrame(timeseries_list)
    timeseries_list_str = ''.join([f'{ts},' for ts in list(timeseries_list['uuid'].values)])[:-1]
    TIMESERIES_URL = f'{LIZARD_TS_ENDPOINT}?uuid__in={timeseries_list_str}&observation_type__code__startswith=WNS9040'
    
    timeseries = lizard_api_collection(TIMESERIES_URL, headers, page_size=100)
    timeseries.get()
    timeseries = timeseries.results    
    timeseries['location_code']=timeseries['location'].apply(lambda x: x['code'])
    timeseries['observation_type_code']=timeseries['observation_type'].apply(lambda x: x['code'])
    
    df['code']=df.index
    df['uuid_hand']=df['code'].apply(lambda x: get_meting(x, timeseries, 'WNS9040.hand'))
    df['uuid_diver']=df['code'].apply(lambda x: get_meting(x, timeseries, 'WNS9040'))
    df = df.drop(['code'],axis=1)
    df['bkf']=df['surface_level']-df['bkf']
    df['okf']=df['surface_level']-df['okf']
    
    return df

def get_timeseries(uuid, page_size=5000, api_key=None, tmin=None, tmax=None, report=False, proxydict={}):
    timeseries_events = lizard_timeseries(uuid=uuid, base_url="https://vitens.lizard.net",headers=None)
    timeseries_events.get()
    timeseries_events = timeseries_events.results
    if len(timeseries_events)==0:
        timeseries_events = pd.DataFrame(columns = ['time','value','flag','validation_code','comment','detection_limit'])
    timeseries_events['datetime'] = pd.to_datetime(timeseries_events['time'],format='%Y-%m-%dT%H:%M:%SZ')
    timeseries_events.set_index('datetime', inplace=True)
    timeseries_events['head'] = timeseries_events['value']
    timeseries_events = timeseries_events.loc[:, 'head']    
    return timeseries_events


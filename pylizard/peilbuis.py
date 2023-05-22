import requests, pyproj, pandas, matplotlib.pyplot as plt

from .func import get_timeseries, get_groundwaterstation

class Peilbuis:
    def __init__(self, code, filt, api_key=None, report=False, url_lizard='https://vitens.lizard.net/api/v4/', proxydict={}):
        self.report = report
        self.code = code
        self.filt = filt

        lizard_url_loc = url_lizard + 'locations/'
        url_loc = '{}?code__icontains={}{}'.format(lizard_url_loc, code, str(filt).zfill(3) )
        if self.report:
            print('GET', url_loc)
        data = requests.get(url_loc, proxies=proxydict).json()['results']

        lat, lon = data[0]['geometry']['coordinates'][1], data[0]['geometry']['coordinates'][0]

        p_rd =  pyproj.Proj("+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +towgs84=565.237,50.0087,465.658,-0.406857,0.350733,-1.87035,4.0812 +units=m +no_defs")
        p_wgs = pyproj.Proj(proj='latlong',datum='WGS84')

        x, y = pyproj.transform(p_wgs, p_rd, lon, lat)

        p = get_groundwaterstation(code, report=self.report)
        buis = p.loc[(p.loc[:,'buis']==code) & (p.loc[:, 'filter_number']== filt)]

        if len(buis)!=1:
            raise Exception('None or to many filters found')
        self.x = buis['x'][0]
        self.y = buis['y'][0]
        self.lat = buis['lat'][0]
        self.lon = buis['lon'][0]
        self.surface_level = buis['surface_level'][0]
        self.bkf = buis['bkf'][0]
        self.okf = buis['okf'][0]
        self.ref = buis['ref'][0]
        self.uuid_hand  = buis['uuid_hand'][0]
        self.uuid_diver = buis['uuid_diver'][0]

    def __getattr__(self, name):
        if name == 'head_hand':
            self.head_hand = get_timeseries(self.uuid_hand, report=self.report).rename('hand')
            return self.head_hand.sort_index()
        elif name == 'head_diver':
            self.head_diver = get_timeseries(self.uuid_diver, report=self.report).rename('diver')
            return self.head_diver.sort_index()
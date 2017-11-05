import json
import requests
import csv
import math
import numpy as np
import pandas as pd
from math import radians, cos, sin, asin, sqrt

def index(lat,lon):
    myfile = csv.reader(open("combined.csv"))
    df= {}
    firstLine = True
    for lines in myfile:
        if firstLine:
            firstLine = False
            continue
        else:
            df[lines[1]]=lines[2:4]
    df

    from functools import partial

    lati = lat
    longi = lon

    def haversine_np(longi, lati, lon2, lat2):
        longi, lati, lon2, lat2 = map(np.radians, [longi, lati, lon2, lat2])

        dlon = lon2 - longi
        dlat = lat2 - lati

        a = np.sin(dlat/2.0)**2 + np.cos(lati) * np.cos(lat2) * np.sin(dlon/2.0)**2

        c = 2 * np.arcsin(np.sqrt(a))
        km = 6367 * c
        return km

    euc = partial(haversine_np, lati, longi)

    dis = []
    for k in df.keys():
            if df[k][0] is '':
                continue
            else: 
                x = float(df[k][0]) / 10e5
                y = float(df[k][1]) / 10e5

                d=euc(x,y)
                dis.append((k,x,y,d))

    dis = sorted(dis, key=lambda x: x[3])
    
    distance = dis[1:11]

    dataf = pd.DataFrame(np.array(distance).reshape(10,4), columns = list("abcd"))
    dataf = dataf.rename(columns={'a': 'PlaceID', 'b': 'Latitude', 'c': 'Longitude', 'd': 'Distance'})
    dataf.to_csv('list.csv', index=False)
    data = []
    with open('list.csv') as f:
        for row in csv.DictReader(f):
            data.append(row)

    json_data = json.dumps(data)

    input = json_data
    #json2html.convert(json = input) 

    #display(HTML(json2html.convert(json = input)))
    
    locations = dataf[['Latitude', 'Longitude']]
    locationlist = locations.values.astype(float).tolist()
    print(locationlist)
    return(input)
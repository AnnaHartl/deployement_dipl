import pickle
import threading

import fiona
import requests

URL = 'https://mygeodata.cloud/data/cs2cs'
INCRS = "+proj=lcc +lat_1=49 +lat_2=46 +lat_0=47.5 +lon_0=13.33333333333333 +x_0=400000 +y_0=400000 +datum=hermannskogel +units=m +no_defs"
OUTCRS = "+proj=longlat +datum=WGS84 +no_defs"


def convert_to_lat_long(coordinates):
    result = list()
    payload = ''
    for coordinate in coordinates:
        payload += f'{coordinate[0]} {coordinate[1]}%n\n'

    post = requests.post(url=URL,
                         data={'coords': payload, 'incrs': INCRS, 'outcrs': OUTCRS, 'addinput': 'false',
                               'switch': 'false'},
                         headers={})
    data = post.json()
    data = data['data']
    lines = data.split('\n')
    i = 0
    for line in lines:
        if len(line) <= 0:
            continue
        parts = line.split(';')
        result.append((float(parts[1]), float(parts[0])))
        i += 1
    return result


def save_converted_coords(ind_start, ind_end, file_ind_start):
    fields = list()
    ind = 0
    file_ind = file_ind_start

    with fiona.open('INSPIRE_SCHLAEGE_2022_POLYGON.gpkg') as layer:
        for feature in layer:
            if ind > ind_start:
                geometry = feature['geometry']
                coordinates = geometry['coordinates']

                converted_coords = convert_to_lat_long(coordinates[0])
                fields.append(converted_coords)

                print(ind)

                if ind % 1000 == 0:
                    with open(f'fields/{str(file_ind).zfill(4)}.pkl', 'wb') as f:
                        pickle.dump(fields, f)
                    fields = list()
                    file_ind += 1
                    print("Gespeichert")
            ind += 1

            if ind > ind_end:
                break



if __name__ == "__main__":
    t1 = threading.Thread(target=save_converted_coords, args=(960000,1060000, 960))
    t2 = threading.Thread(target=save_converted_coords, args=(1060000,1260000, 1060))
    t3 = threading.Thread(target=save_converted_coords, args=(1260000,1460000, 1260))
    t4 = threading.Thread(target=save_converted_coords, args=(1460000,1660000, 1460))

    t1.start()
    t2.start()
    t3.start()
    t4.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()

    print("done")

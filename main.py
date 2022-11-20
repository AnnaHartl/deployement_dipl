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
                    with open(f'fields/{str(file_ind).zfill(3)}.pkl', 'wb') as f:
                        pickle.dump(fields, f)
                    fields = list()
                    file_ind += 1
                    print("Gespeichert")
            ind += 1

            if ind > ind_end:
                break



if __name__ == "__main__":
    t1 = threading.Thread(target=save_converted_coords, args=(82000,82002, 82))
    t2 = threading.Thread(target=save_converted_coords, args=(93000,94000, 93))
    t3 = threading.Thread(target=save_converted_coords, args=(104000,116000, 103))
    # t1 = threading.Thread(target=save_converted_coords, args=(132000, 142000, 132))
    # t2 = threading.Thread(target=save_converted_coords, args=(143000,153000, 143))
    # t3 = threading.Thread(target=save_converted_coords, args=(154000,164000, 154))
    # t4 = threading.Thread(target=save_converted_coords, args=(121000,131000, 121))

    t1.start()
    t2.start()
    t3.start()
    #t4.start()

    t1.join()
    t2.join()
    t3.join()
    #t4.join()

    print("done")
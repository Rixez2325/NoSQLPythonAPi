from pymongo import MongoClient
from pyproj import Transformer
from tqdm import tqdm

from src.pretreatment.pretreatment import Pretreatment


def remove_comma_from_float(string):
    if ',' in string:
        index = string.index(',')
        x = string[:index]
        return x
    else:
        return string


def convert_lambert_93_to_gps(x, y):
    if '.' in x and '.' in y:
        return x, y

    transformer = Transformer.from_crs('epsg:2154', 'epsg:4326')
    return transformer.transform(x, y)


def rename_x_y_lambert_to_x_y_gps(column):
    column.update_many(
        {},
        {
            "$rename": {
                "x_lambert_93": "x_gps",
                "y_lambert_93": "y_gps"
            }
        }
    )


def lambert_to_gps(df, column):
    for row in tqdm(df.iterrows()):
        x_updated = remove_comma_from_float(row[1].get('x_lambert_93'))
        y_updated = remove_comma_from_float(row[1].get('y_lambert_93'))

        x_gps, y_gps = convert_lambert_93_to_gps(x_updated, y_updated)
        myquery = {
            "x_lambert_93": row[1].get('x_lambert_93'),
            "y_lambert_93": row[1].get('y_lambert_93')
        }
        newvalues = {
            "$set": {
                "x_lambert_93": x_gps,
                "y_lambert_93": y_gps
            }
        }

        column.update_one(myquery, newvalues)
    rename_x_y_lambert_to_x_y_gps(col)


# use this focntion if you have gps string data convert all gps data in float
def lambert_to_gps_float(df, column):
    for row in tqdm(df.iterrows()):
        myquery = {
            "x_gps": row[1].get('x_gps'),
            "y_gps": row[1].get('y_gps')
        }
        newvalues = {
            "$set": {
                "x_gps": float(row[1].get('x_gps')),
                "y_gps": float(row[1].get('y_gps'))
            }
        }

        column.update_many(myquery, newvalues)


if __name__ == '__main__':
    URI = 'mongodb+srv://esgiMongo:esgiMongo@cluster0.uu5cw.mongodb.net/test'
    client = MongoClient(URI, serverSelectionTimeoutMS=30000)

    db = client['backup']
    col = db.get_collection('mobile')

    res = Pretreatment().pretraitement(db)
    # lambert_to_gps(res, col)
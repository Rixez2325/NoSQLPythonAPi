import pandas as pd
from pyproj import Transformer
from tqdm import tqdm

def remove_comma_from_float(str):
    if ',' in str:
        index = str.index(',')
        x = str[:index]
        return x
    else:
        return str

def get_region_county_town(col):
    pipeline = [
        {
            '$project': {
                '_id': 0,
                'nom_reg': 1,
                'nom_dep': 1,
                'nom_com': 1
            }
        }
    ]
    res = col.aggregate(pipeline, allowDiskUse=True)
    df = pd.DataFrame(res)
    df_reg = df.groupby(['nom_reg']).count()
    region = []
    for reg in tqdm(df_reg.iterrows()):
        region.append(reg[0])

    df_dep = df.groupby(['nom_dep']).count()
    department = []
    for dep in tqdm(df_dep.iterrows()):
        department.append(dep[0])

    df_com = df.groupby(['nom_com']).count()
    commune = []
    for com in tqdm(df_com.iterrows()):
        commune.append(com[0])

    return region, department, commune

def convert_lambert_93_to_gps(x, y):
    if '.' in x and '.' in y:
        return x, y

    transformer = Transformer.from_crs('epsg:2154', 'epsg:4326')
    return transformer.transform(x, y)


def lambert_to_gps(df, col):
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
                "x_lambert_93": str(x_gps),
                "y_lambert_93": str(y_gps)
            }
        }

        col.update_one(myquery, newvalues)
    rename_x_y_lambert_to_x_y_gps(col)
    serialize_name_town(df, col)

def rename_x_y__to_x_y_gps(col):
    col.update_many(
        {},
        {
            "$rename": {
                "x_gps": "x_lambert_93",
                "y_gps": "y_lambert_93"
            }
        }
    )

def rename_x_y_lambert_to_x_y_gps(col):
    col.update_many(
        {},
        {
            "$rename": {
                "x_lambert_93": "x_gps",
                "y_lambert_93": "y_gps"
            }
        }
    )

def serialize_name_town(df, col):
    for row in tqdm(df.iterrows()):
        nom_com_updated = row[1].get('nom_com').lower().capitalize()
        myquery = {
            "nom_com": row[1].get('nom_com')
        }
        newvalues = {
            "$set": {
                "nom_com": nom_com_updated,
            }
        }

        col.update_one(myquery, newvalues)
    serialize_name_county(df, col)


def serialize_name_county(df, col):
    for row in tqdm(df.iterrows()):
        nom_dep_updated = row[1].get('nom_dep').lower().capitalize()
        myquery = {
            "nom_dep": row[1].get('nom_dep')
        }
        newvalues = {
            "$set": {
                "nom_dep": nom_dep_updated,
            }
        }

        col.update_one(myquery, newvalues)
    serialize_name_region(df, col)


def serialize_name_region(df, col):
    for row in tqdm(df.iterrows()):
        nom_reg_updated = row[1].get('nom_reg').lower().capitalize()
        myquery = {
            "nom_reg": row[1].get('nom_reg')
        }
        newvalues = {
            "$set": {
                "nom_reg": nom_reg_updated,
            }
        }

        col.update_one(myquery, newvalues)

def launch_serialize(df, col):
    serialize_name_town(df, col)

class Pretreatment:

    def pretraitement(self, db):
        col = db.get_collection('mobile')
        pipeline = [
                        {
                            '$project': {
                                '_id': 1,
                                'x_lambert_93': 1,
                                'y_lambert_93': 1,
                                'nom_reg': 1,
                                'nom_dep': 1,
                                'nom_com': 1
                            }
                        }
                    ]
        res = col.aggregate(pipeline, allowDiskUse=True)
        df = pd.DataFrame(res)
        return df
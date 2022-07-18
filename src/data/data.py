import pandas as pd
from flask import jsonify
from pymongo import MongoClient
from tqdm import tqdm

URI = 'mongodb+srv://esgiMongo:esgiMongo@cluster0.uu5cw.mongodb.net/test'
client = MongoClient(URI, serverSelectionTimeoutMS=30000)

db = client['backup']
col = db.get_collection('mobile')

def get_data(collection_name):
    col = db.get_collection(collection_name)
    pipeline = [
        {
            '$project': {
                '_id': 0,
                'nom_op': 1,
                'x_lambert_93': 1,
                'y_lambert_93': 1,
                'region': 1,
                'departement': 1,
                'commune': 1,
                'site_4g': 1,
                'site_5g': 1
            }
        }, {"$limit": 100}]

    res = col.aggregate(pipeline, allowDiskUse=True)
    df = pd.DataFrame(res)

    return jsonify(len(df.index))


def get_operator(collection_name):
    col = db.get_collection(collection_name)
    pipeline = [{
        '$project': {
            '_id': 0,
            'nom_op': 1,
        }
    }]
    res = col.aggregate(pipeline, allowDiskUse=True)
    df = pd.DataFrame(res)
    df_grouped = df.groupby('nom_op').size()

    return df_grouped


def get_region_county_town(col):
    pipeline = [
        {
            '$project': {
                '_id': 0,
                'region': 1,
                'departement': 1,
                'commune': 1
            }
        }
    ]
    res = col.aggregate(pipeline, allowDiskUse=True)
    df = pd.DataFrame(res)
    df_reg = df.groupby(['region']).count()

    region = []
    for reg in tqdm(df_reg.iterrows()):
        region.append(reg[0])

    df_dep = df.groupby(['departement']).count()
    department = []
    for dep in tqdm(df_dep.iterrows()):
        department.append(dep[0])

    df_com = df.groupby(['commune']).count()
    commune = []
    for com in tqdm(df_com.iterrows()):
        commune.append(com[0])

    return region, department, commune


def get_region_county_town_count(col):
    pipeline = [
        {
            '$project': {
                '_id': 0,
                'region': 1,
                'departement': 1,
                'commune': 1
            }
        }
    ]
    res = col.aggregate(pipeline, allowDiskUse=True)
    df = pd.DataFrame(res)
    df_reg = df.groupby(['region']).size()
    df_dep = df.groupby(['departement']).size()
    df_com = df.groupby(['commune']).size()

    return df_reg, df_dep, df_com


operator = get_operator('mobile_provider')
reg, dep, com = get_region_county_town(col)

def mobile_data(collection_name,operator_name):
    col = db.get_collection(collection_name)

    pipeline = [
        {
            '$match': {
                'nom_op': operator_name
            }
        }, {
            '$project': {
                '_id': 0,
                'x_lambert_93': 1,
                'y_lambert_93': 1,
                'region': 1,
                'departement': 1,
                'commune': 1,
                'site_4g': 1,
                'site_5g': 1
            }
        }]

    res = col.aggregate(pipeline, allowDiskUse=True)
    df = pd.DataFrame(res)

    return df

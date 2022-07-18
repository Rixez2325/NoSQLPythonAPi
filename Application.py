import os

import pandas as pd
from bson import Decimal128
from matplotlib import pyplot as plt
from pymongo import MongoClient
from flask import Flask, render_template, request
from geopy.geocoders import Nominatim

from src.data.data import \
    get_operator, \
    mobile_data

geolocator = Nominatim(user_agent="MyApp")
template_dir = os.path.abspath('templates')
app = Flask(__name__, template_folder=template_dir)

URI = 'mongodb+srv://esgiMongo:esgiMongo@cluster0.uu5cw.mongodb.net/test'
client = MongoClient(URI, serverSelectionTimeoutMS=30000)

db = client['backup']
col = db.get_collection('mobile')

df_operator = get_operator('mobile')
operator_name = df_operator.index.tolist()
# reg, dep, com = get_region_county_town(col)
# df_reg, df_dep, df_com = get_region_county_town_count(col)


def get_labels_values_for_plot(df, type):
    keys = []
    values = []
    for key, value in df.items():
        keys.append(key)
        values.append(value)
    generate_plot(keys, values, type)


def generate_plot(keys, values, type):
    fig1, ax1 = plt.subplots(figsize=(len(keys), len(values)))
    ax1.pie(values, labels=keys, autopct='%1.1f%%', shadow=True, startangle=90)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.savefig(f'static/images/{type}.png')


# get_labels_values_for_plot(df_operator, 'operator')
# get_labels_values_for_plot(df_reg, 'region')
# get_labels_values_for_plot(df_dep, 'department')


@app.route('/')
def first_page():
    return render_template('index.html', operators=operator_name, url="static/images/operator.png")


@app.route('/search', methods=['GET'])
def search_operator():
    location = geolocator.geocode(request.args.get('address'))
    lt_gt = round(location.latitude, 2) - 0.05
    lt_lt = round(location.latitude, 2) + 0.05
    lg_gt = round(location.longitude, 2) - 0.05
    lg_lt= round(location.longitude, 2) + 0.05

    pipeline = [
        {
            '$addFields': {
                'x_gps_converted': {
                    '$convert': {
                        'input': '$x_gps',
                        'to': 'double',
                        'onError': 'Error',
                        'onNull': Decimal128('0')
                    }
                },
                'y_gps_converted': {
                    '$convert': {
                        'input': '$y_gps',
                        'to': 'double',
                        'onError': 'Error',
                        'onNull': Decimal128('0')
                    }
                }
            }
        }, {
            '$match': {
                'x_gps_converted': {
                    '$gt': lt_gt,
                    '$lt': lt_lt
                },
                'y_gps_converted': {
                    '$gt': lg_gt,
                    '$lt': lg_lt
                },
            }
        }, {
            '$project': {
                '_id': 0,
                'nom_op': 1,
                'region': 1,
                'departement': 1,
                'commune': 1,
                'site_4g': 1,
                'site_5g': 1,
            }
        }
    ]

    if request.args.get('operator') in operator_name:
        pipeline.append({
            '$match': {
                'nom_op': request.args.get('operator')
            }
        })


    res = col.aggregate(pipeline, allowDiskUse=True)
    df = pd.DataFrame(res)

    return render_template('search_operator.html', data=df.to_html(
                                                    classes='table table-striped text-center',
                                                    justify='center'),
                                                    titles=df.columns.values)

@app.route('/mobile_operator', methods=(["GET"]))
def mobile_data_operator():
    operator_data = mobile_data('mobile', request.args.get('operator'))
    return render_template('operator.html',
                           data=operator_data.to_html(
                               classes='table table-striped text-center',
                               justify='center'
                           ),
                           titles=operator_data.columns.values)


app.run()

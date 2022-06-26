import os

from pymongo import MongoClient
from flask import Flask, jsonify, render_template
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
template_dir = os.path.abspath('templates')
app = Flask(__name__, template_folder=template_dir)


print("hey")
@app.route('/')
def home():
    return jsonify('Hello World!')


@app.route('/mobile', methods=("POST", "GET"))
def get_database():
    URI = 'mongodb+srv://esgiMongo:esgiMongo@cluster0.uu5cw.mongodb.net/test'
    client = MongoClient(URI, serverSelectionTimeoutMS=30000)

    db = client['internet-interactive-map']
    col = db.get_collection('mobile_provider')

    pipeline = [{
        "$match": {
        }
    },{"$limit": 100}]

    res = col.aggregate(pipeline, allowDiskUse=True)

    df = pd.DataFrame(list(res))

    grouped = df.groupby('nom_op').size()
    print(grouped)

    client.close()

    return render_template('index.html',  tables=[df.to_html(classes='data')], titles=df.columns.values)


app.run()

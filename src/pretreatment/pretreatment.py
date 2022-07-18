import pandas as pd
from pyproj import Transformer
from tqdm import tqdm


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


def serialize_name(reg, dep, com, col):
    for r in tqdm(reg):
        reg_updated = r.lower().capitalize()
        myquery = {
            "nom_reg": r
        }
        newvalues = {
            "$set": {
                "nom_reg": reg_updated
            }
        }
        col.update_many(myquery, newvalues)

    for d in tqdm(dep):
        dep_updated = d.lower().capitalize()
        myquery = {
            "nom_dep": d
        }
        newvalues = {
            "$set": {
                "nom_dep": dep_updated
            }
        }
        col.update_many(myquery, newvalues)

    for c in tqdm(com):
        com_updated = c.lower().capitalize()
        myquery = {
            "nom_com": c
        }
        newvalues = {
            "$set": {
                "nom_com": com_updated
            }
        }
        col.update_many(myquery, newvalues)

    change_column_name(col)

def change_column_name(col):
    col.update_many(
        {},
        {
            "$rename": {
                "nom_reg": "region",
                "nom_com": "commune",
                "nom_dep": "departement"
            }
        }
    )

class Pretreatment:

    def pretraitement(self, db):
        col = db.get_collection('mobile')
        pipeline = [
            {
                '$project': {
                    '_id': 1,
                    'x_gps': 1,
                    'y_gps': 1,
                    'nom_reg': 1,
                    'nom_dep': 1,
                    'nom_com': 1
                }
            }
        ]
        res = col.aggregate(pipeline, allowDiskUse=True)
        df = pd.DataFrame(res)
        return df

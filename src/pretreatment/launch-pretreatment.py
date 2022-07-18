from pymongo import MongoClient

from src.pretreatment import Pretreatment, launch_serialize, get_region_county_town

if __name__ == '__main__':
    URI = 'mongodb+srv://esgiMongo:esgiMongo@cluster0.uu5cw.mongodb.net/test'
    client = MongoClient(URI, serverSelectionTimeoutMS=30000)

    db = client['backup']
    col = db.get_collection('mobile')

    # res = Pretreatment().pretraitement(db)
    # lambert_to_gps(res, col)
    reg, dep, com = get_region_county_town(col)
    print(reg)
    print(dep)
    print(com)
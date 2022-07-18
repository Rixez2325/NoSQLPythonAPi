from pymongo import MongoClient

from src.data.data import get_region_county_town
from src.pretreatment.pretreatment import serialize_name

if __name__ == '__main__':
    URI = 'mongodb+srv://esgiMongo:esgiMongo@cluster0.uu5cw.mongodb.net/test'
    client = MongoClient(URI, serverSelectionTimeoutMS=30000)

    db = client['backup']
    col = db.get_collection('mobile')
    reg, dep, com = get_region_county_town(col)
    serialize_name(reg, dep, com, col)
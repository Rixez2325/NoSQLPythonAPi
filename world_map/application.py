import numpy as np
from pycountry_convert import country_alpha2_to_continent_code, country_name_to_country_alpha2
from geopy.geocoders import Nominatim
import folium
from folium.plugins import MarkerCluster
import pandas as pd


def get_continent(col):
    try:
        cn_a2_code = country_name_to_country_alpha2(col)
    except:
        cn_a2_code = 'Unknown'
    try:
        cn_continent = country_alpha2_to_continent_code(cn_a2_code)
    except:
        cn_continent = 'Unknown'
    return cn_a2_code, cn_continent


geolocator = Nominatim(user_agent="my")


def geolocate(country):
    try:
        # Geolocate the center of the country
        loc = geolocator.geocode(country)
        # And return latitude and longitude
        return loc.latitude, loc.longitude
    except:
        # Return missing value
        return np.nan


def world_map():
    # empty map
    world_map = folium.Map(tiles="cartodbpositron")
    marker_cluster = MarkerCluster().add_to(world_map)
    # for each coordinate, create circlemarker of user percent
    for i in range(len(df)):
        lat = df.iloc[i]['Latitude']
        long = df.iloc[i]['Longitude']
        radius = 5
        popup_text = """Country : {}<br>
                        %of Users : {}<br>"""
        popup_text = popup_text.format(df.iloc[i]['Country'],
                                       df.iloc[i]['User_Percent']
                                       )
        folium.CircleMarker(location=[lat, long], radius=radius, popup=popup_text, fill=True).add_to(marker_cluster)


if __name__ == '__main__':
    df = pd.read_csv('country.csv', sep=';')
    for i in range(len(df)):
            res = get_continent(df.iloc[i][0])
            # print(get_continent(df.iloc[i][0]))
            # print(res[0])
            # df.assign(get_continent(df.iloc[i][0]))
            if 'Continent' not in df.columns:
                df.insert(loc=len(df.columns), column='Country', value=res[0])
                df.insert(loc=len(df.columns), column='Continent', value=res[1])
            else:
                df['Country'] = res[0]
                df['Continent'] = res[1]




    print(df.to_string())
    # res = get_continent(df.columns)

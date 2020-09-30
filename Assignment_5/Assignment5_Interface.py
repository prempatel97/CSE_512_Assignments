#
# Assignment5 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
import json
import codecs
from math import sqrt, radians, sin, asin, cos, atan, atan2, pow

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
        search = collection.find({"city" : {'$regex' : '^'+cityToSearch+'$', '$options' : 'i'}})
        output = codecs.open(saveLocation1, 'w', encoding='utf-8')
        for j in search:
            name = j.get("name")
            address = j.get("full_address").replace("\n"," ,")
            city = j.get("city")
            state = j.get("state")
            final_out = name+"$"+address+"$"+city+"$"+state+"\n"
            output.write(final_out)
        output.close()

def dist_func(lat1, lon1, lat2, lon2):
    R = 3959
    latitude1 = radians(lat1)
    latitude2 = radians(lat2)
    lat_difference = radians(lat2 - lat1)
    long_difference = radians(lon2 - lon1)
    t1 = pow(sin(lat_difference / 2), 2) + cos(latitude1) * cos(latitude2) * pow(sin(long_difference / 2), 2)
    t2 = 2 * atan2(sqrt(t1), sqrt(1 - t1))
    distance = R * t2
    return distance


def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):

    if(categoriesToSearch == []):
        pass
    else:
        search = collection.find({"categories" : {'$in' : categoriesToSearch}}, {"categories" : 1,"name" : 1, "longitude" : 1, "latitude" : 1})
        output = codecs.open(saveLocation2, 'w', encoding='utf-8')
        lat1 = (float(myLocation[0]))
        long1 = (float(myLocation[1]))

        for i in search:
            lat = i.get("latitude")
            long = i.get("longitude")
            lat2 = (float(lat))
            long2 = (float(long))
            distance = dist_func(lat1, long1, lat2, long2)
            name = i.get("name")
            if distance <= maxDistance:
                line_to_print = name+"\n"
                output.write(line_to_print)
        output.close()

#!/usr/bin/env python

import os
from geopy import geocoders
import psycopg2

def get_db(db_url):
    cnx = psycopg2.connect(db_url)
    return cnx

def get_api_key():
    if os.path.exists('.credentials'):
        l = open('.credentials').read()
        return l.strip()

def as_point(p):
    # using string interpolation w/ only the typechecking provided by
    # string ( using %f rather than %s ) 
    # @@todo: ensure not sql injection vector
    return "point(%f %f)" % (p[1],p[0]) 

if __name__=='__main__':
    api_key = get_api_key()
    db = get_db('host=localhost port=5433 dbname=bikewillamette')

    cursor = db.cursor()
    # we do two passes one with addresses that have Eugene in them and one with a template
    g = geocoders.MapQuest(api_key)
    cursor.execute("select id, address from supporters where address ilike '%eugene%';")
    addrs = cursor.fetchall()
    addr_locations = [ (a[0],"mapquest", as_point(g.geocode(a[1])[1])) 
                       for a in addrs ]
    cursor.executemany(
        """insert into supporter_points (supporter_id,geocoder,location) values( %s, %s, ST_GeomFromText(%s, 4326);""", addr_locations) 
    db.commit()
    print "round 1 finished"

    #round 2
    cursor = db.cursor()
    g = geocoders.MapQuest(api_key, format_string='%s Eugene, OR')
    cursor.execute("select id, address from supporters where address not ilike '%eugene%';")
    addrs = cursor.fetchall()
    addr_locations = [ (a[0],"mapquest", as_point(g.geocode(a[1])[1])) 
                       for a in addrs ]
    cursor.executemany(
        """insert into supporter_points (supporter_id,geocoder,location)
        values( %s, %s, ST_GeomFromText(%s, 4326));""", addr_locations) 
    db.commit()
    db.close()
    print "round 2 finished"
    

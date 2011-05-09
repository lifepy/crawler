#!/usr/bin/python
import urllib2
import sys

def geocode_json(address):
    print "ASKING GMAP for ", address; sys.stdout.flush()
    response = urllib2.urlopen('http://maps.googleapis.com/maps/api/geocode/json?address=%s&language=zh-CN&sensor=true' % address).read()
    return response

def geocode_xml(address):
    response = urllib2.urlopen('http://maps.googleapis.com/maps/api/geocode/xml?language=zh-CN&address=%s&sensor=true' % address).read()
    return response

if __name__=="__main__":
    response = geocode_json(sys.argv[1])
    print response

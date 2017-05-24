#!/usr/bin/python

###############################################
# Program: Scrape foodpanda website for all the restaurants in a city
# Author: Ankur Pal
# Date: 10-12-2015
# Version 1.0
###############################################

import multiprocessing as mp
from bs4 import BeautifulSoup as bs
import urllib
import urllib2
import json
import pandas as pd
import time
import sys
import re
import math as m
import os

# city = sys.argv[1]


def main():
    cities = scrapeCities()
    print 'Cities available for scrpaing:'
    print ',\t'.join(sorted(cities.keys()))
    city = raw_input('Enter name of the city to be scraped:\n')
    while (city not in cities.keys()):
        print 'Entered city name', city, 'is invalid.'
        city = raw_input('Enter name of the city to be scraped:\n')
    print 'Scraping', city, 'restaurants...'
    cityID = cities[city]
    areas = scrapeAreas(cityID, city)

    # city = 'Hyderabad'
    # areas = readAreas('data/Hyderabad_areas.json')
    # areas = areas

    getRestaurants(city, areas, True)


def scrapeCities():
    checkDir('data')
    checkDir('logs')
    url = 'https://www.foodpanda.in/'
    soup = scrapePage(url)
    cities = soup.find('div', 'city').find_all('option', value=re.compile('[0-9]+'))
    ids = [c['value'] for c in cities]
    names = [c.get_text().encode('utf-8').strip() for c in cities]
    return dict(zip(names, ids))


def scrapeAreas(cityID, city):
    url = 'https://www.foodpanda.in/city-suggestions-ajax/' + cityID
    req = urllib2.Request(url, headers=reqHeaders)
    content = urllib2.urlopen(req)
    areas = json.load(content)
    opf = open('data' + os.sep + city + '_areas.json', 'w')
    json.dump(areas, opf)
    return areas


def readAreas(file):
    return json.load(open(file))


def scrapePage(url, log=None):
    soup = None
    err = 0
    while ((soup == None) and (err < 5)):
        try:
            req = urllib2.Request(url, headers=reqHeaders)
            page = urllib2.urlopen(req).read()
            soup = bs(page, 'html.parser')
        except Exception, e:
            if log == None:
                print e
                print url
            else:
                log.write('Error: ' + str(e) + '\n')
                log.write('Url: ' + url + '\n')
            err += 1
    return soup


def getRestaurants(city, areas, full=False):
    seg = 10
    n = int(m.ceil(1.0 * len(areas) / seg))
    procs = []
    for i in xrange(seg + 1):
        p = mp.Process(target=scrapeRestaurants, args=(city, i, n, areas, full))
        p.start()
        procs.append(p)
    for p in procs:
        p.join()
    appendOutput(city, seg, full)


def scrapeRestaurants(city, i, n, areas, full=False):
    opf = open('data' + os.sep + city + '_' + str(i) + '.csv', 'w')
    log = open('logs' + os.sep + city + '_' + str(i) + '.log', 'w')
    pages = len(areas)
    st = i * n
    end = min((i + 1) * n, pages)
    for j in xrange(st, end):
        area = areas[j]
        url = base_url + urllib.urlencode(area['fillSearchFormOnSelect'])
        soup = scrapePage(url, log)
        if soup:
            restaurants = pd.Series(list(soup.find_all('article')))
            data = restaurants.apply(lambda x: getFields(x, log, full))
            data['Area'] = area['value'].encode('utf-8').strip()
            data.to_csv(opf, index=False, header=False, encoding='utf-8')
            log.write(str(j) + ': Successfully scraped data for area ' +
                      area['value'].encode('ascii', 'ignore').strip() + '\n')
            opf.flush()
            log.flush()
    opf.close()
    log.close()


def getFields(row, log=None, full=False):
    details = row.find('div', 'vendor__details')
    Name = text(details.find('a', 'js-fire-click-tracking-event'))
    Cuisines = ', '.join([text(c) for c in details.find('ul', 'vendor__cuisines').find_all('li')])
    Rating = details.find('i', 'stars')['content'].encode('utf-8').strip()
    Reviews = details.find('p', 'review')
    if Reviews.find('span'):
        Reviews = text(Reviews.find('span'))
    else:
        Reviews = text(Reviews)
    DelTime = row.find('dl', 'vendor__delivery-time')
    if DelTime:
        DelTime = text(DelTime.find('dd'))
    DelFee = row.find('dl', 'vendor__delivery-fee')
    if DelFee:
        DelFee = text(DelFee.find('dd'), rep='Rs.')
    MinOrder = row.find('dl', 'vendor__order-minimum')
    if MinOrder:
        MinOrder = text(MinOrder.find('dd'), rep='Rs.').replace('from', '').strip()
    Link = loc_base_url + \
        row.find('a', 'js-fire-click-tracking-event')['href'].encode('utf-8').strip()

    if not full:
        return pd.Series([Name, Cuisines, Rating, Reviews, DelTime, DelFee, MinOrder, Link],
                         index=['Name', 'Cuisines', 'Rating', 'Reviews', 'DelTime',
                                'DelFee', 'MinOrder', 'Link'])
    Street = None
    Region = None
    PostalCode = None
    Latitude = None
    Longitude = None
    DelLocations = None
    soup = scrapePage(Link, log)
    if soup:
        address = soup.find('address', 'vendor-info__address__content')
        if address:
            Street = text(address.find('span', itemprop='streetAddress'))
            Region = text(address.find('span', itemprop='addressRegion'))
            PostalCode = text(address.find('span', itemprop='postalCode'))
        Latitude = soup.find('meta', itemprop='latitude')['content'].encode('utf-8').strip()
        Longitude = soup.find('meta', itemprop='longitude')['content'].encode('utf-8').strip()
        DelLocations = soup.find('ul', 'delivery-locations-list')
        if DelLocations:
            DelLocations = ', '.join(
                [text(l) for l in DelLocations.find_all('li')])
    return pd.Series([Name, Cuisines, Rating, Reviews, DelTime, DelFee, MinOrder, Link, Street,
                      Region, PostalCode, Latitude, Longitude, DelLocations],
                     index=['Name', 'Cuisines', 'Rating', 'Reviews', 'DelTime', 'DelFee',
                            'MinOrder', 'Link', 'Street', 'Region', 'PostalCode', 'Latitude',
                            'Longitude', 'DelLocations'])


def checkDir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def text(x, rep='', spl=None):
    x = x.text.encode('utf-8').replace(rep, '').strip() if x else ''
    return x.split()[spl] if (len(x) and spl) else x


def appendOutput(city, seg, full=False):
    opf = open('data' + os.sep + city + '_data.csv', 'w')
    if full:
        opf.write(
            'Name,Cuisines,Rating,Reviews,DelTime,DelFee,MinOrder,Link,Street,Region,PostalCode,Latitude,Longitude,DelLocations\n')
    else:
        opf.write(
            'Name,Cuisines,Rating,Reviews,DelTime,DelFee,MinOrder,Link\n')
    for i in range(seg+1):
        ipf = open('data' + os.sep + city + '_' + str(i) + '.csv')
        for line in ipf:
            opf.write(line)
        ipf.close()
    opf.close()


reqHeaders = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; \
     rv:43.0) Gecko/20100101 Firefox/43.0'}
base_url = 'https://www.foodpanda.in/restaurants?'
loc_base_url = 'https://www.foodpanda.in'

if __name__ == "__main__":
    main()
    # city = 'Hyderabad'
    # areas = readAreas('data/Hyderabad_areas.json')
    # scrapeRestaurants(city, 0, 1, areas, True)
    # appendOutput(city, 0, True)

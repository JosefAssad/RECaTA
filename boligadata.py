#!/usr/bin/env python
# -*- coding: utf-8 -*-

# TODO implement logging
# TODO indexes
# TODO consolidated commits
# TODO split out andel/stuff

import BeautifulSoup as bs
import urllib2
import re
import argparse
import pdb
import datetime
from IPython import embed
from sqlalchemy import create_engine, func, desc
from sqlalchemy import Column, Integer, String, ForeignKey, MetaData, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import UniqueConstraint

boligtyper = ['Villa', 'Ejerlejlighed', 'Villalejlighed',
              u'Rækkehus', 'Fritidshus', 'Andelsbolig',
              'Landejendom', u'Helårsgrund', 'Fritidsgrund']
db='postgresql://boligadata:boligadata@localhost:5432/boligadata'
base_url='http://www.boliga.dk/soeg/resultater?type=Villa&type=Ejerlejlighed&type=Villalejlighed&type=R%C3%A6kkehus&type=Fritidshus&type=Andelsbolig&type=Landejendom&type=Hel%C3%A5rsgrund&type=Fritidsgrund&type=&byggetMin=&byggetMax=&q=&amt=&kom=0&fraPostnr=&tilPostnr=&iPostnr=&gade=&minEnergi=&maxEnergi=&min=&max=&minNet=&maxNet=&minKvmPris=&maxKvmPris=&minLiggetid=&maxLiggetid=&minRooms=&maxRooms=&minSize=&maxSize=&minGardenSize=&maxGardenSize=&minBasementSize=&maxBasementSize=&minEtage=&maxEtage=&page='

Base = declarative_base()
# max_pages is to enable testing on fewer pages
# set it to a low number like 2 to limit the run to 2 pages for testing
# set it to something stupidly high (99999) for production use. TODO rework this
max_pages=9999999999999999
# Optimisations; use carefully
assume_listings_unique=False
assume_listingdata_unique=False


class BoligaDataRun(Base):
    __tablename__ = 'boligadataruns'
    id            = Column(Integer, primary_key=True)
    run_date      = Column(DateTime)

    def __init__(self, date=None):
        if not date: date = datetime.datetime.now()
        self.run_date = date


class BoligaDataPage(Base):
    __tablename__ = 'boligadatapages'
    id            = Column(Integer, primary_key=True)
    run           = Column(Integer, ForeignKey('boligadataruns.id'))
    page          = Column(Text)


class Listing(Base):
    __tablename__         ='listings'
    id                    = Column(Integer, primary_key=True)
    boliga_id             = Column(Integer, unique=True)
    postcode              = Column(Integer)
    street_address        = Column(String)
    home_area             = Column(Integer)
    ttl_area              = Column(Integer)
    year_built            = Column(Integer)
    rooms                 = Column(Integer)
    city                  = Column(String)
    boligtype             = Column(String)

    def __str__(self):
        return "Listing id: "+ str(self.boliga_id) + " - Address: " + self.address + " " + str(self.postcode)


class ListingData(Base):
    __tablename__  = 'listingdata'
    __table_args__ = (UniqueConstraint('page_id', 'listing'),{})
    id             = Column(Integer, primary_key=True)
    listing        = Column(Integer, ForeignKey('listings.id'), index=True)
    price          = Column(Integer)
    days_available = Column(Integer)
    page_id        = Column(Integer, ForeignKey('boligadatapages.id'), index=True)


class BoligaDataCacher(object):

    def __init__(self):
        self.engine  = create_engine(db, echo=False)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def zap(self):
        meta = MetaData(self.engine)
        meta.reflect()
        meta.drop_all()

    def initialise(self):
        Base.metadata.create_all(self.engine)       

    def update_pages(self):
        page_no = 1
        run = BoligaDataRun()
        self.session.add(run)
        self.session.commit()
        while not page_no > max_pages:
            bdp = BoligaDataPage()
            bdp.run = run.id
            bdp.page = urllib2.urlopen(base_url + str(page_no)).read()
            self.session.add(bdp)
            if self._is_last_page(bdp.page): break
            page_no += 1
        self.session.commit()

    def update_db(self, run_id=None):
        if not run_id:
            run_id = self.session.query(BoligaDataRun).\
                     order_by(desc(BoligaDataRun.run_date)).first().id
        for page in self.session.query(BoligaDataPage).\
                filter(BoligaDataPage.run==run_id):
            self._extract_entries(page)
                
    def _is_last_page(self, page):
        soup = bs.BeautifulSoup(page)
        try:
            n = soup.find('table', 'searchresultpaging').\
                findAll('td')[2].a.renderContents()
        except AttributeError:
            return True
        if n == '&nbsp;N\xc3\xa6ste >>':
            return False
        return True

    def _listingdata_to_db(self, listingdata):
        datapoint                = ListingData()
        datapoint.listing        = listingdata['listing']
        datapoint.price          = listingdata['price']
        datapoint.days_available = listingdata['days_available']
        datapoint.page_id        = listingdata['page_id']
        self.session.add(datapoint)
        if not assume_listingdata_unique:
            try:
                self.session.commit()
            except IntegrityError:
                self.session.rollback()
            
    def _listing_to_db(self, listing):
        bolig                = Listing()
        bolig.boliga_id      = listing['boliga_id']
        bolig.postcode       = listing['postcode']
        bolig.street_address = listing['street_address']
        bolig.home_area      = listing['home_area']
        bolig.ttl_area       = listing['ttl_area']
        bolig.year_built     = listing['year_built']
        bolig.rooms          = listing['rooms']
        bolig.boligtype      = listing['boligtype']
        bolig.city           = listing['city']
        self.session.add(bolig)
        try:
            self.session.commit()
            return bolig.id
        except IntegrityError:
            self.session.rollback()
            return self.session.query(Listing).\
                   filter(Listing.boliga_id==bolig.boliga_id).one().id

    def _rooms_strtoint(self, str_rooms):
        # this is necessary since apparently there can be 2,5 rooms
        return int(''.join(str_rooms.split(',')))

    def _tokenise_addr(self, line):
        # line contains: address, type of bolig, "i", then city
        # we want address and type of bolig
        for boligtype in boligtyper:
            if re.match(u'.*\ %s\ i.*' % boligtype, unicode(line)):
                [address, city] = line.split(' '+boligtype+' i')
                return [address, boligtype, city]
        return None

    def _extract_entries(self, page):
        soup = bs.BeautifulSoup(page.page)
        entries = soup.findAll('tr', {'class': re.compile(r'pRow\ even|pRow\ ')})
        for entry in entries:
            # BEGIN FEW CLEANUPS ------------------
            # remove <br> tags
            for b in entry.findAll('br'):
                b.extract()
            # get rid of energithing
            try:
                energimaerke = entry.find('div', {'class': re.compile(r'.*energi\ .*')})
                energimaerke.extract()
            except:
                pass # no energimaerke thing
            # we have no use for images.
            try:
                for p in entry.findAll('img'):
                    p.extract()
            except:
                pass
            # get rid of the price change fields, if they're there
            try:
                span_pricediff = entry.find('span', re.compile(r'red|green'))
                span_pricediff.extract()
            except:
                pass
            # Don't need to know when there's open house either
            try:
                openhousespan = entry.find('span', 'openHouseText')
                openhousespan.extract()
            except:
                pass
            # END CLEANUPS -------------------------
            elements = entry.findAll()
            listing = {}
            if len(elements) == 17: # has picture, which adds an element
                listing['boliga_id']      = int(elements[0].a.attrs[0][1].split('=')[1])
                listing['postcode']       = int(elements[11].string)
                address_tokens            = self._tokenise_addr(elements[0].contents[3]['title'])
                listing['street_address'] = address_tokens[0]
                listing['boligtype']      = address_tokens[1]
                listing['city']           = address_tokens[2]
                #listing['address']        = elements[0].contents[3]['title']
                listing['home_area']      = int(elements[7].string.split(' ')[0])
                listing['ttl_area']       = int(elements[8].string.split(' ')[0])
                listing['year_built']     = int(elements[9].string)
                listing['rooms']          = self._rooms_strtoint((elements[3].string))
                listing_data = {}
                bolig_id = self._listing_to_db(listing)
                listing_data['price']     = int(''.join(elements[5].string.split('.')))
                listing_data['days_available'] = int(elements[11].string)
                listing_data['listing']        = bolig_id
                listing_data['page_id']        = page.id
                self._listingdata_to_db(listing_data)
                if assume_listingdata_unique:
                    self.session.commit()
            elif len(elements) == 16: # no photo
                listing['boliga_id']      = int(elements[0].a.attrs[0][1].split('=')[1])
                listing['postcode']       = int(elements[11].string)
                address_tokens            = self._tokenise_addr(elements[0].contents[3]['title'])
                listing['street_address'] = address_tokens[0]
                listing['boligtype']      = address_tokens[1]
                listing['city']           = address_tokens[2]
                #listing['address']        = elements[0].contents[3]['title']
                listing['home_area']      = int(elements[7].string.split(' ')[0])
                listing['ttl_area']       = int(elements[8].string.split(' ')[0])
                listing['year_built']     = int(elements[9].string)
                listing['rooms']          = self._rooms_strtoint(elements[3].string)
                listing_data = {}
                bolig_id = self._listing_to_db(listing)
                if assume_listingdata_unique:
                    self.session.commit()
                listing_data['price'] = int(''.join(elements[4].span.string.split('.')))
                listing_data['days_available'] = int(elements[12].span.string)
                listing_data['listing']        = bolig_id
                listing_data['page_id']        = page.id
                self._listingdata_to_db(listing_data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A foo that bars.')
    parser.add_argument('--init', help='Initialise the database.', action='store_true')
    parser.add_argument('--zap', help='Initialise the database.', action='store_true')
    parser.add_argument('--update-pages', help='Update the page cache.',
                        action='store_true')
    parser.add_argument('--update-db', help='Update the db from page cache.',
                        action='store_true')
    parser.add_argument('--ipython', '-i',
                        help="Start an interative IPython interpreter",
                        action='store_true')
    args = parser.parse_args()
    if args.ipython and not args.init and not args.update_db and not args.zap and not args.update_pages:
        bd = BoligaDataCacher()
        embed()
    if args.init and not args.update_db and not args.zap and not args.update_pages and not args.ipython:
        bd = BoligaDataCacher()
        bd.initialise()
    elif args.zap and not args.update_db and not args.init and not args.update_pages and not args.ipython:
        bd = BoligaDataCacher()
        bd.zap()
    elif args.update_pages and not args.update_db and not args.init and not args.zap and not args.ipython:
        bd = BoligaDataCacher()
        bd.update_pages()
    elif args.update_db and not args.update_pages and not args.init and not args.zap and not args.ipython:
        bd = BoligaDataCacher()
        bd.update_db()
    else:
        print IPython.__version__
        parser.print_help()
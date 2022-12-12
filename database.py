from pymongo import MongoClient
from dotenv import load_dotenv, find_dotenv

import os
from urllib.parse import quote_plus 
from datetime import datetime, timedelta
import logging


class Database:

    def __init__(self):
        load_dotenv(find_dotenv())
        password: str = quote_plus(os.environ.get('MONGODB_PWD'))
        DB_CONNECTION = ''

        self.client = MongoClient(DB_CONNECTION)
        self.investment_database = self.client["investment"]
        self.stocks = self.investment_database["stocks"]
        self.yields = self.investment_database["yields"]


    def _get_databases (self):
        '''Returns databases' name'''
        return self.client.list_database_names()


    def insert_data ( self, data: dict) -> None:
        '''Inserts data (one row) to yields or stocks collection. 
        If yield_type in data.keys(), redirects to yields collection.
        If transaction_type as "Sell" or "Buy", redirects it to stocks colletcion

        :param data: the dict to be inserted into the database
        :return: None'''
        logger = logging.getLogger(Database.insert_data.__qualname__)
        try:
            if 'yield_type' in data.keys():
                inserted = self.yields.insert_one(data)
            elif 'Sell' in data['transaction_type'] or 'Buy' in data['transaction_type']:
                inserted = self.stocks.insert_one(data)
            logger.info(f'{inserted} data has been successfully inserted')
        except Exception as e:
            raise Exception('Could not insert data: ', str(e))


    def drop_collection(self, collection: str) -> bool:
        '''Drops yields or stocks collection

        :param collection: refers to yields or stocks
        :return : bool'''
        logger = logging.getLogger(Database.drop_collection.__qualname__)
        if collection == 'yields': col = self.yields
        elif collection == 'stocks': col = self.stocks
        else:
            raise KeyError (f'{collection} not Found.')

        dropped = col.drop()
        if dropped:
            logger.info(f'Collection {col} succesully dropped')
        else:
            logger.info(f'Collection {col} does not exist yet')
        return dropped


    def genreport_stocks (self, threshold_date: str =None, broker: str ='Rico') -> list:
        '''Query stocks matching transaction date before a threshold date 
            and the broker where th stocks were bought 
        
        :param threshold_date :  Will query stocks bought/sold beofre this date
        :param broker : Stocks bought/sold matching this broker field
        :return : list of dicts containing the result of this query'''
        logger = logging.getLogger(Database.genreport_stocks.__qualname__)
        results = []
        if not threshold_date:
            logger.debug('Setting date to today')
            threshold_date = datetime.now().strftime("%Y-%m-%#d") 
            threshold_date = datetime.strptime(threshold_date,'%Y-%m-%d' )  
        else:
            threshold_date = datetime.strptime(threshold_date, '%Y-%m-%d')
        result = self.stocks.aggregate([ {'$match':{ "date": { '$lte': threshold_date}, "broker": {'$eq': broker} } }, { '$group': { '_id': '$stock', 'total': { '$sum': "$total_price"}, 'quantity': { '$sum': "$quantity"} } } ] )
        logger.info(f'Threshold date "{threshold_date}"')
        for row in result:
            results.append({ 'stock' : row['_id'], 'total': float(row['total']) , 'quantity': int(row['quantity'])})
        return results


    def genreport_sold_stocks(self, from_date: str = None, to_date: str = None, broker: str='Rico') -> list:
        '''Query Sold Stocks from/to specific period of time, matching a broker
        
        :param from_date : Start date of the time period
        :param to_date: End date fo the time period
        :broker : Stocks sold matching this broker field
        :return : list of dict containing the result of this query'''
        logger = logging.getLogger(Database.genreport_sold_stocks.__qualname__)
        if from_date:
            from_date = datetime.strptime(from_date, "%Y-%m-%d")
        if to_date:
            to_date = datetime.strptime(to_date, "%Y-%m-%d")
        if not from_date:
            this_year = str(datetime.now().year)
            from_date = datetime.strptime(str(this_year)+'-01-1', '%Y-%m-%d')
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%#d")
            to_date = datetime.strptime(to_date,'%Y-%m-%d') 

        logger.info(f'From date: {from_date} | To date: {to_date}')
        results = list(self.stocks.find ({'$and' : [{'date': {'$gt': from_date, '$lt': to_date }}, {"broker": {'$eq': broker}},  {"transaction_type": {'$eq': 'Sell'}} ]}))
        return results
        

    def genreport_yield(self, from_date: str = None, to_date: str = None, broker: str =None) -> list:
        '''Query yields received in a period of time matching a broker
        
        :param from_date : Start date of the time period
        :param to_date: End date fo the time period
        :broker : Stocks sold matching this broker field
        :return : list of dict containing the result of this query'''
        logger = logging.getLogger(Database.genreport_yield.__qualname__)
        if from_date:
            from_date = datetime.strptime(from_date, "%Y-%m-%d")
        if to_date:
            to_date = datetime.strptime(to_date, "%Y-%m-%d")
        if not from_date:
            this_year = str(datetime.now().year)
            from_date = datetime.strptime(str(this_year)+'-01-1', '%Y-%m-%d')
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%#d")
            to_date = datetime.strptime(to_date,'%Y-%m-%d') 
        result =  list(self.yields.find ({'$and' : [{'date': {'$gt': from_date, '$lt': to_date }}, {"broker": {'$eq': broker}} ]}))
        #result = self.yields.aggregate([ {'$match':{ "date": { '$gt': from_date, '$lt': to_date}, "broker": {'$eq': broker}} }, { '$group': { '_id': {'stock':'$stock','yield_type':'$yield_type'}, 'total_value': { '$sum': "$value"}  } }  ] )
        logger.info(f'From date: {from_date} | To date: {to_date}')
        return result


    def yield_per_stock(self):
        '''Legacy function'''
        from_date = datetime.strptime('2010-10-10', "%Y-%m-%d")
        to_date = datetime.strptime('2022-10-31', "%Y-%m-%d")
        broker = 'Rico'
        result =  list(self.stocks.find ({'$and' : [{'date': {'$gt': from_date, '$lt': to_date }}, {"broker": {'$eq': broker}} ]}))
        #result = self.yields.aggregate([ {'$match':{ "date": { '$gt': from_date, '$lt': to_date}, "broker": {'$eq': broker}, "stock" : {'$eq': stock}} }, { '$group': { '_id': '$yield_type', 'total_value': { '$sum': "$value"} } , 'date':'$date'} ] )
        return list(result)
        

    def find_all(self, collection):
        '''returns all rows from a collection
            
            :param collection: the name of the collection, can be either stocks or yields'''
        if collection == 'stocks':
            all_rows = list(self.stocks.find())
        elif collection == 'yields':
            all_rows = list(self.yields.find())
        print(all_rows)
        return all_rows


    def get_all_rows(self):
        '''Legacy function'''
        rows = []
        for row in self.stocks.find():
            rows.append(row)
            #print(row)
        return rows

    def update_from_csv(self):
        pass
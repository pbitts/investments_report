
import argparse
import logging
import os
from datetime import datetime
from typing import Union

import pytz

from create_table import Table
from database import Database
from statistics import Statistics
from export_logic import Export

def run_operations(operation_type: str = None, product_type: str =None):
    '''Defines and call functions do realize desired operation
        :param operation_type: the kind of the desired operation.
        :param product_type: The operation will act upon this product'''

    def add_transaction(product_type = 'stocks'):
        '''Adds to database and CSV file new transaction'''

        logger = logging.getLogger(add_transaction.__qualname__)
        logger.info(f'Product Type: {product_type}')

        def add_stock():
            '''Adds a new stock transaction ( Buy or Sell)
            Receives input from user (stock, broker, date, quantity, price etc)
            Creates a payload translating strings to right type
            Export to csv and to database'''

            stock: str = input('\nStock (ex: EGIE3F) : ')
            if not stock: stock = 'None'
            broker: str = input('Broker: ').capitalize()
            date: str = input('\nDate of the transaction (format: 2022-10-4) :')
            if not date: date =  datetime.now().strftime("%Y-%m-%#d")
            quantity: str = input('\nQuantity of share(ex: 4) : ')
            if not quantity: quantity = 0
            price: str = input('\nIndividual price for each share: ')
            if not price: price = 0
            total_price: str = input('\nTotal paid price on this transaction: ')
            if not total_price : price = 0
            transaction_type: str = input('\nBuy or Sell: ').capitalize()
            if 'Buy' == transaction_type:
                data = {
                        'broker': str(broker),
                        'transaction_type': str(transaction_type),
                        'stock' : str(stock),
                        'date' : datetime.strptime(date, '%Y-%m-%d'),
                        'quantity' : int(quantity),
                        'price' : float(price),
                        'total_price' : float(total_price)
                        }
            elif 'Sell' == transaction_type:
                performance = None
                mongo_db = Database()
                report: list = mongo_db.genreport_stocks()
                for row in report:
                    if row['stock'] == stock:
                        pm = row['total']/row['quantity']
                        performance = int(quantity)*float(price)  -  pm*int(quantity) 
                if not performance:
                    raise UnboundLocalError (' Stock has not been bought yet ')
                data = {
                        'broker': str(broker),
                        'transaction_type': str(transaction_type),
                        'stock' : str(stock),
                        'date' : datetime.strptime(date, '%Y-%m-%d'),
                        'quantity' : -int(quantity),
                        'price' : -float(price),
                        'total_price' : -float(total_price),
                        'performance' : float(performance)
                        }
            else:
                print (f'Invalid transaction type "{transaction_type}" '
                                'Choose either "Buy" or "Sell" ')
                return

            print('Adding new transaction . . .')
            print(data)
            export_to_csv(data)
            export_to_db(data)

        def add_yield():
            '''Adds a new yield transaction (Dividend,JCP, rendimentos de clientes 
            frações de ações, etc)
            Receives input from user (stock, broker, date, quantity, price etc)
            Creates a payload translating strings to right type
            Export to csv and to database'''

            broker: str = input('Broker: ').capitalize()
            yield_type: str = input('Yield type : ').lower()
            if not ('dividend' == yield_type or 'jcp' == yield_type or\
                'rendimentos_de_clientes' == yield_type or 'fracoes_de_acoes' == yield_type):
                raise ValueError (f'Invalid yield type "{yield_type}" '
                                'Choose either "Dividend", "JCP",  '
                                '"rendimentos_de_clientes", "fracoes_de_acoes')
            
            stock: str = input('\nStock (ex: EGIE3F) : ')
            if not stock: stock = 'None'
            date: str  = input('\nDate of the transaction (format: 2022-10-4) :')
            if not date: date =  datetime.now().strftime("%Y-%m-%#d")
            value: str  = input('\nTotal received value: ')
            if not value: value = 0
            data = {
                'broker' : broker,
                'yield_type' : str(yield_type),
                'stock' : str(stock),
                'date' : datetime.strptime(date, '%Y-%m-%d'),
                'value' : float(value)  
            }
            print('Adding received yield . . .')
            export_to_csv(data)
            export_to_db(data)

        if product_type == 'stocks':
            add_stock()
        elif product_type == 'yields':
            add_yield()
        else:
            raise ValueError(f'Product type "{product_type}" not available!')
    
    def get_report(product_type: str = 'stocks'):
        '''Generates query report from database
        Sends report to Statistics Class
        Generates report'''

        logger = logging.getLogger(get_report.__qualname__)
        logger.info(f'Product Type: {product_type}')
        
        mongo_db = Database()
        if product_type == 'stocks':
            threshold_date: str  =  input('[Buy] Treshold date (format %Y-%m-%d, Press enter if today): ')
            from_date: str  = input('[Sell] From date (format %Y-%m-%d, Press enter if 1st day of this year): ')
            to_date: str  =  input('[Sell] To date (format %Y-%m-%d, Press enter if today): ')
            broker: str  = input('Broker platform: ')
            report: list = mongo_db.genreport_stocks(threshold_date, broker)
            statistics: str  = Statistics(report)
            statistics.getreport_stocks(broker)
            #Sell Report
            sell_report: list = mongo_db.genreport_sold_stocks(from_date=from_date, to_date=to_date, broker=broker)
            statistics = Statistics(sell_report)
            statistics.getreport_sell(broker)
        elif product_type == 'yields':
            from_date: str  = input('From date (format %Y-%m-%d, Press enter if 1st day of this year): ')
            to_date: str  =  input('To date (format %Y-%m-%d, Press enter if today): ')
            broker: str  = input('Broker platform: ')
            report: list = mongo_db.genreport_yield(from_date=from_date, to_date=to_date, broker=broker)
            statistics = Statistics(report)
            statistics.getreport_yield(broker)
        else:
            raise ValueError(f'Invalid "{product_type}" Selection. '
                    'Please choose either stocks or yields')
    
    def get_statistics(product_type = 'stocks'):
        '''Generates query from database
        Sends it to Statistics class
        Generates statistics'''
        logger = logging.getLogger(get_statistics.__qualname__)
        logger.info(f'Product Type: {product_type}')
        
        portfolio: str  = input('Portifolio: ')
        broker: str  = input('Broker platform: ')
        mongo_db = Database()
        if product_type == 'yields':
            from_date: str  = input('From date (format %Y-%m-%d, Press enter if 1st day of this year): ')
            to_date: str  =  input('To date (format %Y-%m-%d, Press enter if today): ')
            report: list = mongo_db.genreport_yield(from_date=from_date, to_date=to_date, broker=broker)
            statistics = Statistics(report)
            statistics.generate_yields_summary(portfolio)
        elif product_type == 'stocks':
            threshold_date: str  =  input('Treshold date (format %Y-%m-%d, Press enter if today): ')
            report: list = mongo_db.genreport_stocks(threshold_date, broker)
            statistics = Statistics(report)
            statistics.generate_stocks_summary(portfolio)  
        else:
            raise ValueError(f'Invalid "{product_type}" Selection. '
                    'Please choose either stocks or yield')
    
    def update_from_csv(product_type = 'stocks'):
        logger = logging.getLogger(update_from_csv.__qualname__)
        logger.info(f'Product Type: {product_type}')
        '''Update databse from a CSV file'''
        csv_path : str = input('CSV PATH: ')
        csv_path = os.path.abspath(csv_path)
        #TODO: Check path
        data = Table.import_csv(csv_path)
        mongo_db = Database()
        mongo_db.drop_collection(product_type)
        for document in data:
            mongo_db.insert_data(document)

    def get_csv(product_type = 'stocks'):
        logger = logging.getLogger(get_csv.__qualname__)
        logger.info(f'Product Type: {product_type}')
        '''Export database collection as a CSV File'''
        csv_filename: str = input('CSV filename: ')
        mongo_db = Database()
        if product_type.lower() == 'stocks' or product_type.lower() == 'yields':
            if product_type.lower() == 'stocks':
                all_rows: list = mongo_db.find_all(product_type.lower())
            else:
                all_rows: list = mongo_db.find_all(product_type.lower())
            table = Table(all_rows)
            table.export_csv(csv_filename) 
            #TODO: Check creation     
        else:
            raise ValueError(f'Invalid "{product_type}" Selection. '
                    'Please choose either stocks or yield')
        

    operations = {
                    'add_transaction' : add_transaction,
                    'get_report'      : get_report,
                    'get_statistics'  : get_statistics,
                    'update_from_csv' : update_from_csv,
                    'get_csv'         : get_csv
                }

    
    operations[operation_type](product_type)
    


def export_to_csv(data) -> None:
    '''Export received data to CSV file
        through Table class'''
    table = Table(data)
    table.create_csv()

def export_to_db(data: dict) -> None:
    '''Export received data to CSV file
        through Table class'''
    mongo_db = Database()
    mongo_db.insert_data(data)

def main():
    logging.basicConfig(
                        format='%(asctime)s\t[%(name)s]\t[%(levelname)s]\t%(message)s',datefmt ="%Y-%m-%d %H:%M:%S%z",
                        level=logging.DEBUG,
                        handlers=[
                            logging.FileHandler('logs.log'),
                            logging.StreamHandler()
                            ]
                        )
    logger = logging.getLogger(__name__)
    parsed_args: argparse.Namespace = __parse_arguments__()
    logger.info('====START====')
    logger.debug(f'Arguments parsed: {parsed_args}')
    start = datetime.now()
    try:
        result = run_operations(operation_type = parsed_args.operation_type,
                                product_type = parsed_args.product_type)
        
    except Exception as e:
        print(str(e))
    finally:
        end = datetime.now()
        logger.info(f'Total time of execution: {(end - start).total_seconds()} seconds')
        logger.info('====END====')
        

def __parse_arguments__():
    '''Parse arguments from CLI'''
    __author__ = 'pbitts'
    __description = '''This project is intended to help beginners managing their investments:\n
                        - Store transaction (buy/sell stocks),\n
                        - Historical of yields\n
                        - Get a summary and statistics of the portfolio\n
                        - Facilitate IR Brazilian report information gathering ( it is just a hobby project, not intended to be a trusted source)\n'''
    parser = argparse.ArgumentParser(description=__description)
    parser.add_argument('-o','--operation_type', help='Operation type', dest='operation_type',
                        choices =('add_transaction', 'get_report', 'get_statistics', 
                                'update_from_csv', 'get_csv'), required=True)
    parser.add_argument('-p','--product_type', help='Product type', dest='product_type',
                        choices =('yields', 'stocks'), required=True)

    return parser.parse_args()

if __name__ == '__main__':
    main()
   
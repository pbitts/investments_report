import typing
import logging

from yfinance import Ticker

from database import Database
from create_table import Table
from config import portfolios, currency



class Statistics:

    def __init__(self, data):
        self.data =  data
        self.total_asset = 0
        self.current_total_asset = 0
    
    def generate_yields_summary(self, portfolio: str) -> None:
        '''This method generates yields summary.
        It gets stocks from selected portfolio, calculates:
        - total and monthly yields 
        - total yields per stock in the time period
        received from the database query at self.data. 
        It asks an input from user to get the export method.
        
        :param portfolio : The name of the registered portfolio at config.py'''
        import calendar

        logger = logging.getLogger(Statistics.generate_yields_summary.__qualname__)
        logger.info('Generating yields summary')

        monthly_performance = {}
        yields_per_stocks = []
        yield_per_stock = {}
        selected_stocks = portfolios.get(portfolio)

        if not selected_stocks:
            logger.info(f'Portfolio "{portfolio}" does not exist at config.py')
            return 
        elif '*' in selected_stocks:
            logger.info('All stocks selected')
            selected_stocks = []
            for row in self.data:
                selected_stocks.append(row['stock'])

        logger.info(f'Selected Stocks from portfolio {portfolio}: {selected_stocks}')
        for row in self.data:
            if row['stock'] in selected_stocks:
                month = row["date"].month
                monthly_performance[calendar.month_name[month]] =monthly_performance.get(calendar.month_name[month],0.0) \
                                                                + round(float(row["value"]),2) 
                yield_per_stock[row['stock']] = {
                                                'dividend': 0.0,
                                                'jcp': 0.0,
                                                'rendimentos_de_clientes': 0.0,
                                                'fracoes_de_acoes': 0.0
                                                }
        for row in self.data:
            if row['stock'] in selected_stocks:
                yield_per_stock[row['stock']][row['yield_type']] = yield_per_stock[row['stock']].get(row['yield_type'],0) + row['value']
        for key in yield_per_stock.keys():
            yields_per_stocks.append({
                                    'stock': key, 
                                    'dividend':  round(yield_per_stock[key].get('dividend', 0.0 ),2),
                                    'jcp':  round(yield_per_stock[key].get('jcp', 0.0 ),2),
                                    'rendimentos_de_clientes':  round(yield_per_stock[key].get('rendimentos_de_clientes', 0.0 ),2),
                                    'fracoes_de_acoes': round(yield_per_stock[key].get('fracoes_de_acoes', 0.0 ),2)
                                    })
        monthly_performance['average_per_month'] = round(sum(monthly_performance.values())/12, 2)
        export_to = input('\nYield general summary - Export to ( select print or csv): ')
        self.export(payload = [monthly_performance], export_to=export_to.lower())
        export_to = input('\nYield per stock summary - Export to ( select print or csv): ')
        self.export(payload = yields_per_stocks, export_to=export_to.lower())
        

    def generate_stocks_summary(self, portfolio: str)-> None:
        '''This method generates stocks summary.
        It gets stocks from selected portfolio, calculates:
        - total current/invested value and performance
        - current/invested value and performance value per stock, weight and quantity
        It asks an input from user to get the export method.
        
        :param portfolio : The name of the registered portfolio at config.py'''
        logger = logging.getLogger(Statistics.generate_stocks_summary.__qualname__)
        total_invested = 0
        current_total_value = 0
        stocks_summary = []

        selected_stocks = portfolios.get(portfolio)

        if not selected_stocks:
            logger.info(f'Portfolio "{portfolio}" does not exist at config.py')
            return 
        elif '*' in selected_stocks:
            logger.info('All stocks selected')
            selected_stocks = []
            for row in self.data:
                selected_stocks.append(row['stock'])

        logger.info(f'Selected Stocks from portfolio {portfolio}: {selected_stocks}')
        for row in self.data:
            if row['stock'] in selected_stocks:
                #Calculates total summary
                market_price = Ticker(row['stock']+'.SA').info['regularMarketPrice']
                if not market_price:
                    market_price = 0.0
                total_invested = total_invested + row['total']   
                current_total_value = current_total_value + market_price*row['quantity']
        performance = [round(current_total_value - total_invested,2),
                        round(current_total_value/total_invested - 1,2)*100]
        summary = {'total_invested': round(total_invested,2), 'current_total_value': round(current_total_value,2) ,
                    'performance': performance}

        for row in self.data:
        #Calculates individual stock 
            if row['stock'] in selected_stocks:  
                if row['quantity'] == 0:
                    pass
                else:    
                    market_price = Ticker(row["stock"]+".SA").info["regularMarketPrice"] 
                    if not market_price:
                        market_price = 0.0                                    
                    current_stock_value = market_price
                    total_current_stock_value =  round(current_stock_value*row["quantity"],2)
                    total_invested_stock_value = round(row["total"],2)
                    stock_weight = round(total_current_stock_value/current_total_value,2)*100
                    stock_performance = [round(total_current_stock_value - total_invested_stock_value,2), 
                                            round(total_current_stock_value/total_invested_stock_value - 1,2)*100]
                    
                    stock_summary = {
                                        'stock':row['stock'], 
                                        'current_stock_value': current_stock_value,
                                        'total_current_stock_value': total_current_stock_value,
                                        'total_invested_stock_value': total_invested_stock_value,
                                        'stock_weight': stock_weight,
                                        'stock_performance': stock_performance,
                                        'quantity': row['quantity']
                                    }
                    stocks_summary.append(stock_summary)
                
        export_to = input('\nGeneral Staticstics - Export to ( select print or csv): ')
        self.export([summary], export_to.lower())
        export_to = input('\nStocks Staticstics - Export to ( select print or csv): ')
        self.export(stocks_summary, export_to.lower())
        #return summary, stocks_summary

    def getreport_stocks(self, broker: str=None) -> None:
        '''This method generates info to Stocks IR report (Brazil)
        It asks an input from user to get the export method
        
        :param broker: The broker from where to generate this report'''
        logger = logging.getLogger(Statistics.getreport_stocks.__qualname__)
        logger.info(f'Stocks IR report at "{broker}"')
        export_to = input('Stocks report - Export to ( select print or csv): ')
        stocks_to_report = []
        for row in self.data:
            if row['quantity'] == 0:
                pass
            else:
                row['pm'] = round(float(row["total"])/int(row["quantity"]), 2)
                stocks_to_report.append(row)
        self.export(stocks_to_report, export_to.lower())

    def getreport_sell(self,broker: str=None) -> None:
        '''This method generates info to Sold Stocks IR report (Brazil)
        It asks an input from user to get the export method
        
        :param broker: The broker from where to generate this report'''
        logger = logging.getLogger(Statistics.getreport_sell.__qualname__)
        import calendar
        monthly_performance = {}# [0., 0., 0., 0., 0., 0.,  0., 0., 0., 0., 0., 0., 0., ]
        logger.info(f'Sold Stocks IR report at "{broker}": ')
        export_to = input('Sold Stocks report - Export to ( select print or csv): ')
        for row in self.data:
            month = row["date"].month
            #monthly_performance[month] += round(float(row["performance"]),2) 
            monthly_performance[calendar.month_name[month]] = monthly_performance.get(calendar.month_name[month], 0.0) \
                                                                + round(float(row['performance']),2)
            row_stock = { 'stock': row['stock'],
                            'date': str(row['date']),
                            'quantity': -row['quantity'],
                            'total_value' : -row['total_price'],
                            'performance' : round(float(row['performance']),2)
                        }
            self.export([row_stock], export_to.lower())
        logger.info(f'Sold Stocks Monthly Performance at "{broker}": ')
        export_to = input('Monthly Performance - Export to ( select print or csv): ')
        self.export([monthly_performance], export_to.lower())
           

    def getreport_yield(self, broker: str = None):
        '''This method generates info to Sold Stocks IR report (Brazil)
        Date - Stock - Type and value
        It asks an input from user to get the export method
        
        :param broker: The broker from where to generate this report'''
        logger = logging.getLogger(Statistics.getreport_yield.__qualname__)
        logger.info(f'Yield IR report at "{broker}": ')
        export_to = input('\nYield report - Export to ( select print or csv): ')
        self.export(payload = sorted(self.data, key=lambda d: d['yield_type']), export_to=export_to.lower())
         
    def export(self, payload: list = [] , export_to = None):
        #TODO: Implement export logic (db, api, document, etc)
        import calendar
        if export_to == 'print':
            for row in payload:
                keys = list(row.keys())
                print('\n')
                for key in keys:
                    if 'value' in key.lower() or 'performance' in key.lower() or 'dividend' in key.lower() \
                        or 'jcp' in key.lower() or 'rendimentos_de_clientes' in key.lower() \
                        or 'fracoes_de_acoes' in key.lower() or 'average' in key.lower():
                        if type(row[key]) == list:
                            print(f'{key.capitalize().replace("_"," ")}: {row[key][0]} {currency[1]}  | {row[key][1]} %')
                        else:
                            print(f'{key.capitalize().replace("_"," ")}: {row[key]} {currency[1]}')
                    elif 'weight' in key.lower():
                        print(f'{key.capitalize().replace("_"," ")}: {row[key]} %')
                    elif 'quantity' in key.lower():
                        print(f'{key.capitalize().replace("_"," ")}: {row[key]} shares')
                    elif 'type' in key.lower():
                        print(f'{key.capitalize().replace("_"," ")}: {row[key].capitalize().replace("_"," ")}')
                    elif key in list(calendar.month_name):
                        print(f'{key.capitalize().replace("_"," ")}: {round(row[key],2)} {currency[1]}')
                    else:
                        print(f'{key.capitalize().replace("_"," ")}: {row[key]} ')

        if export_to == 'csv':
            table = Table(payload)
            filename =  input('CSV Filename: ')
            table.export_csv(filename)
        if export_to == 'database':
            pass
        if export_to == 'api':
            return payload
        print('\n')
        return

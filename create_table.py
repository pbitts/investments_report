
import pandas as pd
from datetime import datetime



class Table():
    '''Class to handle methods related to csv table'''
    def __init__(self, data ) -> None:
        self.data = data

    def create_csv(self) -> None:
        '''Creates a csv filename specific for yields or specific for stocks
            Calls export_csv() to create and export the file'''
        if  'yield_type' in self.data.keys():
            filename: str = 'yields.csv'
        elif 'Sell' in self.data['transaction_type'] or 'Buy' in self.data['transaction_type']:
                filename: str = 'stocks.csv' 
        self.export_csv(filename)
        
     
    def export_csv(self, filename: str) -> None:
        '''Export self.data as CSV File
        
        :param filename: The desired name of the file'''
        from os.path import exists
        try:
            file_exists: bool = exists(filename)
            if type(self.data) == dict:
                df = pd.DataFrame.from_dict([self.data]) 
            elif type(self.data) == list:
                df = pd.DataFrame.from_dict(self.data) 
            if file_exists:
                df.to_csv (filename, sep=';' ,mode='a', index = False, header=False)
            else:
                df.to_csv (filename, sep=';' , index = False, header=True)
        except Exception as e:
            raise Exception('Erro exporting csv, reason: ', str(e))

    @staticmethod
    def import_csv(csv_path) -> list:
        '''From a CSV File, import its data translating some of its content
            to be standart with row's type used in this program'''
        from csv import DictReader

        data: list = []
        with open(csv_path , encoding='utf-8') as csvf: 
            csvReader = DictReader(csvf, delimiter = ';') 
            for row in csvReader: 
                if row.get('quantity'): row['quantity'] = int(row['quantity'])
                if row.get('price') : row['price'] = float(row['price'])
                if row.get('total_price'): row['total_price'] = float(row['total_price'])
                if row.get('date') : row['date'] = datetime.strptime(row['date'], '%Y-%m-%d')
                if row.get('value') : row['value'] = float(row['value'])
                if row.get('performance') : row['performance'] = float(row['performance'])
                data.append(row)
        return data
# investiments Report

## Who is it
This project is intended to help beginners managing their investiments:
- Store transaction (buy/sell stocks),
- Historical of yields
- Get a summary and statistics of the portfolio
- Facilitate IR Brazilian report information gathering ( it is just a hobby project, not intended to be a trusted source)

## Quick Start
First, you need a MongoDB Atlas (https://www.mongodb.com/cloud/atlas/register).
* Add your connection string to a .env or at the database.py's DB_CONNECTION variable at the __init__ function.
* Add your portfolios and currency, following the pattern at config.py
* Add your data to your MongoDB Atlas:
It can be done by :
    * using the command ```python main.py -o add_transaction -p yields``` or stocks instead. The program will ask input from the user.

## How to use
Commands are run via CLI on the program's directory:

**usage:** main.py [-h] -o {add_transaction,get_report,get_statistics,update_from_csv,get_csv} -p {yields,stocks}



**options:**
  -h, --help            show this help message and exit
  -o {add_transaction,get_report,get_statistics,update_from_csv,get_csv}, --operation_type {add_transaction,get_report,get_statistics,update_from_csv,get_csv}
                        Operation type
  -p {yields,stocks}, --product_type {yields,stocks}
                        Product type

All operations receive input from users. Don't worry, follow the flow.

* **add_transaction:** Adds to database and CSV file a new transaction (stocks or yields)
* **get_report:** Generates report from database, for stocks or yields
* **get_statistics:** Generates a summary/statistics of the current status of your stocks or yields.
* **update_from_cvs:** Update database from a CSV file. Must respect the fields.
* **get_csv:** Export database collection as a CSV File


*Documentation under construction...*
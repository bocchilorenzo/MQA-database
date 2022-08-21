# MQA-database

Create a local database of MQA albums from Tidal. This script relies on the csv files posted here: https://www.meridianunplugged.com/ubbthreads/ubbthreads.php?ubb=showflat&Number=268318

## Requirements

- Python 3
- MySQL

## Usage

Create a .env file in the root of the project with the following keys:

```
MYSQL_USER=
MYSQL_PASSWORD=
MYSQL_DB=
MYSQL_HOST=
```

After that, simply install the requirements and run the main.py file. The first run will take a lot of time (currently there are more than 500k MQA albums and singles combined), but after that a weekly update should be enough to keep the database up-to-date.

## Disclaimer

This project is meant for private use only. Make sure to read and not violate Tidal's terms and conditions before using this script.
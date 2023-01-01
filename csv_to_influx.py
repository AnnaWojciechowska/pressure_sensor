#Anna Wojciechowska, Oslo, December 2022

#  Script to process data from sensor generated csv files
#  The scripts reads all csv files in "sensor_data" directory and after successful read moved the file to "processed_data".
#  
#  This script is wrting to 
#  database: 'pressure_sensor'
#  measurement: 'pressure' 
#  tags: 'place'
#  fields: 'pressure_mbar' (unit mBars), 'temp_c' (unit degrees Celcius )
  
# TAGS
# +---------+--------+--------------+
# |tag name | place  | sensor_model |
# +---------+--------+--------------+
# | type    | string | int          |
# +---------+--------+--------------+
#sensor model is currently set to 0, place is not yet used, to be added in the future

# FIELDS
# +-------------+--------------+---------+
# |field name   | pressure_mbar| temp_c  |
# +-------------+--------------+---------
# | type        |    integer   | float   |
# +-------------+--------------+---------+


from influxdb import DataFrameClient

from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError
from requests.exceptions import Timeout, ConnectionError

from datetime import datetime

import pandas as pd
import numpy as np
import json

import sys
import os
import glob

import argparse

import traceback
import logging

from datetime import datetime as dt


def set_up_log(log_dir, log_filename):
    ''' creates a log in script running directory '''
    script_run_dir = os.getcwd()
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    #check if log dir exist, create both log dir and log file if necessary
    if not os.path.isdir(os.path.join(script_run_dir, log_dir)):
        os.mkdir(os.path.join(script_run_dir, log_dir))
    file_handler = logging.FileHandler(os.path.join(script_run_dir, log_dir, log_filename), mode='w')
    file_handler.setFormatter(logging.Formatter(fmt='%(asctime)s [%(pathname)s:%(lineno)d] [%(levelname)s] %(message)s', datefmt='%a, %d %b %Y %H:%M:%S'))
    logger = logging.getLogger()
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)
    return logger

def get_script_name():
    ''' gets a script name, log are named after the script '''
    script_path = sys.argv[0]
    path_elems = script_path.split('/')
    script_name = path_elems[len(path_elems) - 1]
    res = script_name.split('.py')
    return res[0]


def wirte_csv_to_influx(file_path, write_run): 
    ''' reads from csv at file_path and stores to influx '''
    ''' returns true if writen, together with datapoints count'''
    if os.stat(file_path).st_size > 0:
        df = pd.read_csv(file_path, skiprows=1) 
        if df.shape[0] > 0:
            df['sensor_model'] = 0
            df['frac_string'] = df['frac.seconds'].apply(lambda x: str(x))
            df['dt_string'] = df['DateTime'].apply(lambda x: str(x))
            df['time'] = pd.to_datetime( df['dt_string'] + '.' + df['frac_string'])
            df = df.drop(columns=['dt_string', 'frac_string', 'POSIXt', 'DateTime', 'frac.seconds' ])
            df.set_index('time', inplace=True)
            df = df.rename(columns={"Pressure.mbar": "pressure_mbar", "TempC": "temp_c"})
            #tutaj add exceptions?
            if write_run:
                try:
                    result = INFLUX_WRITE_CLIENT.write_points(df,'pressure',tag_columns = ['sensor_model'], field_columns = ['pressure_mbar', 'temp_c'],protocol='line')
                    if result:
                        return (result,df.shape[0])
                except ConnectionError:
                    # thrown if influxdb is down or (spelling) errors in connection configuration
                    LOGGER.error("ConnectionError, check connection setting and if influxdb is up: 'systemctl status influxdb'")
                    LOGGER.error(traceback.format_exc())
                    sys.exit(1)
                except Timeout:
                    LOGGER.error("Timeout, check influx timeout setting and network connection")
                    LOGGER.error(traceback.format_exc())
                except InfluxDBClientError:
                    LOGGER.error("InfluxDBClientError, check if database exist in influx: 'SHOW DATABASES'")
                    LOGGER.error(traceback.format_exc())
                    sys.exit(1)
                except InfluxDBServerError:
                    LOGGER.error("InfluxDBServerError")
                    LOGGER.error(traceback.format_exc())
                    sys.exit(1)
        else: 
            # no datapoints
            return (True,0)
    else: 
        #file is 0 size:
        return (True,0)
           
def process_csv(write_run):
    ''' reads all csv from DATA_DIR, after successful writing they are moved to PROCESSED_DIR '''
    SCRIPT_DIR = os.getcwd()
    DATA_DIR = 'sensor_data'
    if not os.path.exists(os.path.join(SCRIPT_DIR, DATA_DIR)):
        LOGGER.error("{} data folder is missng, aborting.".format(os.path.join(SCRIPT_DIR, DATA_DIR)))
        sys.exit(1)

    PROCESSED_DIR = 'sensor_processed'
    if not os.path.exists(os.path.join(SCRIPT_DIR,  PROCESSED_DIR)):
        os.mkdir(os.path.join(SCRIPT_DIR,  PROCESSED_DIR))

    os.chdir(DATA_DIR)
    files = glob.glob("*.csv")
    for f in files:
        start_processing = dt.now()
        full_file_path = os.path.join(SCRIPT_DIR, DATA_DIR, f)
        write_res = wirte_csv_to_influx(full_file_path, write_run)
        if (write_run and write_res[0]):
            dest_file_path = os.path.join(SCRIPT_DIR, PROCESSED_DIR, f)
            os.rename(full_file_path, dest_file_path)
            end_processing = dt.now()
            LOGGER.info("processed: {}".format(f))
            LOGGER.info("processed {} datapoints in: {}".format(write_res[1], end_processing - start_processing))
    LOGGER.info("total processed {} files".format(len(files)))


START_SCRIPT_TIME = dt.now()
LOGNAME = get_script_name() + '.log'
LOG_DIR = 'logs'
LOGGER = set_up_log(LOG_DIR, LOGNAME)

LOGGER.info("start script")

if not os.path.exists(os.path.join(os.getcwd(), 'influxdb_credentials')):
    LOGGER.error("influxdb_credentials file is missing")
    sys.exit(1)
influx_auth = json.load(open(os.path.join(os.getcwd(), 'influxdb_credentials')))

INFLUX_WRITE_CLIENT = DataFrameClient(
    host = 'localhost',
    port = 8086,
    database ='sensor',
    username = influx_auth['username'],
    password = influx_auth['password'])

parser = argparse.ArgumentParser()
parser.add_argument('-d', '--dry-run', action='store_true',
    help="does not write to database, just show result in csv file")
args = parser.parse_args()

process_csv(not args.dry_run)

LOGGER.info("end script script, duration: {}".format(dt.now() - START_SCRIPT_TIME))
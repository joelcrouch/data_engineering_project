# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import time
import psycopg2
import argparse
import re
import csv
import json
from datetime import datetime, timedelta

DBname = "postgres"
DBuser = "postgres"
DBpwd = "1234"
Datafile = "filedoesnotexist"  # name of the data file to be loaded
CreateDB = False  # indicates whether the DB table should be (re)-created

def calculate_timestamp(row):
  opd = datetime.strptime(row['OPD_DATE'], '%d%b%Y:%H:%M:%S')
  return opd + timedelta(seconds = row['ACT_TIME'])

def row2bc(row):
	for key in row:
		if not row[key]: # drop null values
			return None

	ret = f"""
		{calculate_timestamp(row)},		-- Timestamp
		{row['GPS_LATITUDE']},			-- Latitude
		{row['GPS_LONGITUDE']},			-- Longitude
		{row['SPEED']},					-- Speed
		{row['EVENT_NO_TRIP']},			-- Trip ID
	"""

	return ret

def row2trip(row):
	for key in row:
		if not row[key]: # drop null values
			return None

	ret = f"""
		{row['EVENT_NO_TRIP']},			-- Trip ID
		{row['ROUTE_ID']},				-- Route ID
		{row['VEHICLE_ID']},			-- Vehicle ID
		{row['SERVICE_KEY']},			-- Service Key
		{row['DIRECTION']},				-- Direction
	"""

	return ret


def initialize():
  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--datafile", required=True)
  parser.add_argument("-c", "--createtable", action="store_true")
  args = parser.parse_args()

  global Datafile
  Datafile = args.datafile
  global CreateDB
  CreateDB = args.createtable

# read the input data file into a list of row strings
def readdata(fname):
	print(f"readdata: reading from File: {fname}")
	with open(fname, mode="r") as fil:
		dr = json.load(fil)
		
		rowlist = []
		for row in dr:
			rowlist.append(row)

	return rowlist

# convert list of data rows into list of SQL 'INSERT INTO ...' commands
def getSQLcmnds(rowlist):
	cmdlist = []
	for index, row in rowlist:
		bc_val = row2bc(row)
		if bc_val:
			cmd = f"INSERT INTO BreadCrumb VALUES ({bc_val});"
			cmdlist.append(cmd)
		tr_val = row2trip(row)
		if tr_val:
			cmd = f"INSERT INTO Trip VALUES ({tr_val});"
			cmdlist.append(cmd)
	return cmdlist

# connect to the database
def dbconnect():
	connection = psycopg2.connect(
		host="localhost",
		database=DBname,
		user=DBuser,
		password=DBpwd,
	)
	connection.autocommit = True
	return connection

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):

	with conn.cursor() as cursor:

		cursor.execute(
			"""
				drop table if exists BreadCrumb;
				drop table if exists Trip;
				drop type if exists service_type;
				drop type if exists tripdir_type;

				create type service_type as enum ('Weekday', 'Saturday', 'Sunday');
				create type tripdir_type as enum ('Out', 'Back');

				create table Trip (
						trip_id integer,
						route_id integer,
						vehicle_id integer,
						service_key service_type,
						direction tripdir_type,
						PRIMARY KEY (trip_id)
				);

				create table BreadCrumb (
						tstamp timestamp,
						latitude float,
						longitude float,
						speed float,
						trip_id integer,
						FOREIGN KEY (trip_id) REFERENCES Trip
				);	
			"""
		)

		print(f"Created tables: BreadCrumb, Trip")

def load(conn, icmdlist):

	with conn.cursor() as cursor:
		print(f"Loading {len(icmdlist)} rows")
		start = time.perf_counter()
	
		for cmd in icmdlist:
			cursor.execute(cmd)

		elapsed = time.perf_counter() - start
		print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')


def main():
	initialize()
	conn = dbconnect()
	rlis = readdata(Datafile)
	cmdlist = getSQLcmnds(rlis)

	if CreateDB:
		createTable(conn)

	load(conn, cmdlist)


if __name__ == "__main__":
	main()
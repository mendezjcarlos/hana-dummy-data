import sys
import csv
import getopt
import json
import random
import string
from datetime import datetime, timedelta
from hdbcli import dbapi

from diccionarios.valores import plantas
from diccionarios.valores import materiales

def randomDec(p,s):
	return random.randint(0, (10**p)-1)/(10**s)



def randomStr(length):
	STR = string.ascii_uppercase
	return ''.join(random.choice(STR) for i in range(length))


def randomDate(start, end):
	start = datetime.strptime(str(start), '%Y%m%d')
	end = datetime.strptime(str(end), '%Y%m%d')
	return str(start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())),
    ))


def setVal(colDef):
	if colDef[1] == 'DECIMAL':
		return randomDec(colDef[2]-colDef[3], colDef[3])
	
	elif colDef[1] == 'NVARCHAR':
		return randomStr(colDef[2])

	elif colDef[1] == 'SMALLINT' or colDef[1] == 'INTEGER' or colDef[1] == 'DOUBLE' or colDef[1] == 'BIGINT':
		return random.randint(0,10**(colDef[2]-1)-1)

	elif colDef[1] == 'DATE' or colDef[1] == 'TIMESTAMP':
		return randomDate(20170101, 20181231)

	elif colDef[1] == 'PLANTA':
		return random.choice(plantas)

	elif colDef[1] == 'MATERIAL':
		return random.choice(materiales)

	elif colDef[1] == 'FECHALARGA':
		return str(DATE)

	elif colDef[1] == 'FECHACORTA':
		return str(DATE[0:6])

	else:
		return ''



def getVal(colDef, schema, table, exception):

	if schema in exception and table in exception[schema] and colDef[0] in exception[schema][table]:
		return setVal([table]+exception[schema][table][colDef[0]])

	else:
		return setVal(colDef)


def main():

	global DATE

	options = getopt.getopt(sys.argv[1:], 's:t:r:f:d:o')
	for op in options[0]:
		if op[0] == '-s':
			SCHEMA = op[1]
		elif op[0] == '-t':
			TABLE = op[1]
		elif op[0] == '-r':
			ROWS = int(op[1])
		elif op[0] == '-f':
			FILE = op[1]
		elif op[0] == '-d':
			DATE = op[1]
		
		if op[0] == '-o':
			OUT = True
		else:
			OUT = False


	connection = dbapi.connect(
	    address='hxehost',
	    port= 39015,
	    user='SYSTEM',
	    password='PASSWORD'
	)
	if connection.isconnected():
		print("Conexion establecida")
	else:
		print("Error de conexion")
	

	if OUT:
 		csvout = open("data/"+SCHEMA+"."+TABLE+"_"+DATE+".csv", 'w', newline='')
 		writer = csv.writer(csvout, delimiter='|')

	with open(FILE) as f:
		EXCEPTION = json.load(f)

	cursor = connection.cursor()

	QRY = " SELECT COLUMN_NAME, DATA_TYPE_NAME, LENGTH, SCALE "
	QRY += " FROM TABLE_COLUMNS WHERE SCHEMA_NAME ='"
	QRY += SCHEMA + "' AND TABLE_NAME ='"
	QRY += TABLE + "' ORDER BY POSITION ASC"

	cursor.execute(QRY)
	coldefinition = cursor.fetchall()
	
	T0 = datetime.now()
	print("=================", TABLE, "Inicio:", T0)

	for x in range(ROWS):

		if x % (ROWS/20) == 0:
			print( SCHEMA, TABLE, x/ROWS*100, '%')

		defVal = lambda p: getVal(p, SCHEMA, TABLE, EXCEPTION)
		rowVal = list(map(defVal, coldefinition))

		if OUT:
			writer.writerow(rowVal)
		else:
			QRYINS = 'INSERT INTO "'
			QRYINS += SCHEMA +'"."'+TABLE+'" VALUES ('
			QRYINS += ','.join( "'"+str(x)+"'" for x in rowVal )
			QRYINS += ")"
			
			cursor.execute(QRYINS)

	else:
		print( SCHEMA, TABLE, '100 %')
	
	if OUT:
		csvout.close()

	T1 = datetime.now()
	print("=================", "Fin:", T1, "\n")
	print("Duracion:", T1-T0)

	cursor.close()



if __name__ == '__main__':
	main()

#python genDummy.py -s SCHEMA -t TABLEA -f exceptions.json -r 1000 -d 20170101 -o
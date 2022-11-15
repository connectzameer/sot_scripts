import argparse
import json
import os
import psycopg2
import csv

########################################################################################################
parser = argparse.ArgumentParser()
parser.add_argument("-sql", "--sqlfile", help="To Execute the Script, Pass the SQL file path in the argument!", dest="sqlfile")
parser.add_argument("-json", "--json", help="Pass the JSON (DB List file) configuration file path in the argument!", dest="db_json_list")
parser.add_argument("-out", "--output", help="Mention the output file path, postgres db will be append in prefix", dest="output")
args = parser.parse_args()

# validating SQL File #
if args.sqlfile is None:
        print ("SQLFile is Missing in the argument!")
        exit()
else:
        if not os.path.exists(args.sqlfile):
                print ("The given SQL file path: \""+ args.sqlfile  + "\"is not available!")
                exit()
        else:
                if os.path.getsize(args.sqlfile) == 0:
                        print ("The given SQL File: \"" + args.sqlfile + "\" is Empty!")
                        exit()

# validating JSON file #
if args.db_json_list is None:
        print (" DB JSON CONFIG file is Missing in the argument!")
        exit()
else:
        if not os.path.exists(args.db_json_list):
                print ("The given DB JSON CONFIG file path: \""+ args.db_json_list  + "\"is not available!")
                exit()
        else:
                if os.path.getsize(args.db_json_list) == 0:
                        print ("The given DB JSON CONFIG FILE File: \"" + args.db_json_list + "\" is Empty!")
                        exit()
print (args.sqlfile)
print (args.db_json_list)
##############################################################################################################

def connect_db(database, host, port, user, password):
        print ("##",database, host, port, user, password)
        #establishing the connection
        output1= ""+ database + "_postgres.csv"
        print(output1)
        conn = None
        try:
                conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
                #Creating a cursor object using the cursor() method
                cursor = conn.cursor()#Executing an MYSQL function using the execute() method
                cursor.execute("select cobrand_id, name, is_channel,CHANNEL_ID,ENVIRONMENT,DEPLOYMENT_MODE from cobrand where cobrand_status_id=1")
                # Fetch a single row using fetchone() method.
                data = cursor.fetchall()
                #data.to_csv(output1)
                conn.close()
        except (Exception, psycopg2.DatabaseError) as error:
                print("Error: ", error)
        finally:
                if conn is not None:
                        conn.close()
                        print('Database: ' +database + 'connection closed.')

# Reading the JSON file
with open(args.db_json_list, 'r') as openfile:
        json_object = json.load(openfile)
#print(json_object)

for dblist in json_object["db_list"]:
        print(dblist["database"] , dblist["host"], dblist["port"], dblist["user"], dblist["password"])
        connect_db(dblist["database"], dblist["host"], dblist["port"], dblist["user"], dblist["password"])

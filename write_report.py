#!/usr/bin/env python

import MySQLdb
from sys import path, exit
path.append('/home/automation/scripts/clayScripts/resources')
from claylib import query_nomad_db, TELEMETRY_DICT

# GPS boundary box
maxlat = '<SNIPPED>'
minlat = '<SNIPPED>'
maxlong = '<SNIPPED>'
minlong = '<SNIPPED>'

speed_count = {0:0, 10:0, 20:0, 30:0, 40:0, 50:0, 60:0, 70:0, 80:0, 90:0, 100:0, 110:0, 120:0, 130:0, 140:0, 150:0}

try:
    telemetry_connection = MySQLdb.connect('<SNIPPED IP>', '<SNIPPED USER>', '<SNIPPED PASSWORD>', '<SNIPPED DB NAME>')
    cursor = telemetry_connection.cursor()
    print('Connected to db')
    try:
        for month in ['<SNIPPED LIST OF MONTHS>']:
            for day in ['<SNIPPED LIST OF DAYS']:
                print('Table:t_ccu_15%s%s' % (month, day))
                query = 'SELECT gps_speed, count(tstamp) FROM t_ccu_15%s%s WHERE (gps_lat BETWEEN %s AND %s) AND (gps_long BETWEEN %s AND %s) GROUP BY gps_speed ORDER BY gps_speed ASC;' % (month, day, minlat, maxlat, maxlong, minlong)
                cursor.execute(query)
                response = cursor.fetchall()
                print('# Rows retrieved:%d' % len(response))
                for line in response:
                    if line[0] >=150:
                        speed_count[150] += line[1]
                    elif line[0] >=140:
                        speed_count[140] += line[1]
                    elif line[0] >=130:
                        speed_count[130] += line[1]
                    elif line[0] >=120:
                        speed_count[120] += line[1]
                    elif line[0] >=110:
                        speed_count[110] += line[1]
                    elif line[0] >=100:
                        speed_count[100] += line[1]
                    elif line[0] >=90:
                        speed_count[90] += line[1]
                    elif line[0] >=80:
                        speed_count[80] += line[1]
                    elif line[0] >=70:
                        speed_count[70] += line[1]
                    elif line[0] >=60:
                        speed_count[60] += line[1]
                    elif line[0] >=50:
                        speed_count[50] += line[1]
                    elif line[0] >=40:
                        speed_count[40] += line[1]
                    elif line[0] >=30:
                        speed_count[30] += line[1]
                    elif line[0] >=20:
                        speed_count[20] += line[1]
                    elif line[0] >=10:
                        speed_count[10] += line[1]
                    else:
                        speed_count[0] += line[1]
        for speed in sorted(speed_count):
            print(speed, int(speed_count[speed]))
    except Exception, e:
        print('Problem querying db!')
        print(e)
except Exception:
    print('Problem connecting to telemetry db!')


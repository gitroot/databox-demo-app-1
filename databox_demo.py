# -*- coding: utf-8 -*-

# Databox push data example w/ multiple widget types & pulling data from local DB

import json
import urllib3
import mysql.connector

# Database parameters
db_hostname = '127.0.0.1'
db_database = 'databox_demo'

# Connection parameters
user_access_token = ""
source_token = "" 
push_url = "https://app.databox.com/push/custom/" + source_token

headers = urllib3.util.make_headers(basic_auth=user_access_token)
headers['Content-type'] = 'application/json'

# Initializations 
post_data = [] # All JSON data will be appended here before posting

cnx = mysql.connector.connect(user='root', password='', host=db_hostname, database=db_database)

# NOTE: In SQL queries below, all dates and non-integer numeric values are cast to strings,
# to avoid subsequent casting in code - as the Python JSON library cannot encode these values

#---------- WIDGET 1 / Main widget ----------
query = ("""
SELECT DATE_FORMAT(day, '%Y-%m-%d') AS day, 
CAST(SUM(revenue) AS CHAR) AS revenue 
FROM mshop_sales_day msd 
JOIN mshop_metrics mm ON mm.metric_id = msd.metric_id
WHERE day >= DATE_SUB(CURRENT_DATE, INTERVAL 14 DAY) AND day <= CURRENT_DATE 
AND metric_type = 'real'
GROUP BY DAY ORDER BY day;""")

cursor = cnx.cursor()
cursor.execute(query, ())

for row in cursor:
    main_total_rev_7d = {
               'key' : 'main_total_rev_7d',
               'date' : row[0],
               'value' : row[1]
               }
    post_data.append(main_total_rev_7d)

cursor.close()

#---------- WIDGET 2 / Big Number ----------
query = ("""
SELECT DATE_FORMAT(CURRENT_DATE, '%Y-%m-%d') AS day, 
CAST(SUM(revenue) AS CHAR) AS revenue 
FROM mshop_sales_day msd
JOIN mshop_metrics mm ON mm.metric_id = msd.metric_id
WHERE day >= DATE(CURDATE() - INTERVAL DAYOFYEAR(CURDATE()) DAY) AND day <= CURRENT_DATE 
AND metric_type = 'real';""")

cursor = cnx.cursor()
cursor.execute(query, ())

for row in cursor:
    bign_total_rev_ytd = {
               'key' : 'bign_total_rev_ytd',
               'date' : row[0],
               'value' : row[1]
               }
    post_data.append(bign_total_rev_ytd)

cursor.close()

#---------- WIDGET 3 / Compare ----------
query = ("""
SELECT DATE_FORMAT(day, '%Y-%m-%d') AS day, 
CAST(SUM(revenue) AS CHAR) AS  revenue 
FROM mshop_sales_day msd 
JOIN mshop_products mp ON mp.prod_id = msd.prod_id 
JOIN mshop_metrics mm ON mm.metric_id = msd.metric_id
WHERE day >= DATE_SUB(DATE_ADD(CURRENT_DATE, INTERVAL - DAY(CURRENT_DATE) + 1 DAY), INTERVAL 1 MONTH) AND day <= CURRENT_DATE 
AND prod_cat = 'Products' AND metric_type = 'real' 
GROUP BY DAY ORDER BY day;""")

cursor = cnx.cursor()
cursor.execute(query, ())

for row in cursor:
    comp_prod_rev_7d = {
               'key' : 'comp_prod_rev_7d',
               'date' : row[0],
               'value' : row[1]
               }
    post_data.append(comp_prod_rev_7d)

cursor.close()

#---------- WIDGET 4 / Bar Chart ----------
query = ("""
SELECT DATE_FORMAT(day, '%Y-%m-%d') AS day, 
CAST(SUM(revenue) AS CHAR) AS revenue 
FROM mshop_sales_day msd 
JOIN mshop_products mp ON mp.prod_id = msd.prod_id 
JOIN mshop_metrics mm ON mm.metric_id = msd.metric_id
WHERE day >= DATE_SUB(CURRENT_DATE, INTERVAL 14 DAY) AND day <= CURRENT_DATE 
AND prod_cat = 'Services' AND metric_type = 'real' 
GROUP BY DAY ORDER BY day;""")

cursor = cnx.cursor()
cursor.execute(query, ())

for row in cursor:
    barc_serv_rev_7d = {
               'key' : 'barc_serv_rev_7d',
               'date' : row[0],
               'value' : row[1]
               }
    post_data.append(barc_serv_rev_7d)

cursor.close()

#---------- WIDGET 5 / Interval Values ----------
query = ("""
SELECT DATE_FORMAT(day, '%Y-%m-%d') AS day, 
CAST(SUM(units) AS CHAR) AS units
FROM mshop_sales_day msd 
JOIN mshop_products mp ON mp.prod_id = msd.prod_id 
JOIN mshop_metrics mm ON mm.metric_id = msd.metric_id
WHERE day >= DATE_ADD(CURRENT_DATE, INTERVAL - DAY(CURRENT_DATE) + 1 DAY) AND day <= CURRENT_DATE 
AND prod_type = 'Electronics' AND metric_type = 'real' 
GROUP BY DAY ORDER BY day;""")

cursor = cnx.cursor()
cursor.execute(query, ())

for row in cursor:
    intv_elec_units_mtd = {
               'key' : 'intv_elec_units_mtd',
               'date' : row[0],
               'value' : row[1]
               }
    post_data.append(intv_elec_units_mtd)

cursor.close()

#---------- WIDGET 6 / Pipeline ----------
query = ("""
SELECT DATE_FORMAT(MAX(day), '%Y-%m-%d') AS day, 
CAST(SUM(vis_total) AS CHAR) AS vt, CAST(SUM(vis_unique) AS CHAR) AS vu, 
CAST(SUM(vis_active) AS CHAR) AS va, CAST((SUM(vis_p_carts) + SUM(vis_q_fill)) AS CHAR) AS vc 
FROM mshop_wsite_day mwd 
JOIN mshop_metrics mm ON mm.metric_id = mwd.metric_id
WHERE day >= DATE_ADD(CURRENT_DATE, INTERVAL - DAY(CURRENT_DATE) + 1 DAY) AND day <= CURRENT_DATE 
AND metric_type = 'real';""")

cursor = cnx.cursor()
cursor.execute(query, ())

date = ""

for row in cursor:
    date = row[0]
    pipe_wsite_act_mtd_v = {
               'key' : 'pipe_wsite_act_mtd@values',
               'date' : date,
               'value' : row[1:len(row)]
               }
    post_data.append(pipe_wsite_act_mtd_v)

cursor.close()

pipe_wsite_act_mtd_l = {
        'key' : 'pipe_wsite_act_mtd@labels',
        'date' : date,
        'value' : ["Visits", "Unique", "Active", "Carts"]
        }
post_data.append(pipe_wsite_act_mtd_l)

#---------- WIDGET 7 / Funnel ----------
query = ("""
SELECT DATE_FORMAT(MAX(day), '%Y-%m-%d') AS day, 
CAST(SUM(vis_p_carts) AS CHAR) AS vc, 
CAST(SUM(vis_p_chkout) AS CHAR) AS vo,
CAST(SUM(vis_p_sales) AS CHAR) AS vs 
FROM mshop_wsite_day mwd 
JOIN mshop_metrics mm ON mm.metric_id = mwd.metric_id
WHERE day >= DATE_ADD(CURRENT_DATE, INTERVAL - DAY(CURRENT_DATE) + 1 DAY) AND day <= CURRENT_DATE 
AND metric_type = 'real';""")

cursor = cnx.cursor()
cursor.execute(query, ())

date = ""

for row in cursor:
    date = row[0]
    funl_wsite_sales_mtd_v = {
               'key' : 'funl_wsite_sales_mtd@values',
               'date' : date,
               'value' : row[1:len(row)]
               }
    post_data.append(funl_wsite_sales_mtd_v)

cursor.close()

funl_wsite_sales_mtd_l = {
        'key' : 'funl_wsite_sales_mtd@labels',
        'date' : date,
        'value' : ["Carts", "Check-outs", "Sales"]
        }
post_data.append(funl_wsite_sales_mtd_l)

#---------- WIDGET 8 / Table ----------
query = ("""
SELECT DATE_FORMAT(MAX(day), '%Y-%m-%d') AS day, prod_name, 
CAST(SUM(revenue) AS CHAR) AS revenue, 
CAST(SUM((price - cost) * units) AS CHAR) AS profit,
CAST(MAX(price) AS CHAR) AS unit_price, 
CAST(MAX(cost) AS CHAR) AS unit_cost, 
CAST(SUM(units) AS CHAR) AS units_sold
FROM mshop_sales_day msd 
JOIN mshop_products mp ON mp.prod_id = msd.prod_id 
JOIN mshop_metrics mm ON mm.metric_id = msd.metric_id
WHERE day >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY) AND day <= CURRENT_DATE 
AND prod_cat = 'Products' AND metric_type = 'real'
GROUP BY mp.prod_id ORDER BY mp.prod_id;""")

cursor = cnx.cursor()
cursor.execute(query, ())

date = ""
values = []

for row in cursor:
    date = row[0]
    values.append(list(row[1:len(row)]))

tabl_all_metrics_30d_v = {
        'key' : 'tabl_all_metrics_30d@rows',
        'date' : date,
        'value' : values
        }
post_data.append(tabl_all_metrics_30d_v)

cursor.close()

tabl_all_metrics_30d_l = {
        'key' : 'tabl_all_metrics_30d@columns',
        'date' : date,
        'value' : ["Product", "Revenue", "Profit", "Unit Price", "Unit Cost", "Units Sold"]
        }
post_data.append(tabl_all_metrics_30d_l)

#---------- WIDGET 9 / Pie Chart ----------
query = ("""
SELECT DATE_FORMAT(MAX(day), '%Y-%m-%d') AS day, prod_name, 
CAST(SUM(revenue) AS CHAR) AS revenue
FROM mshop_sales_day msd 
JOIN mshop_products mp ON mp.prod_id = msd.prod_id 
JOIN mshop_metrics mm ON mm.metric_id = msd.metric_id
WHERE day >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY) AND day <= CURRENT_DATE 
AND metric_type = 'real'
GROUP BY mp.prod_id ORDER BY mp.prod_id;""")

cursor = cnx.cursor()
cursor.execute(query, ())

date = ""
labels = []
values = []

for row in cursor:
    date = row[0]
    labels.append(row[1])
    values.append(row[2])
    
piec_prod_rev_30d_v = {
        'key' : 'piec_prod_rev_30d@values',
        'date' : date,
        'value' : values
        }
post_data.append(piec_prod_rev_30d_v)

piec_prod_rev_30d_l = {
        'key' : 'piec_prod_rev_30d@labels',
        'date' : date,
        'value' : labels
        }
post_data.append(piec_prod_rev_30d_l)

cursor.close()

#---------- WIDGET 10 / Progress ----------
query = ("""
SELECT DATE_FORMAT(MAX(day), '%Y-%m-%d') AS day, metric_type, 
CAST((SUM(CASE WHEN (metric_type = 'real' AND day <= CURRENT_DATE) OR metric_type = 'projected' THEN revenue ELSE 0.0 END)) AS CHAR) AS revenue 
FROM mshop_sales_day msd 
JOIN mshop_metrics mm ON mm.metric_id = msd.metric_id
WHERE day >= DATE_ADD(CURRENT_DATE, INTERVAL - DAY(CURRENT_DATE) + 1 DAY) AND day <= LAST_DAY(CURRENT_DATE) 
GROUP BY metric_type ORDER BY mm.metric_id;""")

cursor = cnx.cursor()
cursor.execute(query, ())

date = ""

for row in cursor:
    date = row[0]
    
    if(row[1] == "real") :
        prog_tot_rev_mtd_v = {
               'key' : 'prog_tot_rev_mtd',
               'date' : date,
               'value' : row[2]
               }
        post_data.append(prog_tot_rev_mtd_v)
    else :
        prog_tot_rev_mtd_m = {
               'key' : 'prog_tot_rev_mtd@max_value',
               'date' : date,
               'value' : row[2]
               }
        post_data.append(prog_tot_rev_mtd_m)
    
prog_tot_rev_mtd_l = {
        'key' : 'prog_tot_rev_mtd@label',
        'date' : date,
        'value' : 'MTD'
        }
post_data.append(prog_tot_rev_mtd_l)

cursor.close()

# Cleanup
cnx.commit()
cnx.close()

# Push data to server
data = {
    "data": post_data
}

json_data = json.dumps(data)
print(json_data)

http = urllib3.PoolManager()
response = http.urlopen('POST', push_url, body=json_data, headers=headers)

print("HTTP status  = %s" % response.status)
print("HTTP headers = %s" % response.headers)
print("HTTP data    = %s" % response.data)

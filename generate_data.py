# -*- coding: utf-8 -*-

# Generate in local DB one year worth of product/sales data for a fictitious company 
# Premise: Company w/ product & services sales w/ day/week/month seasonal factors

import random
import math
import mysql.connector

db_hostname = '127.0.0.1'
db_database = 'databox_demo'
db_company_name = 'mshop'

db_table_metrics   = db_company_name + '_metrics'
db_table_products  = db_company_name + '_products'
db_table_sales_day = db_company_name + '_sales_day'
db_table_wsite_day = db_company_name + '_wsite_day'

sql_drop_metrics   = "DROP TABLE IF EXISTS " + db_table_metrics + ";"
sql_drop_products  = "DROP TABLE IF EXISTS " + db_table_products + ";"
sql_drop_sales_day = "DROP TABLE IF EXISTS " + db_table_sales_day + ";"
sql_drop_wsite_day = "DROP TABLE IF EXISTS " + db_table_wsite_day + ";"

sql_create_metrics = "CREATE TABLE " + db_table_metrics + """ (
metric_id INT PRIMARY KEY,
metric_type VARCHAR(32) NOT NULL,
metric_desc VARCHAR(256)
);"""

sql_create_products = "CREATE TABLE " + db_table_products + """ (
prod_id INT PRIMARY KEY,
prod_cat VARCHAR(32) NOT NULL,
prod_type VARCHAR(32) NOT NULL,
prod_name VARCHAR(64) NOT NULL,
prod_desc VARCHAR(256)
);"""

sql_alter_products = "ALTER TABLE " + db_table_products + " ADD UNIQUE KEY(prod_cat, prod_type, prod_name);"

sql_create_sales_day = "CREATE TABLE " + db_table_sales_day + """ (
day DATE NOT NULL,
prod_id INT NOT NULL,
metric_id INT NOT NULL,
revenue DECIMAL(12,2), price DECIMAL(12,2), cost DECIMAL(12,2), units INT
);"""

sql_alter_sales_day = "ALTER TABLE " + db_table_sales_day + " ADD PRIMARY KEY(day, prod_id, metric_id);"

sql_create_wsite_day = "CREATE TABLE " + db_table_wsite_day + """ (
day DATE NOT NULL,
metric_id INT NOT NULL,
vis_total INT, vis_unique INT, vis_active INT,
vis_p_carts INT, vis_p_chkout INT, vis_p_sales INT,
vis_q_fill INT, vis_q_submit INT, vis_q_accept INT
);"""

sql_alter_wsite_day = "ALTER TABLE " + db_table_wsite_day + " ADD PRIMARY KEY(day, metric_id);"

sql_insert_metrics = "INSERT INTO " + db_table_metrics + """ (
metric_id, metric_type, metric_desc) VALUES
(0, 'real', NULL),
(1, 'projected', NULL);"""

sql_insert_products = "INSERT INTO " + db_table_products + """ (
prod_id, prod_cat, prod_type, prod_name, prod_desc) VALUES
(0, 'Products', 'Accessories', 'Wall Chargers', NULL),
(1, 'Products', 'Accessories', 'Memory Cards', NULL),
(2, 'Products', 'Electronics', 'Media Players', NULL),
(3, 'Products', 'Electronics', 'GPS Units', NULL),
(4, 'Products', 'Electronics', 'Tablets 10in', NULL),
(5, 'Services', 'Consulting', 'Home Cinemas', NULL);"""

sql_insert_sales_day = "INSERT INTO " + db_table_sales_day + """ (
day, prod_id, metric_id, revenue, price, cost, units)
VALUES (%s,%s,%s,%s,%s,%s,%s);"""

sql_insert_wsite_day = "INSERT INTO " + db_table_wsite_day + """ (
day, metric_id, vis_total, vis_unique, vis_active,
vis_p_carts, vis_p_chkout, vis_p_sales, vis_q_fill, vis_q_submit, vis_q_accept)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

#--------------------------------------------------
# Revenue metrics
year_revenue_proj = 10000000.00

month_coeff_base = [20,16,22,25,20,16,12,10,20,25,22,16] # Seasonal / per-month-of-year factors 
week_coeff_base = [35,25,20,15,10] # Seasonal / per-week-of-month factors
day_coeff_base = [17,20,23,17,13,7,3] # Seasonal / per-day-of-week factors

month_coeff_delta = 0.25 # Percent allowed factor variation +/-
week_coeff_delta = 0.25 # Percent allowed factor variation +/-
day_coeff_delta = 0.25 # Percent allowed factor variation +/-

year_months = 12 # Months in a year
month_days = [31,28,31,30,31,30,31,31,30,31,30,31] # Days in each month Jan - Dec
week_days = 7 # Days in a week

month_coeff_avg = sum(month_coeff_base) / len(month_coeff_base) # Calculated

day_split_coeff_base = [5.00, 5.00, 10.00, 15.00, 45.00, 20.00] # Revenue split between products and services
day_split_coeff_delta = 0.25 # Percent allowed split variation +/-
day_split_price_base = [10.00, 20.00, 55.00, 90.00, 300.00, 100.00] # Base price of unit sold or hour billed
day_split_price_delta = 0.10 # Percent allowed price variation +/-
day_split_cost_base = [6.00, 12.00, 32.00, 46.00, 190.00, 40.00] # Base cost of unit sold or hour billed
day_split_cost_delta = 0.05 # Percent allowed cost variation +/-

# Website metrics
year_visit_proj = 250000000

day_visit_base = [100.00, 80.00, 30.00] # Funnel in %: Visits -> Unique Visitors -> Browsing
day_visit_delta = 0.20 # % funnel variation
day_ws_prod_base = [5.00, 20.00, 50.00]    # Funnel in %: Fill Carts -> Checkout -> Complete Sale
day_ws_prod_delta = 0.20 # % funnel variation
day_ws_serv_base = [0.15, 20.00, 10.00]    # Funnel in %: Begin Inquiry -> Submit -> Accept Quote 
day_ws_serv_delta = 0.20 # % funnel variation

#--------------------------------------------------
random.seed()

# MySQL local DB
cnx = mysql.connector.connect(user='', password='', host=db_hostname, database=db_database)

cursor = cnx.cursor()
cursor.execute(sql_drop_metrics, ())
cursor.execute(sql_drop_products, ())
cursor.execute(sql_drop_sales_day, ())
cursor.execute(sql_drop_wsite_day, ())
cnx.commit()
cursor.execute(sql_create_metrics, ())
cursor.execute(sql_create_products, ())
cursor.execute(sql_create_sales_day, ())
cursor.execute(sql_create_wsite_day, ())
cnx.commit()
cursor.execute(sql_alter_products, ())
cursor.execute(sql_alter_sales_day, ())
cursor.execute(sql_alter_wsite_day, ())
cnx.commit()
cursor.execute(sql_insert_metrics, ())
cursor.execute(sql_insert_products, ())
cnx.commit()

month_coeff_real = [0.0] * len(month_coeff_base)
month = 0
while(month < len(month_coeff_base)) :
    month_coeff_real[month] = max(month_coeff_base[month] * (1 + random.uniform(-month_coeff_delta, month_coeff_delta)), 0.0)
    month += 1

month = 0
while(month < len(month_coeff_base)) :
    week_coeff_real = [0.0] * len(week_coeff_base)
    week = 0
    while(week < math.ceil(month_days[month] / week_days)) :
        week_days_real = max(month_days[month] - week * week_days, week_days);
        week_coeff_real[week] = max(week_coeff_base[week] * (week_days_real / week_days) * (1 + random.uniform(-week_coeff_delta, week_coeff_delta)), 0.0)
        week += 1
    
    day_coeff_real = [0.0] * month_days[month]
    day = 0
    while(day < month_days[month]) :
        day_coeff_real[day] = max(day_coeff_base[day % week_days] * (1 + random.uniform(-day_coeff_delta, day_coeff_delta)), 0.0)
        day += 1

    week = -1
    day = 0
    while(day < month_days[month]) :
        if(day % week_days == 0) :
            week += 1
        date_str = '%04d-%02d-%02d' % (2014, month + 1, day + 1)
            
        first_wk_day = day - day % week_days
        
        # Final coefficients for the day, actual and projected
        coeff_real = (month_coeff_real[month] / sum(month_coeff_base)) * (week_coeff_real[week] / sum(week_coeff_real)) * (day_coeff_real[day] / sum(day_coeff_real[first_wk_day : first_wk_day + week_days - 1]))
        coeff_proj = (month_coeff_base[month] / sum(month_coeff_base)) * (week_coeff_base[week] / sum(week_coeff_base)) * (day_coeff_base[day % week_days] / sum(day_coeff_base))

        day_revenue_real = year_revenue_proj * coeff_real
        day_revenue_proj = year_revenue_proj * coeff_proj
        
        day_split_coeff_real = [0.0] * len(day_split_coeff_base)
        split = 0
        while(split < len(day_split_coeff_base)) :
            day_split_coeff_real[split] = max(day_split_coeff_base[split] * (1 + random.uniform(-day_split_coeff_delta, day_split_coeff_delta)), 0.0)
            split += 1
        
        day_split_price_real = [0.0] * len(day_split_price_base)
        day_split_price_proj = [0.0] * len(day_split_price_base)
        split = 0
        while(split < len(day_split_price_base)) :
            day_split_price_real[split] = max(day_split_price_base[split] * (1 + random.uniform(-day_split_price_delta, day_split_price_delta)), 0.0)
            day_split_price_proj[split] = day_split_price_base[split]
            split += 1

        day_split_cost_real = [0.0] * len(day_split_cost_base)
        day_split_cost_proj = [0.0] * len(day_split_cost_base)
        split = 0
        while(split < len(day_split_cost_base)) :
            day_split_cost_real[split] = max(day_split_cost_base[split] * (1 + random.uniform(-day_split_cost_delta, day_split_cost_delta)), 0.0)
            day_split_cost_proj[split] = day_split_cost_base[split]
            split += 1

        day_split_rev_real = [0.0] * len(day_split_coeff_base)
        day_split_rev_proj = [0.0] * len(day_split_coeff_base)
        split = 0
        while(split < len(day_split_coeff_base)) :
            day_split_rev_real[split] = day_revenue_real * day_split_coeff_real[split] / sum(day_split_coeff_real)
            day_split_rev_proj[split] = day_revenue_proj * day_split_coeff_base[split] / sum(day_split_coeff_base)
            split += 1

        day_split_units_real = [0.0] * len(day_split_price_base)
        day_split_units_proj = [0.0] * len(day_split_price_base)
        split = 0
        while(split < len(day_split_price_base)) :
            day_split_units_real[split] = math.ceil(day_split_rev_real[split] / day_split_price_real[split])
            day_split_price_real[split] = day_split_rev_real[split] / day_split_units_real[split] # Re-calculate
            day_split_units_proj[split] = math.ceil(day_split_rev_proj[split] / day_split_price_base[split])
            day_split_price_proj[split] = day_split_rev_proj[split] / day_split_units_proj[split] # Re-calculate
            split += 1

        # Website metrics calculations
        day_visit_real = [0.0] * len(day_visit_base)
        day_visit_proj = [0.0] * len(day_visit_base)
        prev_step_val_real = year_visit_proj * coeff_real
        prev_step_val_proj = year_visit_proj * coeff_proj
        step = 0
        while(step < len(day_visit_base)) : 
            day_visit_real[step] = prev_step_val_real * max((day_visit_base[step] / 100.0) * (1 + random.uniform(-day_visit_delta, day_visit_delta)), 0.0)
            day_visit_proj[step] = prev_step_val_proj * (day_visit_base[step] / 100.0)
            prev_step_val_real = day_visit_real[step]
            prev_step_val_proj = day_visit_proj[step]
            step += 1

        day_ws_prod_real = [0.0] * len(day_ws_prod_base)
        day_ws_prod_proj = [0.0] * len(day_ws_prod_base)
        step = 0
        prev_step_val_real = day_visit_real[-1]
        prev_step_val_proj = day_visit_proj[-1]
        while(step < len(day_ws_prod_base)) : 
            day_ws_prod_real[step] = prev_step_val_real * max((day_ws_prod_base[step] / 100.0) * (1 + random.uniform(-day_ws_prod_delta, day_ws_prod_delta)), 0.0)
            day_ws_prod_proj[step] = prev_step_val_proj * (day_ws_prod_base[step] / 100.0)
            prev_step_val_real = day_ws_prod_real[step]
            prev_step_val_proj = day_ws_prod_proj[step]
            step += 1

        day_ws_serv_real = [0.0] * len(day_ws_serv_base)
        day_ws_serv_proj = [0.0] * len(day_ws_serv_base)
        step = 0
        prev_step_val_real = day_visit_real[-1]
        prev_step_val_proj = day_visit_proj[-1]
        while(step < len(day_ws_serv_base)) : 
            day_ws_serv_real[step] = prev_step_val_real * max((day_ws_serv_base[step] / 100.0) * (1 + random.uniform(-day_ws_serv_delta, day_ws_serv_delta)), 0.0)
            day_ws_serv_proj[step] = prev_step_val_proj * (day_ws_serv_base[step] / 100.0)
            prev_step_val_real = day_ws_serv_real[step]
            prev_step_val_proj = day_ws_serv_proj[step]
            step += 1

        # Insert values in database
        split = 0
        while(split < len(day_split_coeff_base)) :
            cursor.execute(sql_insert_sales_day, (date_str, split, 0, 
                                                  day_split_rev_real[split], day_split_price_real[split],
                                                  day_split_cost_real[split], day_split_units_real[split]))
            cursor.execute(sql_insert_sales_day, (date_str, split, 1, 
                                                  day_split_rev_proj[split], day_split_price_proj[split],
                                                  day_split_cost_proj[split], day_split_units_proj[split]))
            split += 1

        # day, metric_id, vis_total, vis_unique, vis_active,
        # vis_p_carts, vis_p_chkout, vis_p_sales, vis_q_fill, vis_q_submit, vis_q_accept)
        
        cursor.execute(sql_insert_wsite_day, (date_str, 0, 
                                              day_visit_real[0], day_visit_real[1], day_visit_real[2],
                                              day_ws_prod_real[0], day_ws_prod_real[1], day_ws_prod_real[2],
                                              day_ws_serv_real[0], day_ws_serv_real[1], day_ws_serv_real[2]))
        cursor.execute(sql_insert_wsite_day, (date_str, 1, 
                                              day_visit_proj[0], day_visit_proj[1], day_visit_proj[2],
                                              day_ws_prod_proj[0], day_ws_prod_proj[1], day_ws_prod_proj[2],
                                              day_ws_serv_proj[0], day_ws_serv_proj[1], day_ws_serv_proj[2]))

        day += 1
    month += 1

cnx.commit()
cursor.close()
cnx.close()
#--------------------------------------------------

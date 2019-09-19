#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 24 11:42:51 2019

@author: training2
"""

import time 
import string
import random
import pyspark.sql.functions as F

User_ID_Columns = ['user_id']
User_ID_Table = 'user_id'

Personal_Details_Columns = ['user_id','name','email','date_of_birth','country',
                            'address_line_1','address_line_2','city','post_code']
Personal_Details_Table = 'personal_details'

Bank_Details_Columns = ['user_id','acc_number','sort_code','name','currency']
Bank_Details_Table = 'bank_details'

Table_Details = {User_ID_Table : User_ID_Columns,
                 Personal_Details_Table : Personal_Details_Columns,
                 Bank_Details_Table : Bank_Details_Columns}

New_User_Questions = ['Enter full name: ',
                      'Enter email address: ',
                      'Enter date of birth (yyyy-mm-dd): ', 
                      'Enter country: ',
                      'Enter address line 1: ',
                      'Enter address line 2: ',
                      'Enter city of residence: ',
                      'Enter post/zip code: '
                      ]

New_Bank_Questions = ['Enter account number: ',
                      'Enter sort code: ',
                      'Enter name on account: ', 
                      'Enter currency type: '
                      ]
  

def create_sample_data():
  
  ids = ['MKPXIA3809','ZLWJKS8140','SHIZCF0984','EMJGXM8166']
  id_sample_data = spark.createDataFrame(ids, User_ID_Columns)
  id_sample_data.write.format('parquet').mode('overwrite').saveAsTable(User_ID_Table)
  
  personals = [('MKPXIA3809', 'Alice Jones', 'aj@gmail.com','1990-01-01', 'United Kingdom', '12 Mill Lane', '', 'Newport', 'NP111PN'), 
               ('ZLWJKS8140', 'Robert Smith', 'rs@gmail.com', '1991-02-02', 'United Kingdom', '13 Mill Lane', '', 'Newport', 'NP111PN'),
               ('SHIZCF0984', 'Charlotte Cooper', 'cc@gmail.com', '1992-03-03', 'United Kingdom', '14 Mill Lane', '', 'Newport', 'NP111PN'),
               ('EMJGXM8166', 'Daniel Price', 'dp@gmail.com', '1993-04-04', 'United Kingdom', '15 Mill Lane', '', 'Newport', 'NP111PN')]
  personal_sample_data = spark.createDataFrame(personals, Personal_Details_Columns)
  personal_sample_data.write.format('parquet').mode('overwrite').saveAsTable(Personal_Details_Table)

  banks = [('MKPXIA3809', 12345678, 203212, 'Alice Jones', 'GDP'),
           ('ZLWJKS8140', 24512315, 422321, 'Bob Smith', 'GDP'),
           ('SHIZCF0984', 76326171, 848212, 'Charlotte Cooper', 'GDP'),
           ('EMJGXM8166', 19874611, 452152, 'Daniel Price', 'GDP')]
  bank_sample_data = spark.createDataFrame(banks, Bank_Details_Columns)
  bank_sample_data.write.format('parquet').mode('overwrite').saveAsTable(Bank_Details_Table)                




def generate_new_id_value():
  
  seed = int(10000 * (time.monotonic() - int(time.monotonic())))
  random.seed(seed)
  
  id_numeric = randint(0, 9999)
  #convert to str and pad to 4 digits
  id_num_as_str = str(id_numeric)
  if (len(id_num_as_str) < 4):
    id_num_as_str = '0' * (4 - len(id_num_as_str)) + id_num_as_str
    
  id_alpha = ''.join(random.choices(string.ascii_uppercase, k = 6))
  
  user_id = id_alpha + str(id_numeric)
  
  print('Creating unique ID')
  print(user_id)
  
  user_data = spark.sql('SELECT * FROM user_id') 
  repeat_user_id = user_data.where(user_data[User_ID_Columns[0]] == user_id).count()
  
  if(repeat_user_id):
    user_id = generate_new_id_value()
  
  
  insert_user_id(user_id)
  
  return user_id  


def insert_user_id(user_id):
  
  df = spark.createDataFrame([user_id], User_ID_Columns)
  
  df.write.format('parquet').mode('append').saveAsTable(User_ID_Table)
  
  
def add_new_inputted_data(user_id, questions):
  
  results = [user_id]
  
  for q in questions:
    results.append(input(q))
    
  return results

def add_new_user(user_id):
  
  results = [user_id]
  
  for q in New_User_Questions:
    results.append(input(q))
  
  return results


def add_bank_details(user_id):
  
  results = [user_id]
  
  for q in New_Bank_Questions:
    results.append(input(q))
    
  return results
  

def insert_record(user_id, record, table):
  
  col_names = Table_Details[table]
  df = spark.createDataFrame(record, col_names)
  
  df.write.format('parquet').mode('append').saveAsTable(table)
  
  return df


def edit_details(user_id, table, questions):
  
  results = ['']
  for q in questions:
    replace_answer = input('Edit {0} (y/n) '.format(q[6:]))
    if(replace_answer.lower()[0] == 'y'):
      results.append(input(q))
    else:
      results.append('')
  
  
  entire_data, editing_record = get_original_data(user_id, table)
  
  for idx, column in enumerate(editing_record.columns):
    if(results[idx] != ''):
      editing_record = editing_record.withColumn(column, F.lit(results[idx]))
      
  edit_record(user_id, editing_record, entire_data, table)


def get_original_data(user_id, table):
     
  sql_string = 'SELECT * FROM ' + table
  original_df = spark.sql(sql_string)
  
  old_record = original_df.where(original_df['user_id'] == user_id)
  
  return original_df, old_record
  

 def edit_record(user_id, new_record, data, table):
  
  new_data = data.where(data['user_id'] != user_id).union(new_record)
  
  new_data.write.format('parquet').mode('overwrite').saveAsTable(table)


def add_user():
  
  user_id = generate_new_id_value()
  
  new_user = insert_record(user_id, add_new_user(user_id), Personal_Details_Table)
  
  bank_details = insert_record(user_id, add_bank_details(user_id), Bank_Details_Table)


def edit_user_details(user_id):
  edit_details(user_id, Personal_Details_Table, New_User_Questions)
  
  
def edit_bank_details(user_id):
  edit_details(user_id, Bank_Details_Table, New_Bank_Questions)
  
  
 

  
  
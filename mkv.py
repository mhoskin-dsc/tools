#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug  9 10:47:33 2019

@author: training2
"""

#mkv


import pandas as pd
import numpy as np
np.random.seed(1)

text = 'the quick brown fox jumped over the lazy dog the the the the the dog the dog'

text_list = text.lower().split(' ')
unique_words = set(text_list)
unique_list = list(unique_words)
dict_locations = {word:unique_list.index(word) for word in unique_list}

df = pd.DataFrame(columns = unique_words)

empty_np = np.zeros(shape=(len(unique_words), len(unique_words) + 2))

for word in unique_list:
  row_index = dict_locations[word]
  
  flag_next = 0
  for sample_word in text_list:
    if flag_next == 1:
      col_index = dict_locations[sample_word]
      empty_np[row_index, col_index] += 1
    if word == sample_word:
      flag_next = 1
    else:
      flag_next = 0

first_word = text_list[0]
empty_np[dict_locations[first_word], len(unique_words)] += 1
      
last_word = text_list[len(text_list)-1]
empty_np[dict_locations[last_word], len(unique_words) + 1] += 1

empty_np

df = pd.DataFrame(data = empty_np, columns = unique_list + ['FIRST','END'], index = unique_list)

first = df[['FIRST']]
n_first_values = sum(first)[0]

first.loc[:,'norm'] = first.loc[:,'FIRST'] / n_first_values
first = first.sort_values('norm')
first['cum_sum'] = first["norm"].cumsum()
first

rnd_start = np.random.random(1)[0]
start_word = first[first['cum_sum'] > rnd_start].index[0]





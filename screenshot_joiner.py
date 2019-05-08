#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  8 10:34:04 2019

@author: mhoskin-dsc
"""
import matplotlib.pyplot as plt
from PIL import Image
import glob
import numpy as np
import os

directory = ''

#read in and store in a list
idx = 0 
im = []
for image_name in sorted(glob.glob(directory + 'Screenshot*.png')):
  print(image_name)
  if image_name not in ['python.png','R.png']:
    im.append(np.array(Image.open(image_name)))
    print(np.shape(im[idx]))
    
    idx += 1
  
print(len(im))  


  
#for each pair, elongate with whitespace if different widths

out_im = []
for i in range(int(len(im)/2)):
  #print(i)
  
  shape_python = np.shape(im[2*i])
  shape_r = np.shape(im[2*i + 1])
  
  diff = shape_python[1] - shape_r[1]
  
  if diff > 0:
    #extend r
    print(i, diff)
    #add python to list
    out_im.append(im[2*i])
    if diff % 2 == 0:
      #put half on both sides of R
      blank_cols = np.full((shape_r[0], int(diff/2), 4), 255)
      out_im.append(np.hstack((blank_cols, im[2*i + 1], blank_cols)))
    else:
      #put 1 + half of (total - 1) on left, half of total - 1 on right 
      blank_cols_left = np.full((shape_r[0], 1 + int((diff - 1)/2), 4), 255)
      blank_cols_right = np.full((shape_r[0], int((diff - 1)/2), 4), 255)
      out_im.append(np.hstack((blank_cols_left, im[2*i + 1], blank_cols_right)))
    
  elif diff < 0:
    #extend py
    print(i, diff)
    diff = abs(diff)
    if diff % 2 == 0:
      blank_cols = np.full((shape_python[0], int(diff/2), 4), 255)
      out_im.append(np.hstack((blank_cols, im[2*i], blank_cols)))
    else:
      blank_cols_left = np.full((shape_python[0], 1 + int((diff - 1)/2), 4), 255)
      blank_cols_right = np.full((shape_python[0], int((diff - 1)/2), 4), 255)
      out_im.append(np.hstack((blank_cols_left, im[2*i], blank_cols_right)))
    
    #add r to list
    out_im.append(im[2*i + 1])
  
  else:
    out_im.append(im[2*i])
    out_im.append(im[2*i + 1])
  
#Check same, should print nothing out  
for i in range(int(len(out_im)/2)):
  #print(i)
  
  shape_python = np.shape(out_im[2*i])
  shape_r = np.shape(out_im[2*i + 1])
  
  diff = shape_python[1] - shape_r[1]
  if diff != 0:
    print(i, diff)


#Add Python/R headings and join vertically

r = np.array(Image.open(directory + 'R.png'))
py = np.array(Image.open(directory + 'python.png'))

joined_ims = []
for i in range(int(len(out_im)/2)):
  #elongate R/py flag to width required
  width_needed = np.shape(out_im[2*i])[1]
  curr_height = np.shape(r)[0]
  curr_width = np.shape(r)[1]
  
  r_ext = np.hstack((r, np.full((curr_height, width_needed - curr_width, 4), 255)))
  py_ext = np.hstack((py, np.full((curr_height, width_needed - curr_width, 4), 255)))
  
  joined_ims.append(
      np.vstack((py_ext, 
                 out_im[2*i],
                 r_ext, 
                 out_im[2*i+1]))
      )
    
#Save image, requires output folder to exist, so creates if doesn't exist

if not os.path.exists(directory + 'output'):
  os.makedirs(directory + 'output')
  
for idx, image in enumerate(joined_ims):
  im_to_save = Image.fromarray(image.astype(np.uint8))
  im_to_save.save(directory + 'output/joined_image' + str(idx) + '.png')
  



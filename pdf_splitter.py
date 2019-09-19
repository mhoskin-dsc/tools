#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 29 09:35:12 2019

@author: training2
"""





from PyPDF2 import PdfFileReader, PdfFileWriter
idx = 0
with open("receipts_all.pdf", 'rb') as infile:
    for i in range(100):
      
  
      reader = PdfFileReader(infile)
      writer = PdfFileWriter()
      writer.addPage(reader.getPage(idx))
      
      fn = 'output' + str(idx) + '.pdf'
      idx += 1
      print(idx)
      with open(fn, 'wb') as outfile:
          writer.write(outfile)
          

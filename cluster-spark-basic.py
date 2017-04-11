# -*- coding: utf-8 -*-
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
import argparse
import json
import yaml
import hashlib
import os
import nibabel
import sys
from time import gmtime, strftime

conf = SparkConf() \
       .setMaster("local[*]") \
       .setAppName('spark-basic')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

parser = argparse.ArgumentParser()
parser.add_argument("--r", help="root_directory")
parser.add_argument("--d",help="Dictionary")
args = parser.parse_args()
ngrams=None
dictionary={}
if args.r:
    root_dir = args.r
    print root_dir
if args.d:
    json_dictionary = args.d


def compute_ssd(x):
    print "Enter:",strftime("%Y-%m-%d %H:%M:%S", gmtime())
    file_path_1 = root_dir+"/"+x[0]+"/"+x[2]+"/"+x[3]
    file_path_2 = root_dir+"/"+x[1]+"/"+x[2]+"/"+x[3]
    im1 = nibabel.load(file_path_1)
    im2 = nibabel.load(file_path_2)
        
    # Check that both images have the same dimensions
    shape1 = im1.header.get_data_shape()
    shape2 = im2.header.get_data_shape()
    if shape1 != shape2:
        log_error("Images don't have the same shape!")

    data1 = im1.get_data()
    data2 = im2.get_data()
    
    xdim = shape1[0]
    ydim = shape1[1]
    zdim = shape1[2]
    ssd=0
    if len(shape1) == 4:
      tdim = shape1[3]
      # Go through all the voxels and get the SSD
      for x in range(0,xdim):
            for y in range(0,ydim):
              for z in range(0,zdim):
                  for t in range(0,tdim):
                      ssd += (data1[x][y][z][t]-data2[x][y][z][t])**2
    else:
      for x in range(0,xdim):
          for y in range(0,ydim):
              for z in range(0,zdim):
                  ssd += (data1[x][y][z]-data2[x][y][z])**2

    # That's it!
    print "Exit:",strftime("%Y-%m-%d %H:%M:%S", gmtime())
    return ssd
	


def get_checksum(path_name):
     hasher=hashlib.md5()
     if os.path.isfile(path_name):
          md5_sum=file_hash(hasher,path_name)
     return md5_sum

def file_hash(hasher,file_name):
    file_content=open(file_name)
    while True:
      read_buffer=file_content.read(2**20)
      if len(read_buffer)==0:
        break
      hasher.update(read_buffer)
    file_content.close()
    return hasher.hexdigest()

def find_if_different(x):
    print x
    if len(x) == 6:
      if x[4]== x[5]:
	return 0,0
      else:
	if ".txt" not in x[3] and ".mat" not in x[3]:
	  print "Computing ssd using checksum"
	  return 1,compute_ssd(x)
      return 1,0
    else:
      print "Computing checksum locally"
      checksum_1=get_checksum(root_dir+"/"+x[0]+"/"+x[2]+"/"+x[3])
      checksum_2=get_checksum(root_dir+"/"+x[1]+"/"+x[2]+"/"+x[3])
      if checksum_1!=checksum_2:
      	 if ".txt" not in x[3] and ".mat" not in x[3]:
	    print "Computing ssd with difference found locally"
	    return 1,compute_ssd(x)
      return 1,0
    return 0,0

    

text_file = sc.textFile("comparisons.txt")


all_pairs = text_file.map(lambda x:x.split(",")) \
       .map(lambda x:(x,find_if_different(x))) \
       .filter(lambda x:True if x[1][0] == 1 else False) \
       .map(lambda x:(str(x[0][0]+"_"+x[0][1]+"_"+x[0][3]),x[1])) \
       .reduceByKey(lambda x, y:(x[0]+y[0],x[1]+y[1])) \
       .collect()

print all_pairs
print len(all_pairs)

def map_values(conditions_list,subjects_list,file_names):
    for condition in conditions_list:
	for subject in subjects_list:
	  mapped_values=file_names.flatMap(lambda x:[condition,subject]).collect()
   	  print mapped_values
	

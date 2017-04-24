# -*- coding: utf-8 -*-
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
import argparse
import logging
import json
import yaml
import hashlib
import os
import nibabel
import sys
import commands
from time import gmtime, strftime
import subprocess
import shutil

#Spark configuration, tuning parameters !!
conf = SparkConf() \
       .setMaster("local[*]") \
       .set("spark.executor.cores","8") \
       .set("spark.executor.instances","2")\
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

#Function to compute ssd
#Input is a pair of conditions and a common file
#def compute_ssd(x):
    #print "Enter:",strftime("%Y-%m-%d %H:%M:%S", gmtime())
    #file_path_1 = root_dir+"/"+x[0]+"/"+x[2]+"/"+x[3]
    #file_path_2 = root_dir+"/"+x[1]+"/"+x[2]+"/"+x[3]
    #im1 = nibabel.load(file_path_1)
    #im2 = nibabel.load(file_path_2)
        
    # Check that both images have the same dimensions
    #shape1 = im1.header.get_data_shape()
    #shape2 = im2.header.get_data_shape()
    #if shape1 != shape2:
        #log_error("Images don't have the same shape!")

    #data1 = im1.get_data()
    #data2 = im2.get_data()
    
    #xdim = shape1[0]
    #ydim = shape1[1]
    #zdim = shape1[2]
    #ssd=0
    #if len(shape1) == 4:
      #tdim = shape1[3]
      # Go through all the voxels and get the SSD
      #for x in range(0,xdim):
            #for y in range(0,ydim):
              #for z in range(0,zdim):
                  #for t in range(0,tdim):
                      #ssd += (data1[x][y][z][t]-data2[x][y][z][t])**2
    #else:
      #for x in range(0,xdim):
          #for y in range(0,ydim):
              #for z in range(0,zdim):
                  #ssd += (data1[x][y][z]-data2[x][y][z])**2

    # That's it!
    #print "Exit:",strftime("%Y-%m-%d %H:%M:%S", gmtime())
    #return ssd
	
#Compute mse
def compute_mse(x):
   file_path_1 = root_dir+"/"+x[0]+"/"+x[2]+"/"+x[3]
   file_path_2 = root_dir+"/"+x[1]+"/"+x[2]+"/"+x[3]
   command_string = "./mse.sh"+" "+file_path_1+" "+file_path_2+" "+"2>/dev/tty"
   return_value,output = commands.getstatusoutput(command_string)
   if return_value != 0:
     log_error(str(return_value)+" "+ str(output) +" "+"Command "+ "mse.sh" +" failed ("+command_string+").")
   return output


#Mse using fslmaths
#def compute_mse_sh(x):
  #file_path_1 = root_dir+"/"+x[0]+"/"+x[2]+"/"+x[3]
  #file_path_2 = root_dir+"/"+x[1]+"/"+x[2]+"/"+x[3]  
  #diff="diff-XXXX.nii.gz"
  #sqr="sqr-XXXX.nii.gz"
  #diff=subprocess.check_output(["mktemp",diff])
  #sqr=subprocess.check_output(["mktemp",sqr])
  #print diff,sqr
  #subprocess.call(["fslmaths",file_path_1,"-sub",file_path_2,"-sqr",diff])
  #mean=subprocess.check_output(["fslstats",diff,"-m"])
  #print mean
  #os.remove(diff.replace("\n",""))
  #os.remove(sqr.replace("\n",""))
  #print "Removed folders"
  #return mean.replace("\n","")

#Log Error
def log_error(message):
  logging.error("ERROR: " + message)
  sys.exit(1)



#Function to compute checksum locally
def get_checksum(path_name):
     hasher=hashlib.md5()
     if os.path.isfile(path_name):
          md5_sum=file_hash(hasher,path_name)
     return md5_sum

#Function to read the contents for creating the checksum
def file_hash(hasher,file_name):
    file_content=open(file_name)
    while True:
      read_buffer=file_content.read(2**20)
      if len(read_buffer)==0:
        break
      hasher.update(read_buffer)
    file_content.close()
    return hasher.hexdigest()

#Function to find if files differ in their checksum
def find_if_different(x):
    if len(x) == 6:
      #print "Checksums from file"
      checksum_1=x[4]
      checksum_2=x[5]

    else:
      #print "Computing checksum locally"
      checksum_1=get_checksum(root_dir+"/"+x[0]+"/"+x[2]+"/"+x[3])
      checksum_2=get_checksum(root_dir+"/"+x[1]+"/"+x[2]+"/"+x[3])
    if checksum_1!=checksum_2:
      if ".txt" not in x[3] and ".mat" not in x[3]:
        print "Computing mean squared error value"
	return 1,float(compute_mse(x))
      return 1,0
    return 0,0

#To do,check why the reducebykey is not doing the summation operation
    
#Read input file
text_file = sc.textFile("comparisons.txt")

#Spark map and reduce jobs
all_pairs = text_file.map(lambda x:x.split(",")) \
       .map(lambda x:(x,find_if_different(x))) \
       .filter(lambda x:True if x[1][0] == 1 else False) \
       .map(lambda x:(str(x[0][0]+"_"+x[0][1]+"_"+x[0][3]),x[1])) \
       .reduceByKey(lambda x, y:(x[0]+y[0],x[1]+y[1])) \
       .collect()

print all_pairs

	

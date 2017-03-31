# -*- coding: utf-8 -*-
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
import argparse
import json
import yaml
import hashlib
import os

conf = SparkConf()
conf.setAppName('spark-basic')
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



def get_checksum(path_name):
     hasher=hashlib.md5()
     if os.path.isfile(path_name):
          md5_sum=file_hash(hasher,path_name)
     #elif os.path.isdir(path_name):
          #md5_sum=directory_hash(hasher,path_name)
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
    #print x
    #print root_dir+"/"+x[0]+"/"+x[2]+"/"+x[3]
    #m = hashlib.md5()
    #m.update(x)
    
    checksum_1=get_checksum(root_dir+"/"+x[0]+"/"+x[2]+"/"+x[3])
    checksum_2=get_checksum(root_dir+"/"+x[1]+"/"+x[2]+"/"+x[3])
    if checksum_1!=checksum_2:
	return 1
    return 0

    
    return x

#dictionary=yaml.safe_load(json_dictionary)

#print "****************************"

#print ngrams

#sqlContext.createDataFrame(dictionary).collect()

#condition = sc.parallelize(dictionary)

text_file = sc.textFile("comparsions.txt")


all_pairs = text_file.map(lambda x:x.split(",")) \
       .map(lambda x:(x,find_if_different(x))) \
       .filter(lambda x:True if x[1] == 1 else False) \
       .map(lambda x:(str(x[0][0]+"_"+x[0][1]+"_"+x[0][3]),x[1])) \
       .reduceByKey(lambda x, y: x+y ) \
       .collect()

print all_pairs

#print dictionary['OS1'][dictionary['OS1'].keys()[0]]

#conditions_list = condition.collect()
#print conditions_list

#subjects_list=condition.map(lambda x:dictionary[x].keys()).collect()
#subjects_list=subjects_list[0]
#print subjects_list

#file_names=condition.flatMap(lambda x:dictionary[x][dictionary[x].keys()[0]])
#print file_names

def map_values(conditions_list,subjects_list,file_names):
    for condition in conditions_list:
	for subject in subjects_list:
	  mapped_values=file_names.flatMap(lambda x:[condition,subject]).collect()
   	  print mapped_values
	
#map_values(conditions_list,subjects_list,file_names)

#files=condition.map(lambda x:dictionary[x][dictionary[x].keys()[0]]).collect()
#print files


#print(str(rdd))

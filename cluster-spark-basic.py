# -*- coding: utf-8 -*-
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
import argparse
import json
import yaml
import hashlib

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



def checksum(path_name):
     hasher=hashlib.md5()
     if os.path.isfile(path_name):
          md5_sum=file_hash(hasher,path_name)
     elif os.path.isdir(path_name):
          md5_sum=directory_hash(hasher,path_name)
     return md5_sum

def find_checksum(x):
    print x
    print root_dir+"/"+x[0]+"/"+x[2]+"/"+x[3]
    #m = hashlib.md5()
    #m.update(x)
    return x#+","+m.hexdigest

#dictionary=yaml.safe_load(json_dictionary)

#print "****************************"

#print ngrams

#sqlContext.createDataFrame(dictionary).collect()

#condition = sc.parallelize(dictionary)

text_file = sc.textFile("comparsions.txt")


all_pairs = text_file.map(lambda x:x.split(",")) \
       .filter(lambda x:find_checksum(x)) \
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
	  mapped_values=file_names.map(lambda x:[condition,subject,x]).collect()
   	  print mapped_values
	
#map_values(conditions_list,subjects_list,file_names)

#files=condition.map(lambda x:dictionary[x][dictionary[x].keys()[0]]).collect()
#print files


#print(str(rdd))

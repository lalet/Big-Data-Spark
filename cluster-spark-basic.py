# -*- coding: utf-8 -*-
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
import argparse
import json
import yaml

conf = SparkConf()
conf.setAppName('spark-basic')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

parser = argparse.ArgumentParser()
parser.add_argument("--r", help="some useful description.")
parser.add_argument("--d",help="Dictionary")
args = parser.parse_args()
ngrams=None
dictionary={}
if args.r:
    ngrams = args.r
if args.d:
    json_dictionary = args.d


dictionary=yaml.safe_load(json_dictionary)

#print "****************************"

#print ngrams

#sqlContext.createDataFrame(dictionary).collect()
condition = sc.parallelize(dictionary)

conditions_list = condition.collect()
print conditions_list

subjects_list=condition.map(lambda x:dictionary[x].keys()).collect()
subjects_list=subjects_list[0]
print subjects_list

file_names=condition.flatMap(lambda x:dictionary[x][dictionary[x].keys()[0]])
#print file_names

def map_values(conditions_list,subjects_list,file_names):
    for condition in conditions_list:
	for subject in subjects_list:
		mapped_values=file_names.map(lambda x:[condition,subject,x]).collect()
   		print mapped_values
	
map_values(conditions_list,subjects_list,file_names)

#files=condition.map(lambda x:dictionary[x][dictionary[x].keys()[0]]).collect()
#print files


#print(str(rdd))

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import codecs
import json
import happybase

connection = happybase.Connection('10.1.94.57')
tab = connection.table('aaron_memex_ht-images')

#s=codecs.open("/Users/amandeep/ads-image-avro-schema.avsc",'r')
reader = DataFileReader(open('/Users/amandeep/Downloads/part-m-00000-2.avro','r'),DatumReader())
for line in reader:
	#print line
	#print "images id:" + str(line["images_id"])
	row = tab.rows(str(line["images_id"]))
	print 	row[0][1]
	jsonO={}
	jsonO["ads_id"]=line["ads_id"]
	#jsonO["columbia_near_dups"]=row[1]['meta:columbia_near_dups']
	jsonO["original_url"]
	jsonO["columbia_near_dups_dist"]
	jsonO["ads_url"]=line['ads_url']
	jsonO["s3_url"]
	jsonO["images_id"]=line['images_id']
	


	#print row[1]['meta:columbia_near_dups']#,row[1]["meta:columbia_near_dups"]
	#break
reader.close()

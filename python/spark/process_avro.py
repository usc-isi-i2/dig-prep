import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import codecs
import json
import happybase



def processlines(inputset):
        keys = inputset.keys()
        rows = tab.rows(keys)
        for row in rows: 
                value = inputset[row[0]].split('TABSS')
                if 'meta:columbia_near_dups' in row[1] and 'meta:columbia_near_dups_dist' in row[1]:
                        jsonO={}
                        ads_url = value[1]
                        jsonO["ads_id"]=value[0]
                        jsonO["columbia_near_dups"]=row[1]['meta:columbia_near_dups']
                        jsonO["original_url"]=row[1]['meta:url']
                        jsonO["columbia_near_dups_dist"]=row[1]['meta:columbia_near_dups_dist']
                        jsonO["ads_url"]=ads_url
                        jsonO["s3_url"]=row[1]['meta:location']
                        jsonO["images_id"]=key
                        print row[0],key
                        o.write(ads_url + '\t' + json.dumps(jsonO))
                        o.write('\n')

connection = happybase.Connection('10.1.94.57')
tab = connection.table('aaron_memex_ht-images')

#s=codecs.open("/Users/amandeep/ads-image-avro-schema.avsc",'r')
o=codecs.open("/mnt/dig-lsh2/ht/ads-images/output/ads-image-mapping-input-for-karma",'w','utf-8')
#o=codecs.open("/mnt/dig-lsh2/ht/ads-images/output/ads-image-mapping-input-for-karma",'w','utf-8')
reader = DataFileReader(open('/mnt/dig-lsh2/ht/ads-images/input-avro/ads_images/part-m-00000.avro','r'),DatumReader())
#reader = DataFileReader(open('/mnt/dig-lsh2/ht/ads-images/input-avro/sample/part-m-00000.avro','r'),DatumReader())
input_lines=100000
counter=0
inputset={}
TABSS = '<TAB>'
for line in reader:
        if counter > input_lines:
                processlines(inputset)
                inputset={}
                counter=0
        else
                inputset[str(line["images_id"])]=str(line["ads_id"]) + TABSS + line["ads_url"]
                counter = counter + 1

o.close()

        #print row[1]['meta:columbia_near_dups']#,row[1]["meta:columbia_near_dups"]
        #break
reader.close()
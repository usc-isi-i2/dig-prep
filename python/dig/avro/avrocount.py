import fastavro as avro

k = 0

with open('/mnt/resource/avro_temp/part-m-00000.avro', 'rb') as fo:
    reader = avro.reader(fo)
    schema = reader.schema

    for record in reader:
        k += 1
        if k%10000==0:
            print k

print k


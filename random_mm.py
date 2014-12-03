import sys
from math import sqrt, log, ceil
from time import time
from random import seed, random, randrange, randint

from elasticsearch import Elasticsearch

# ES_HOST = { 
#     "host" : "localhost", 
#     "port" : 9200 
# }

ES_HOST = { 
    "host" : "59e7f4604f20f503000.qbox.io", 
    "port" : 80 
}

def createMatrixIndex(es_client, index_name, shards):
    if es_client.indices.exists(index_name):
        print("deleting '%s' index..." % (index_name))
        print(es_client.indices.delete(index = index_name, ignore=[400, 404]))

    request_body = {
        "settings" : {
            "number_of_shards": shards,
            "number_of_replicas": 1
        }
    }

    print("creating '%s' index..." % (index_name))
    res = es_client.indices.create(index = index_name, body = request_body)
    print(" response: '%s'" % (res))


def createRandomSparseMatrixES(es_client, index_name, n, elem_max, shards, density):
    createMatrixIndex(es_client, index_name, shards)
    bulk_data = [] 
    for pt_num in xrange(int(round(density * n**2))):
        
        i = randint(0, n-1)
        j = randint(0, n-1)

        cell_val = randrange(-elem_max, elem_max, 1)

        if cell_val == 0:
            continue

        bulk_data.append({
            "index": {
                "_index": index_name, 
                "_type": 'elem', 
                "_id": '%s-%s' % (i,j)
            }
        })
        bulk_data.append({
            'row': i+1,
            'col': j+1,
            'val': cell_val
        })

        if len(bulk_data) > 10000:
            res = es_client.bulk(index = index_name, body = bulk_data, refresh = True)
            bulk_data = []
        
    if len(bulk_data) > 0:
        res = es_client.bulk(index = index_name, body = bulk_data, refresh = True)


        
#from subprocess import call
from os import system

if __name__ == '__main__':

    val_max = 10
    shards = 5

    seed(time())

    es_client = Elasticsearch(hosts = [ES_HOST])

    n_vals = sorted( [10**(p+2) for p in xrange(6)] + [3*10**(p+2) for p in xrange(6)] )

    #for N in [3*10**6, 10**7, 3*10**7, 10**8, 3*10**8]:
    for N in n_vals:

        start_time = time()

        # 0.5, 1, 1.5, 2, 3
        D = 1 * log(N, 2)**4 / N**2

        print('\nN = %s\nD* = %s' % (N, D))

        createMatrixIndex(es_client, 'matrix-c1', shards)

        createRandomSparseMatrixES(es_client, 'matrix-a1', N, val_max, shards, D)

        createRandomSparseMatrixES(es_client, 'matrix-b1', N, val_max, shards, D)

        matA_count = es_client.count(index='matrix-a1', doc_type='elem')['count']
        matB_count = es_client.count(index='matrix-b1', doc_type='elem')['count']

        D = (matA_count + matB_count) / float(2 * N**2)

        G = int(round(sqrt(sqrt(D * N**2))))
        GRP_SIZE = int(ceil(N / float(G)))

        print('N = %s' % N)
        print('N^2 = %s' % N**2)
        print('D = %s' % D)
        print('D * N^2 = %s' % int(round(D * N**2)))
        print('G: %s' % G)
        print('GRP_SIZE: %s' % GRP_SIZE)
        
        es_client.index(index='matrix-mult-stats', doc_type='param', id=N, body={ 'n': N, 'time': int(1000*time()), 'd': D }, refresh=True)

        elapsed = round(time() - start_time, 2)
        print("--- %s seconds ---" % elapsed)

        system("~/spark/bin/spark-submit --master local[4] --jars ~/spark/jars/elasticsearch-hadoop-2.1.0.Beta2.jar ~/local_code/es-spark-matmult/es_spark_mm.py")
        # system("~/spark/bin/spark-submit --master local --jars ~/spark/jars/elasticsearch-hadoop-2.1.0.Beta2.jar ~/es-spark-matmult/es_spark_mm.py")



# from math import log, ceil, sqrt
# print('ceil(sqrt(N * sqrt(D / 5)) / 2)     int(log(N)**2) / 2')
# for N in sorted( [10**(p+2) for p in xrange(10)] + [3*10**(p+2) for p in xrange(10)] ):
#     D = 2 * log(N, 2)**4 / N**2
#     print('N: %.2e, N^2: %.0e, D: %.8e, D*N^2: %s (%s)' % (N, N**2, D, int(D*N**2), N))
#     G = int(round(sqrt(sqrt(D * N**2 / 2))))
#     if G <= 0:
#         G = 1
#     print('G: %s, G^2: %s, D*N^2/G^2: %s, N/G: %s  (%s)' % (G, G**2, int(D*N**2/G**2), int(ceil(N / float(G))), N))


# D = 0.613
# for n in n_vals:
#     D = 3 * log(N, 2)**4 / N**2

#     G = int(round(sqrt(sqrt(D * n**2)))) / 2
#     GRP_SIZE = int(ceil(n / float(G)))
#     print('N: %s, N*N: %s, D*N*N: %s, G: %s, D*N*N/(GG): %s' % (n, n**2, D*n**2, G, D*n**2/G**2))

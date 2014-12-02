from pyspark import SparkContext, SparkConf
# import json
from math import log, ceil, sqrt
from time import time

from elasticsearch import Elasticsearch

# ES_HOST = { 
#     "host" : "localhost", 
#     "port" : 9200 
# }

ES_HOST = { 
    "host" : "59e7f4604f20f503000.qbox.io", 
    "port" : 80 
}

NUM_TASKS = 20

if __name__ == "__main__":

    start_time = time()

    es_client = Elasticsearch(hosts = [ES_HOST])

    res = es_client.search(index='matrix-mult-stats',doc_type='param',sort='time:desc',size=1)

    N = res['hits']['hits'][0]['_source']['n']

    conf = SparkConf().setAppName("ESSparkMM")
    sc = SparkContext(conf=conf)

    # es_conf = {
    #     "es.nodes" : "localhost",
    #     "es.port" : "9200"
    # } 

    # es_conf = {
    #     "es.nodes" : ES_HOST['host'],
    #     "es.port" : str(ES_HOST['port'])
    # } 

    es_conf = {
        "es.net.proxy.http.host" : ES_HOST['host'],
        "es.net.proxy.http.port": str(ES_HOST['port']),
        "es.nodes.discovery": "false",
    } 
    

    es_conf["es.resource"] = "matrix-a/elem"
    mat_A_rdd = sc.newAPIHadoopRDD(
        inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
        keyClass="org.apache.hadoop.io.NullWritable", 
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
        conf=es_conf)
    mat_A_rdd.cache()

    es_conf["es.resource"] = "matrix-b/elem"
    mat_B_rdd = sc.newAPIHadoopRDD(
        inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
        keyClass="org.apache.hadoop.io.NullWritable", 
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
        conf=es_conf)
    mat_B_rdd.cache()

    matA_count = mat_A_rdd.count()
    matB_count = mat_B_rdd.count()

    D = (matA_count + matB_count) / float(2 * N**2)

    G = int(round(sqrt(sqrt(D * N**2 / 2))))

    GRP_SIZE = int(ceil(N / float(G)))


    def group_mapper_A(item):
        row = item[1]['row']
        col = item[1]['col']
        val = item[1]['val']

        i_grp = int(ceil(row / float(GRP_SIZE)))

        j_grp = int(ceil(2 * col / float(GRP_SIZE)))
        # j_grp = int(ceil(col / float(GRP_SIZE) / 2))

        return [( (i_grp, j_grp, k + 1), ('A', row, col, val) ) for k in xrange(G + 1)]

    def group_mapper_B(item):
        row = item[1]['row']
        col = item[1]['col']
        val = item[1]['val']

        j_grp = int(ceil(2 * row / float(GRP_SIZE)))

        k_grp = int(ceil(col / float(GRP_SIZE)))

        return [( (i + 1, j_grp, k_grp), ('B', row, col, val) ) for i in xrange(G + 1)]


    A_groups = mat_A_rdd.flatMap(group_mapper_A)

    B_groups = mat_B_rdd.flatMap(group_mapper_B)

    mapped_union = A_groups.union(B_groups)


    def partialSums(item):
        partials = {}

        for elem in item[1]:
            if elem[0] == 'B': 
                continue

            A_row = elem[1]
            A_col = elem[2]
            A_val = elem[3]

            for elem in item[1]:
                if elem[0] == 'A':
                    continue

                B_row = elem[1]
                B_col = elem[2]
                B_val = elem[3]

                if A_col == B_row:
                    group = partials.setdefault((A_row, B_col), [])
                    group.append(A_val * B_val)

        partial_sums = [(key, sum(partials[key])) for key in partials.keys()]
        
        return [item for item in partial_sums if item[1] != 0]


    partial_results = mapped_union.groupByKey(NUM_TASKS).flatMap(partialSums)

    matrix_C = partial_results.reduceByKey(lambda a,b: a+b, NUM_TASKS).filter(lambda item: item[1] != 0)

    result_docs = matrix_C.map(lambda item: ('%s-%s' % (item[0][0],item[0][1]), {
        'row': item[0][0],
        'col': item[0][1],
        'val': item[1]
    }))

    es_conf["es.resource"] = "matrix-c/elem"
    result_docs.saveAsNewAPIHadoopFile(
        path='-', 
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable", 
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
        conf=es_conf)



    matC_count = result_docs.count()
    matC_zeros = N**2 - matC_count
    matC_density = matC_count / float(N**2)
    matC_norm = sqrt(result_docs.map(lambda item: item[1]['val']**2).reduce(lambda a,b: a+b))

    matB_zeros = N**2 - matB_count
    matB_density = matB_count / float(N**2)
    matB_norm = sqrt(mat_B_rdd.map(lambda item: item[1]['val']**2).reduce(lambda a,b: a+b))
    
    matA_zeros = N**2 - matA_count
    matA_density = matA_count / float(N**2)
    matA_norm = sqrt(mat_A_rdd.map(lambda item: item[1]['val']**2).reduce(lambda a,b: a+b))

    mapped_grouped = mapped_union.groupByKey(NUM_TASKS)
    mapped_group_count_average = mapped_grouped.map(lambda i: len(i[1])).reduce(lambda a,b: a+b) / mapped_grouped.count()


    # matA = mat_A_rdd.map(lambda i: ((i[1]['row'],i[1]['col']), i[1]['val'])).sortByKey(True).collect()
    # matB = mat_B_rdd.map(lambda i: ((i[1]['row'],i[1]['col']), i[1]['val'])).sortByKey(True).collect()
    # matC = matrix_C.sortByKey(True).collect()
  
    # def print_matrix(A):
    #     matrix = [[0 for i in range(N)] for j in range(N)]
    #     for result in A:
    #         row = result[0][0]
    #         col = result[0][1]
    #         matrix[row-1][col-1] = result[1]
    #     for i in range(N):
    #         print(','.join([str(matrix[i][j]) for j in range(N)]) + ',')

    # print('A:')
    # print_matrix(matA)
    # print('B:')
    # print_matrix(matB)
    # print('C:')
    # print_matrix(matC)


    print('-' * 20)
    print('A: count: %s  zero_count: %s, density: %s, norm: %s' % (matA_count, matA_zeros, matA_density, matA_norm))
    print('B: count: %s  zero_count: %s, density: %s, norm: %s' % (matB_count, matB_zeros, matB_density, matB_norm))
    print('C: count: %s  zero_count: %s, density: %s, norm: %s' % (matC_count, matC_zeros, matC_density, matC_norm))

    NN = N**2
    GG = G**2
    DNN = D * NN
    DNN_GG = DNN / GG

    print('N: %s' % (N if N < 1e4 else '%.2e' % N))
    print('D: %s' % (D if D > 1e-4 else '%.2e' % D))
    print('G: %s' % G)
    print('N^2 = %s' % (NN if NN < 1e6 else '%.0e' % NN))
    print('D*N^2: %s' % int(round(DNN)))
    print('G^2: %s' % (GG))
    print('D*N^2/G^2: %s' % int(round(DNN_GG)))

    print('mapped_group_count_average: %s' % mapped_group_count_average)

    elapsed = round(time() - start_time, 2)

    if elapsed > 120:
        if elapsed > 3600:
            print("--- %s hours ---" % round(elapsed / 3600, 2))
        else:
            print("--- %s minutes ---" % round(elapsed / 60, 2))
    else:
        print("--- %s seconds ---" % elapsed)

    
    es_client.index(index='matrix-mult-stats', doc_type='result',  
        # id = 'G%s-N%s-D%.2e' % (G, N if N < 1e4 else '%.2e' % N, D),
        body={
            'elap_sec': elapsed,
            'time': int(1000*time()),
            'nodes': 1,
            'g': G,
            'n': N,
            'd': D,
            'nn': NN,
            'gg': GG,
            'dnn': int(round(DNN)),
            'dnn_gg': int(round(DNN_GG)),
            'a_den': matA_density,
            'b_den': matB_density,
            'c_den': matC_density,
            'rel_den': matC_density / D,
            'ab_norm': sqrt(matA_norm * matB_norm),
            'a_norm': matA_norm,
            'b_norm': matB_norm,
            'c_norm': matC_norm,
            'a_ct': matA_count,
            'b_ct': matB_count,
            'c_ct': matC_count,
            'grp_cnt_avg': mapped_group_count_average

            # 'grp_ct_n': int(n),
            # 'grp_ct_mean': round(ct_mean, 5),
            # 'grp_ct_var': round(ct_var, 5),
            # 'ct_std': round(math.sqrt(ct_var), 5),
            # 'grp_ct_min': min(group_counts.keys()),
            # 'grp_ct_max': max(group_counts.keys()),
            # 'tot_grps': total_groups
        }
    )




    # # mapped_elems = mapped_union.sortByKey(True).collect()
    # # partial_sums = partial_results.collect()
    # # grouped_elems = mapped_union.groupByKey().sortByKey(True).collect()
    # # elem_groups = mapped_union.groupByKey().map(partialSums).first()
    # grouped_elems = mapped_union.groupByKey().collect()

    # summed = 0
    # sumSq = 0

    # group_counts = {}

    # for item in grouped_elems:
    #     count = len(item[1])
    #     if count not in group_counts:
    #         group_counts[count] = 0
    #     group_counts[count] += 1
    #     summed += count
    #     sumSq += count**2

    # n = float(len(grouped_elems))

    # ct_mean = summed / n
    # ct_var = (sumSq / n) - ct_mean**2

    # total_groups = sum([group_counts[k] for k in group_counts])
    # print('total_groups: %s' % total_groups)
    # print('group_counts (%s): \n(len of bin, # times it occurred) ' % len(group_counts))
    # for key in sorted(group_counts.keys()):
    #     print(key, group_counts[key])
    # print('ct_mean: %s' % round(ct_mean, 2))
    # print('ct_std: %s' % round(math.sqrt(ct_var), 2))
    # print('ct_max: %s' % max(group_counts.keys()))
    # print('ct_min: %s' % min(group_counts.keys()))
    # # print(elem_groups)
    # # print('mapped_elems: %s' % len(mapped_elems))
    # # for result in mapped_elems:
    # #     print(result)
    # # print('grouped_elems: %s' % len(grouped_elems))
    # # for result in grouped_elems:
    # #     print(result[0], len(result[1]))
    # # print('partial_sums: %s' % len(partial_sums))
    # # for result in partial_sums:
    # #     print(result)
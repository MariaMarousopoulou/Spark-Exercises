from pyspark import SparkContext


def compute_contribs(pair):
    [url, [pageRank, ranks]] = pair  # split key-value pair
    return [(dest, ranks / len(pageRank)) for dest in pageRank]



sc = SparkContext("local[1]", "Exercise2")
IterN = 10
rdd = sc.textFile("url_pages.txt")

list = rdd.collect()

pageRank = []
ranks = []
for e in list:
    ee = [int(a) for a in e.split()]
    site = ee[0]
    matches = ee[1::]
    pageRank.append((site, matches))
    ranks.append((ee[0], 1))


pageRank = sc.parallelize(pageRank)
ranks = sc.parallelize(ranks)


measurement = []
for i in range(IterN):
    contribs = pageRank.join(ranks).flatMap(compute_contribs)
    ranks = contribs.reduceByKey(lambda x, y: x + y).mapValues(lambda x: 0.15 + 0.85 * x)
    measurement.append(ranks.collect())


measurement_total = measurement
file = open("url_ranks.txt", 'w')

sites = [k for k,v in measurement_total[0]]
res = {}
for site in sites:
    res[site] = []


for measure in measurement_total:
    for pair in measure:
        k = pair[0]
        v = pair[1]
        list_c = res[k]
        list_c.append(v)
        res[k] = list_c


tofile = []
for item in res.items():
    l = []
    l.append(item[0])
    for i in range(IterN):
        l.append(item[1][i])
    tofile.append(l)

for line in tofile:
    file.write(str(line) + "\n")
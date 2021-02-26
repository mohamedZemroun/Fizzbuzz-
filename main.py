import pyspark
import sys

lastRowIndex = 1
flag = 0
# Split data to have date and uri
def split(row):
    if len(row.split("\t")) < 2:
        return "rowToDelete"
    else:
        return (row.split("\t")[0], row.split("\t")[1])
# change date uri to (date,viewmodes),zoom
def mapToCreateKey(row):
    return (row[0], row[1].split("/")[4]), row[1].split("/")[6]
# map viewmodes  and zoom with the index  of the first apperance
def mapTheZoom(row):
    global flag
    if row[1] != 0:
        lastValue = listOfRow[row[1] - 1][0][0][1]
        if lastValue != row[0][0][1]:
            flag = row[1]
    return (row[0][0][1] + "," + str(flag), row[0][1])
# compute how many apperance of each tool then delete duplicate
def toList(x):
    return set(list(x)), len(list(x))

# formate data to order it
def orderTheData(x):
    return int(x[0].split(",")[1]), (x[0].split(",")[0], x[1][1], x[1][0])

# formate data to display it
def formatData(x):
    result = x[1][0] + "\t" + str(x[1][1])
    for i in x[1][2]:
        result = result + "\t" + i

    return result



if __name__ == "__main__":
    args = sys.argv[1:]
    path="./tornik-map-20171006.10000.tsv"
    if len(args)!=0:
        path=args[0]
    sc = pyspark.SparkContext('local[*]')
    rdd = sc.textFile(path)
    cleanedRdd = rdd.map(lambda row: split(row)).filter(lambda row: row != "rowToDelete").filter(
        lambda row: "/map/1.0/slab" in row[1])

    zoomRdd = cleanedRdd.map(mapToCreateKey)
    listOfRow = zoomRdd.zipWithIndex().take(zoomRdd.count())
    zoomRDD = zoomRdd.zipWithIndex().map(lambda row: mapTheZoom(row)).groupByKey().mapValues(toList)
    zoomRDD = zoomRDD.map(orderTheData).sortByKey()
    zoomRDD=zoomRDD.map(formatData)
    for item in zoomRDD.take(zoomRDD.count()):
        print(item)

    zoomRDD.coalesce(1).saveAsTextFile("output")
    sc.stop()




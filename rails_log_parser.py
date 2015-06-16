from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def formStaticsRow(p):
    url = p[2].split("/")[1].split(".")[0]
    if(url == ''):
        url = "/"
    return Row(method=p[1], url=url, date=p[6], time=p[7])

def convertDataHash(dataHash):
    result = {}
    for key, values in dataHash.items():
        result[key] = {}
        for value in values:
            count = value.cnt
            date = value.date
            result[key][date] = count
    dataFrameHash = {}
    for key in result.keys():
        dataFrameHash[key] = pd.Series(result[key], index=result[key].keys())
    return pd.DataFrame(dataFrameHash)

def convertDataFrame(dataHash):
    resultHash = {}
    for key, value in dataHash.items():
        for date, count in value.items():
            date = str(date)
            try:
                resultHash[date] = resultHash[date]
            except KeyError:
                resultHash[date] = {}
            finally:
                resultHash[date][key] = count
    dataFrameHash = {}
    for date in resultHash.keys():
        dataFrameHash[date] = pd.Series(resultHash[date], index=resultHash[date].keys())
    return pd.DataFrame(dataFrameHash)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: parser_analysis <file>"
        exit(-1)
    sc = SparkContext("local[2]", "rails log analysis dashboard")
    sqlContext = SQLContext(sc)
    parts = sc.textFile(sys.argv[1])\
            .filter(lambda line: "Start" in line)\
            .filter(lambda line: not "MOPED" in line)\
            .map(lambda x: x.replace("/zh/", "/"))\
            .map(lambda x : x.replace("/en/", "/"))\
            .map(lambda x: x.replace('"', ''))\
            .map(lambda l : l.split(" "))
    #statics = parts.map(lambda p: Row(method=p[1], url=p[2].split('/')[1], date=p[6], time=p[7]))
    statics = parts.map(formStaticsRow)
    schemaStatics = sqlContext.createDataFrame(statics)
    schemaStatics.registerTempTable("statics")
    methodCntEachDay = sqlContext.sql("SELECT count(method) as cnt, date  FROM statics group by date order by date desc limit 30").collect()
    methodsSoFar = sqlContext.sql("select distinct method from statics").collect()
    urlsSoFar = sqlContext.sql("select distinct url from statics").collect()
    recentDays = sqlContext.sql("select distinct date from statics order by date desc limit 30").collect()
    eachMethodCntEachDay = {}
    eachUrlCntEachDay = {}

    # get how many post/get/put ... for last 30 days
    for e in methodsSoFar:
        res = sqlContext.sql("SELECT count(method) as cnt, date  FROM statics where method='{0}' group by date order by date desc limit 30".format(e.method)).collect()
        eachMethodCntEachDay[e.method]= res

    # get how many access for every url for last 30 days
    for e in urlsSoFar:
        res = sqlContext.sql("SELECT count(url) as cnt, date  FROM statics where url='{0}' group by date order by date desc limit 30".format(e.url)).collect()
        eachUrlCntEachDay[e.url]= res


    #print urlsSoFar
    dfMethod = convertDataHash(eachMethodCntEachDay)
    dfUrl = convertDataHash(eachUrlCntEachDay)

    dfMethod.plot()
    dfUrl.plot()
    plt.show()
    sc.stop()


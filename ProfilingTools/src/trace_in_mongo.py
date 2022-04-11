import pymongo
import numpy as np

client = pymongo.MongoClient("localhost", 27017)

db = client.database
collist = db.list_collection_names()
for c in collist:
    print(c)
    indexuse = db[c].aggregate( [ { "$indexStats": { } } ] )
    for i in indexuse:
        # print(i)
        print(i['name'], i['key'], i['accesses']['ops'])
    print()
    print()

sourceName = 'MongoDB.Driver.Core.Extensions.DiagnosticSources'
sourceName = 'ArmoniK.Core.Compute.PollingAgent'

db = client.traces
collection = db.traces
names = collection.distinct("DisplayName", {'SourceName': sourceName})
print(names)
print()

print()
print()

def timeSpanToFloat(timespan):
    tags = timespan.split(':')
    tags2 = tags[2].split('.')
    return (int(tags[0]) * 3600e7 + int(tags[1]) * 60e7 + int(tags2[0]) * 1e7 + int(tags2[1])) / 1e7

def printBySplit(arr, func, splits):
    arrs = np.array_split(arr, splits)
    return ", ".join([f"{func(x):2.10f}" for x in arrs])

def processByFunction(collection, sourceName, displayName, splits):
    results = collection.find({'SourceName': sourceName, 'DisplayName' : displayName}, {'Duration', 'StartTime'})
    results = sorted(results, key=lambda d: d['StartTime'])

    if len(list(results)) < splits: return

    values = [timeSpanToFloat(x['Duration']) for x in results]

    print(displayName, " - ", len(values))
    print('min    :', printBySplit(values, np.min, splits))
    print('mean   :', printBySplit(values, np.mean, splits))
    print('median :', printBySplit(values, np.median, splits))
    print('max    :', printBySplit(values, np.max, splits))
    # print('std    :', printBySplit(values, np.std, splits))
    # print('var    :', printBySplit(values, np.var, splits))
    # print('99%    :', np.percentile(values, 99))
    # print('1%     :', np.percentile(values, 1))


for n in names:
    if n.startswith("ProcessInternalsAsync."): continue
    processByFunction(collection, sourceName, n, 12)
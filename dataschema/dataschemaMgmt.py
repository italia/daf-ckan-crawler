import pandas as pd
import json
import numpy as np

#DEFINITIONS
NAMESPACE = "it.gov.daf.dataset.opendata"

def getData(path):
    if (path.lower().endswith((".json", ".geojson"))):
        with open(path) as data_file:
            dataJson = json.load(data_file)
        return pd.io.json.json_normalize(dataJson, sep='.|.')

    elif (path.lower().endswith((".csv", ".txt", ".text"))):
        separator = csvInferSep(path)
        return pd.read_csv(path, sep=separator)
    else:
        return "-1"

def getFieldsSchema(data):
    fields = list()
    for c, t in zip(data.columns, data.dtypes):
        field = {"name": c, "type": formatConv(t)}
        fields.append(field)
    return fields;

def getDataSchema(path, datasetName):
    data = getData(path)
    fields = getFieldsSchema(data)
    avro = {
        "namespace": NAMESPACE,
        "type": "record",
        "name": datasetName,
        "fields": fields
    }
    return avro


def formatConv(typeIn):
    dic = {
        np.dtype('O'): "String",
        np.dtype('float64'): 'double',
        np.dtype('float32'): 'double',
        np.dtype('int64'): 'int',
        np.dtype('int32'): 'int',
    }

    return dic.get(typeIn, "String");

def csvInferSep(path):
    f = open(path)
    sepList = [",", ';', ':', '|']
    first = f.readline()
    ordTupleSep = sorted([(x, first.count(x)) for x in sepList], key=lambda x: -x[1])
    return ordTupleSep[0][0]


#print(getDataSchema("data.json", "testData"))

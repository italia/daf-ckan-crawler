import dataschema.dataschemaMgmt as ds
import os

print("*******************************")
print("Test for Data Schema generator")
print("*******************************")

path = input("Insert path where data files are: ")


listFiles = os.listdir(path)
print(listFiles)
dsList = list()
errList = list()
okList = list()
for root, dirs, files in os.walk(path, topdown=False):
    for name in files:
        filePath = os.path.join(root, name)
        pathSplit = filePath.split("/")
        dataName = (pathSplit[len(pathSplit)-2])
        try:
            dataschema = ds.getDataSchema(filePath, dataName)
            dsList.append(dataschema)
            okList.append(filePath)
            #print("ok at: ", filePath)
        except:
            #print("error at: ", filePath)
            errList.append(filePath)

print("***********")
print("Correctly processed")
for file, schema in zip(okList, dsList):
    print(file)
    print("\t -->", schema)

print("***********")
print("Errors: ")
for file in errList:
    print(file)



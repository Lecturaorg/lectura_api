import json

##Get keys of each type -> use keys to find duplicate
def extract_keys():
    file = open("database.json")
    data = json.load(file)
    cols = {
        'texts':list(data["texts"][0].keys()),
        'authors': list(data["authors"][0].keys()),
        'editions': list(data["editions"][0].keys()),
    }
    file.close()
    return cols

def checkDuplicates(newData, data):
    non_duplicates = []#data
    index = len(data)
    for i in newData:
        keysToCheck = i.keys()
        duplicate = False
        for j in data:
            stringCheckNew = ""
            stringCheckOld = ""
            for key in keysToCheck: ##Check every key & value
                if key not in j.keys() or key == "date_uploaded":
                    continue
                else:
                    oldValue = j[key]
                    newValue = i[key]
                    if isinstance(oldValue,float): oldValue = int(oldValue)
                    if isinstance(newValue,float): newValue = int(newValue)
                    oldValue = str(oldValue).split(", ")
                    newValue = str(newValue).split(", ")
                    if len(oldValue) == len(newValue):
                        oldValue = ", ".join(oldValue)
                        newValue = ", ".join(newValue)
                        stringCheckOld = stringCheckOld + oldValue
                        stringCheckNew = stringCheckNew + newValue
                    else:
                        stringCheckNew = stringCheckNew + newValue[0]
                        stringCheckOld = stringCheckOld + oldValue[0]
            if stringCheckNew == stringCheckOld and stringCheckNew != "": 
                duplicate = True
        if duplicate == False: 
            i["id"] = index
            index+= 1
            non_duplicates.append(i)
    return non_duplicates

def searchDict(data, query):
    querySplit = query.split(" ")
    for j in range(len(querySplit)):
        element = querySplit[j]
        results = []
        for i in data:
            found = False
            for key in i.keys():
                if str(element).lower() in str(i[key]).lower():
                    if found == False: results.append(i)
                    found = True
        data = results
    return results
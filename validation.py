import json

def checkDuplicates(newData, data, keysToCheck = None, index = None, importType = None):
    '''Checks "newData" for similar data in "data". If keys are not specified, checks all keys'''
    non_duplicates = []#data
    if index is None: index = len(data)
    else: index = index
    for i in newData:
        if keysToCheck is None: keysToCheck = i.keys()
        else: keysToCheck = keysToCheck
        duplicate = False
        for j in data:
            stringCheckNew = ""
            stringCheckOld = ""
            for key in keysToCheck: ##Check every key & value
                if key not in j.keys() or key == "date_uploaded" or key not in i.keys():
                    continue
                else:
                    oldValue = j[key]
                    newValue = i[key]
                    if isinstance(oldValue,float): oldValue = int(oldValue)
                    if isinstance(newValue,float): newValue = int(newValue)
                    if "," in str(oldValue):
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
                    else:
                            stringCheckOld = stringCheckOld + str(oldValue)
                            stringCheckNew = stringCheckNew + str(newValue)
            if stringCheckNew == stringCheckOld and stringCheckNew != "": 
                duplicate = True
                break
        if duplicate == False:
            non_duplicates.append(i)
    return non_duplicates

def searchDict(data, query, delimiter = " "):
    querySplit = query.split(delimiter)
    for j in range(len(querySplit)):
        element = querySplit[j]
        results = []
        for i in data:
            found = False
            for key in i.keys():
                if delimiter != ", ":
                    if str(element).lower() in str(i[key]).lower():
                        if found == False: results.append(i)
                        found = True
                else:
                    if str(element).lower() == str(i[key]).lower():
                        if found == False: results.append(i)
                        found = True
        data = results
    return results

def mainKeys():
    return {'texts': ["text_title", "text_author"]
            , "authors": ["author_name", "author_birth_year", "author_death_year"]
            , "editions":["edition_title", "edition_author"]} ##Keys to check

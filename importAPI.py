from validation import mainKeys, checkDuplicates, searchDict
from table_models import engine
from main_data import mainData
from datetime import datetime
import json

def executeQuery(data,type):
    conn = engine().connect() 
    for i in data:
        cols = i.keys()
        query = "INSERT INTO " + type + ' (' + ', '.join(cols) + ') VALUES ('
        values = []
        for col in cols:
            if isinstance(i[col],str): 
                formString = i[col]
                if "'" in i[col]:
                    formString = formString.replace("'", "'''")
                values.append("'" + formString + "'")
            else: values.append(str(i[col]))
        values = ', '.join(values)
        query = query + values + ")"
        conn.execute(query)
    conn.close()    

def approveImport(data, type):
    dataToChange = mainData(type="all")[type]
    keysToCheck = mainKeys()[type]
    nonDuplicates = checkDuplicates(data, dataToChange, keysToCheck = keysToCheck)
    nonSimilar = []
    for i in nonDuplicates:
        if type == "authors": duplicateCheck = searchDict(dataToChange,i["author_name"], delimiter = ", ")
        elif type == "texts": duplicateCheck = searchDict(dataToChange,i["text_title"], delimiter = ", ")
        if len(duplicateCheck) == 1: continue
        elif len(duplicateCheck) == 0: nonSimilar.append(i)
    if len(nonSimilar)!=0: executeQuery(nonSimilar, type)
    with open("approved_imports.json") as json_file: approved_imports = json.load(json_file)
    newApproved = {
        'data': nonSimilar,
        'date_approved': datetime.today().strftime('%Y-%m-%d')
    }
    approved_imports[type].append(newApproved)
    with open("approved_imports.json","w") as outfile: json.dump(approved_imports,outfile)
    fileName = type+'_import.json'
    importData = []
    with open(fileName,"w") as outfile: json.dump(importData,outfile)
    return nonSimilar

def importData(data):
    importType = data["type"]
    fileName = importType+'_import.json'
    with open (fileName) as jsonFile: importData = json.load(jsonFile)
    date_uploaded = data["date_uploaded"]
    data = data["data"]
    databaseData = mainData(type="all")[importType]
    keysToAddIfNotExists = databaseData[0].keys()
    keysToCheck = mainKeys()[importType]
    data = checkDuplicates(data,databaseData, keysToCheck = keysToCheck, importType = importType) ##Checks database if it exists
    if len(data)!=0 and len(importData)!=0: 
        data = checkDuplicates(data,importData, keysToCheck = keysToCheck)
        combined = importData + data
    elif len(importData) == 0:
        combined = data
    else: combined = importData
    with open(fileName, "w") as outfile: json.dump(combined, outfile)
    return data

def read_sql(script):
    with open(script,"r") as file_content: return file_content.read()

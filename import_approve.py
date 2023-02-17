from validation import mainKeys
from table_models import engine

def executeQuery(data,type):
    conn = engine().connect() 
    for i in nonSimilar:
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
    nonDuplicates = checkDuplicates(approvedData, dataToChange, keysToCheck = keysToCheck)
    nonSimilar = []
    for i in nonDuplicates:
        if type == "authors": duplicateCheck = searchDict(dataToChange,i["author_name"])
        elif type == "texts": duplicateCheck = searchDict(dataToChange,i["text_title"])
        if len(duplicateCheck) == 1: continue
        elif len(duplicateCheck) == 0: nonSimilar.append(i)
    executteQuery(nonSimilar, type)
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

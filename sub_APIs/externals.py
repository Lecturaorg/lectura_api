import requests
from main_data import parseXML
def source_data_func(response, author, title, label, type):
    response.headers['Access-Control-Allow-Origin'] = "*"
    if type == "bnf":
        url = "http://gallica.bnf.fr/SRU"
        params = {
            "version": "1.2", "operation": "searchRetrieve",
            "query": f'''(dc.creator all "{author}") and (dc.title all "{title}")''',
            "startRecord": "1","maximumRecords": "20"
        }
        columns = ["creator", "date","description","language","publisher","source","title","type","subject","identifier"]
        response = requests.get(url, params=params)
        if response.status_code == 200:
            xml_data = response.content
            return parseXML(xml_data, columns)
        else: return response.status_code

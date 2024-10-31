import requests
import xml.etree.ElementTree as ET

def parseXML(xml, cols):
    root = ET.fromstring(xml)
    records = []
    record_elements = root.findall('.//{http://www.loc.gov/zing/srw/}record')
    for record_element in record_elements:
        record_dict = {}
        for col in cols:
            elements = record_element.findall('.//{http://purl.org/dc/elements/1.1/}'+col)
            if elements:
                creator_list = ' | '.join([element.text for element in elements])
                record_dict[col] = creator_list
        records.append(record_dict)
    return records

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

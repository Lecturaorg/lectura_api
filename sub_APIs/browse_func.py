import pandas as pd
import json
from main_data import returnLabel
from sql_funcs import engine
from sqlalchemy import text

def create_range_where_clause(column, range_dict):
    min_value = range_dict.get('min')
    max_value = range_dict.get('max')
    if min_value != '' and max_value != '' and min_value is not None and max_value is not None:
        where_clause = f"{column} >= {min_value} AND {column} <= {max_value}"
    elif min_value is not None and min_value != '': where_clause = f"{column} >= {min_value}"
    elif max_value is not None and max_value != '': where_clause = f"{column} <= {max_value}"
    else: where_clause = ""
    return where_clause

def build_where_clauses(filters):
    print(filters)
    where_clauses = []
    for column, selections in filters.items():
        if selections and isinstance(selections, list):
            column_clauses = []
            for selection in selections:
                column_clause = f"{column} ILIKE '%{selection}%'"
                column_clauses.append(column_clause)
            column_where = " OR ".join(column_clauses)
            where_clauses.append(f"({column_where})")
        elif selections and isinstance(selections, dict): 
            range_clause = create_range_where_clause(column,selections)
            if range_clause != "": where_clauses.append(range_clause)
    # Join the WHERE clauses for each column with AND
    where_query = " AND ".join(where_clauses)
    if not where_query: return ""
    else: where_query = 'WHERE ' + where_query
    print(where_query)
    return where_query

async def browse_func(response, info):
    response.headers['Access-Control-Allow-Origin'] = "*"
    response.headers['Content-Type'] = 'application/json'
    reqInfo = await info.json()
    dataType = reqInfo["type"]
    sort = reqInfo["sort"]["value"]
    sortOrder = reqInfo["sort"]["order"]
    page = int(reqInfo["page"])
    pageLength = int(reqInfo["pageLength"])
    filters = reqInfo["selectedFilters"]
    offset = (page-1)*pageLength
    if len(filters.keys()) == 0: where = ''
    else: where = build_where_clauses(filters)
    if dataType=="authors":
        count = ", coalesce(c.book_cnt,0) book_cnt"
        countJoin = "left join author_bookcount c on d.author_id = c.author_id"
    else: 
        count = ""
        countJoin = ""
    query = f'''SET statement_timeout = 60000;select * from (SELECT {returnLabel(dataType.replace("s",""))}, d.* {count}
                from {dataType} d {countJoin} {where}) aag ORDER BY {sort} {sortOrder} LIMIT {pageLength} OFFSET {offset} '''
    length_query = f'''SET statement_timeout = 60000; SELECT COUNT(*) result_length from {dataType} {where}'''
    result = pd.read_sql(text(query), con=engine()).to_json(orient='records')
    result_length = pd.read_sql(text(length_query), con=engine()).iloc[0, 0]
    body = {"result": json.loads(result), "result_length": int(result_length)}
    response.body = json.dumps(body).encode("utf-8")
    response.status_code = 200
    return response

def filters_func(response, filter_type):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    def filter_options(filter_type):
        author = ["author_positions", "author_name_language", 'author_birth_country', 'author_death_country', 'author_nationality', 'author_birth_city', 'author_death_city']
        text = ["text_type", "text_language"]
        if filter_type == "authors": options = author
        else: options = text
        query_base = f"SET statement_timeout = 60000;SELECT DISTINCT unnest(string_to_array([col], ', ')) AS [col] FROM {filter_type} ORDER BY [col];"
        filter_list = {}
        for filt in options: filter_list[filt] = pd.read_sql(query_base.replace("[col]",filt), con=engine())[filt].to_list()
        return filter_list
    return filter_options(filter_type)
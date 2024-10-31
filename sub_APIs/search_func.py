import pandas as pd
from sql_funcs import engine, read_sql
import json
from urllib.parse import parse_qs
from sqlalchemy import text

def search_func(info, response, query, searchtype):
    response.headers['Access-Control-Allow-Origin'] = "*" ##change to specific origin later (own website)
    params = info.query_params
    query = query.replace("'","''").strip()
    search_params = {"query": f"%{query}%"}
    if searchtype == None:
        allQuery = read_sql("/Users/tarjeisandsnes/lectura_api/API_queries/search_all.sql")
        queryList = query.split(' ')
        if len(queryList) == 1:
            results = pd.read_sql(allQuery,con=engine(),params=search_params).drop_duplicates().to_dict("records")
            return results
        else:
            results = None
            for subQuery in queryList:
                search_params["query"]=f"%{subQuery}%"
                sub_results = pd.read_sql(allQuery, con=engine(), params=search_params)
                results = sub_results if results is None else pd.merge(results, sub_results, how="inner")
            return results.to_dict('records'); #.head(5)
    else: ###Detailed search by type
        parsed = parse_qs(str(params))
        filters = json.loads(parsed.get('filters', [''])[0])
        def find_results(query):
            queryBase = f'''SET statement_timeout = 60000;
                select  * from {searchtype} WHERE  '''
            variables = searchtype.replace("s","")+"_id"
            if searchtype == "authors": variables+= ''',author_id as value,CONCAT(
                SPLIT_PART(author_name, ', ', 1),
                COALESCE(
                    CASE
                    WHEN author_birth_year IS NULL AND author_death_year IS NULL AND author_floruit IS NULL THEN ''
                    WHEN author_birth_year IS NULL AND author_death_year IS NULL THEN CONCAT(' (fl.', left(author_floruit,4), ')')
                    WHEN author_birth_year IS NULL THEN CONCAT(' (d.', 
                            CASE 
                                WHEN author_death_year<0 THEN CONCAT(ABS(author_death_year)::VARCHAR, ' BC')
                                ELSE CONCAT(author_death_year::VARCHAR, ' AD')
                            END,
                        ')')
                    WHEN author_death_year IS NULL THEN CONCAT(' (b.', 
                        CASE
                            WHEN author_birth_year<0 THEN CONCAT(ABS(author_birth_year)::VARCHAR, ' BC')
                            ELSE concat(author_birth_year::VARCHAR, ' AD')
                        END,
                        ')')
                    ELSE CONCAT(' (', ABS(author_birth_year), '-',
                        CASE 
                            WHEN author_death_year<0 THEN CONCAT(ABS(author_death_year)::VARCHAR, ' BC')
                            ELSE CONCAT(author_death_year::VARCHAR, ' AD')
                        END,
                        ')')
                    END,'')) AS label'''
            elif searchtype == "texts": variables+=''',split_part(author_id, ',', 1) AS author_id,text_id as value,text_title || 
                case
                    when text_original_publication_year is null then ' - ' 
                    when text_original_publication_year <0 then ' (' || abs(text_original_publication_year) || ' BC' || ') - '
                    else ' (' || text_original_publication_year || ' AD' || ') - '
                end
                || coalesce(text_author,'Unknown')
                as label'''
            filterString = ""            
            if len(filters)>0:
                for n in range(len(filters)): #varlist should be a body in API request and optional
                    var = filters[n]
                    if var["value"]=="label": continue
                    variables += ","+ var["value"] + '''::varchar(255) "''' + var["value"] + '''" \n''' #Add every search variable
                    if n == len(filters)-1: filterString+= var["value"] + "::varchar(255) ILIKE '%" + query + "%'"
                    else: filterString += var["value"] + "::varchar(255) ILIKE '%" + query + "%'" + " OR \n"
            else:
                if searchtype == "authors": filterString += "author_name::varchar(255) ILIKE '%" + query + "%'"
                elif searchtype == "texts": 
                    filterString += "text_name::varchar(255) ILIKE '%" + query + "%'" + " OR \n text_author ILIKE '%" + query + "%'"
            query = queryBase.replace("*", variables).replace("WHERE ","WHERE " + filterString)
            print(query)
            results = pd.read_sql(text(query), con=engine()).drop_duplicates()#.to_dict('records')
            return results
        queryList = query.split(" ")
        if len(queryList) == 1: results = find_results(queryList[0]).to_dict('records')
        else:
            results = False
            for subQuery in queryList:
                if isinstance(results, pd.DataFrame):
                    newResults = find_results(subQuery)
                    results = pd.merge(results, newResults, how="inner").drop_duplicates()
                else: results = find_results(subQuery)
            results = results.to_dict('records')
        return results

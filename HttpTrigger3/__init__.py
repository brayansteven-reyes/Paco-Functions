import logging
import azure.functions as func
import json
from .. import connection


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    department = req.route_params.get('department')
    sort = req.route_params.get('sort')
    year = req.params.get('year')
    sort_values = ['count','total']

    if not year:
        year = '1990'
    if sort: 
        if sort.lower() not in sort_values:
            sort = 'count'
    else:
        sort = 'count'

    logging.info('departments:'+department)
    query_result =  connection.query_db(f"""Select RAZON_SOCIAL_CONTRATISTA as contractor,
                            count(*) as count,
                            CAST(sum(VALOR_TOTAL_CONTRATO) AS UNSIGNED) as total
                            from contratos 
                            where DEPARTAMENTO = '{department.upper()}'
                            and year(FECHA_FIRMA_CONTRATO)>'{year}'
                            group by RAZON_SOCIAL_CONTRATISTA 
                            order by '{sort}' desc;""")
        
    result_str = json.dumps(query_result)
    func.HttpResponse.mimetype = 'application/json'
    func.HttpResponse.charset = 'utf-8'
    return func.HttpResponse(
            result_str,
            status_code=200
    )

import logging
import azure.functions as func
import json
from .. import connection


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    department = req.route_params.get('department')
    year = req.params.get('year')
    order = req.params.get('order')
    sort = req.params.get('sort')
    limit = req.params.get('limit')
    sort_values = ['count','total']
    order_values = ['asc','desc']

    if not year:
        year = '1990'
    if sort: 
        if sort.lower() not in sort_values:
            sort = 'count'
    else:
        sort = 'count'
    if order: 
        if order.lower() not in order_values:
            order = 'desc'
    else:
        order = 'desc'

    if not limit:
        query_result =  connection.query_db(f"""Select RAZON_SOCIAL_CONTRATISTA as contractor,
                                count(*) as count,
                                CAST(sum(VALOR_TOTAL_CONTRATO) AS UNSIGNED) as total
                                from contratos 
                                where DEPARTAMENTO = '{department.upper()}'
                                and year(FECHA_FIRMA_CONTRATO)>'{year}'
                                group by RAZON_SOCIAL_CONTRATISTA 
                                order by {sort.lower()} {order} 
                                ;""")
    else:
                query_result =  connection.query_db(f"""Select RAZON_SOCIAL_CONTRATISTA as contractor,
                                count(*) as count,
                                CAST(sum(VALOR_TOTAL_CONTRATO) AS UNSIGNED) as total
                                from contratos 
                                where DEPARTAMENTO = '{department.upper()}'
                                and year(FECHA_FIRMA_CONTRATO)>'{year}'
                                group by RAZON_SOCIAL_CONTRATISTA 
                                order by {sort.lower()} {order} 
                                limit  {limit};""")
        
    result_str = json.dumps(query_result)
    
    return func.HttpResponse(
            result_str,
            status_code=200
    )

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
        limit=100

    if not department:
        query_result =  connection.query_db(f"""Select REFERENCIA_CONTRATO as contractor_reference,
                        RAZON_SOCIAL_CONTRATISTA as contractor,
                        NOMBRE_ENTIDAD as entity,
                        URL_PROCESO as url,
                        VALOR_TOTAL_CONTRATO as value
                        from contratos 
                        where year(FECHA_FIRMA_CONTRATO)>'{year}'
                        order by VALOR_TOTAL_CONTRATO  {order} 
                        limit  {limit};""")
    else:
        query_result =  connection.query_db(f"""Select REFERENCIA_CONTRATO as contractor_reference,
                        RAZON_SOCIAL_CONTRATISTA as contractor,
                        NOMBRE_ENTIDAD as entity,
                        URL_PROCESO as url,
                        VALOR_TOTAL_CONTRATO as value
                        from contratos 
                        where DEPARTAMENTO = '{department.upper()}'
                        and year(FECHA_FIRMA_CONTRATO)>'{year}'
                        order by VALOR_TOTAL_CONTRATO  {order} 
                        limit  {limit};""")
        
    result_str = json.dumps(query_result)
    
    return func.HttpResponse(
            result_str,
            status_code=200
    )

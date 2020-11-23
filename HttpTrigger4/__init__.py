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
    
    query_result =  connection.query_db(f"""Select NOMBRE_ENTIDAD as contractor,
                            NIT_ENTIDAD as nit,
                            count(*) as count,
                            CAST(sum(VALOR_TOTAL_CONTRATO) AS UNSIGNED) as total
                            from contratos 
                            where DEPARTAMENTO = '{department.upper()}'
                            and year(FECHA_FIRMA_CONTRATO)>'{year}'
                            group by NOMBRE_ENTIDAD 
                            order by {sort.lower()} {order};""")
        
    result_str = json.dumps(query_result)
    
    return func.HttpResponse(
            result_str,
            status_code=200
    )

import logging
import azure.functions as func
import json
from .. import connection


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    database = req.route_params.get('database')
    order = req.params.get('order')
    sort = req.params.get('sort')
    sort_values = ['count','base_de_datos']
    order_values = ['asc','desc']

    if sort: 
        if sort.lower() not in sort_values:
            sort = 'base_de_datos'
    else:
        sort = 'base_de_datos'
    if order: 
        if order.lower() not in order_values:
            order = 'asc'
    else:
        order = 'asc'
    if not database:
        query_result =  connection.query_db(f"""select base_de_datos,ESTADO_DEL_PROCESO,count 
                                from v_estatus_process_db 
                                order by {sort.lower()} {order};""")
    else:
        query_result =  connection.query_db(f"""select base_de_datos,ESTADO_DEL_PROCESO,count 
                                from v_estatus_process_db 
                                where base_de_datos = '{database.upper()}'
                                order by {sort.lower()} {order};""")
    logging.info('hola'+str(type(query_result)))
    logging.info('hola'+json.dumps(' '.join([str(elem) for elem in query_result]) ))
    result_str = query_result
    
    return func.HttpResponse(
            result_str,
            status_code=200
    )

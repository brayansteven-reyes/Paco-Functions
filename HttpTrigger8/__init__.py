import logging
import azure.functions as func
import json
from .. import connection


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    department = req.route_params.get('department')
    year = req.params.get('year')
    order = req.params.get('order')
    order_values = ['asc','desc']

    if not year:
        year = '1990'
    if order: 
        if order.lower() not in order_values:
            order = 'desc'
    else:
        order = 'desc'

    if not department:
        query_result =  connection.query_db(f"""select Base_de_Datos as data_base,
                        count(*) as count 
                        from contratos
                        where yyyy>'{year}'
                        group by Base_de_Datos
                        order by Base_de_Datos {order} ;""")
    else:
        query_result =  connection.query_db(f"""select Base_de_Datos as data_base,
                        count(*) as count 
                        from contratos
                        where DEPARTAMENTO = '{department.upper()}'
                        and yyyy>'{year}'
                        group by Base_de_Datos
                        order by Base_de_Datos {order} ;""")
        
    result_str = json.dumps(query_result)
    
    return func.HttpResponse(
            result_str,
            status_code=200
    )

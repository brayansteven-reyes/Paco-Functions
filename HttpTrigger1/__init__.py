import logging
import azure.functions as func
import json
from .. import connection


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    department = req.route_params.get('department')
    municipality = req.route_params.get('municipality')

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
        sort = 'department,municipality, year'
    if order: 
        if order.lower() not in order_values:
            order = 'asc'
    else:
        order = 'asc'
    
    if department:
        if municipality: 
            logging.info('departments:'+department)
            query_result =  connection.query_db(f"""Select DEPARTAMENTO as department, MUNICIPIO as municipality,
                                    year(FECHA_FIRMA_CONTRATO) as year, 
                                    count(*) as count,
                                    CAST(sum(VALOR_TOTAL_CONTRATO) AS UNSIGNED) as total
                                    from contratos 
                                    where DEPARTAMENTO = '{department.upper()}' 
                                    and MUNICIPIO = '{municipality.upper()}'
                                    and year(FECHA_FIRMA_CONTRATO)>'{year}'
                                    group by department,municipality, year  
                                    order by {sort.lower()} {order};""")
        
        else: 
            logging.info('departments:'+department)
            query_result =  connection.query_db(f"""Select DEPARTAMENTO as department, MUNICIPIO as municipality,
                                    year(FECHA_FIRMA_CONTRATO) as year, 
                                    count(*) as count,
                                    CAST(sum(VALOR_TOTAL_CONTRATO) AS UNSIGNED) as total
                                    from contratos 
                                    where DEPARTAMENTO = '{department.upper()}' 
                                    and year(FECHA_FIRMA_CONTRATO)>'{year}'
                                    group by department,municipality, year  
                                    order by {sort.lower()} {order};""")
        
    else:
        query_result =  connection.query_db(f"""Select DEPARTAMENTO as department, MUNICIPIO as municipality,
                                year(FECHA_FIRMA_CONTRATO) as year, 
                                count(*) as count,
                                CAST(sum(VALOR_TOTAL_CONTRATO) AS UNSIGNED) as total
                                from contratos 
                                where  year(FECHA_FIRMA_CONTRATO)>'{year}'
                                group by department,municipality, year  
                                order by {sort.lower()} {order};""")
    result_str = json.dumps(query_result)
    func.HttpResponse.mimetype = 'application/json'
    func.HttpResponse.charset = 'utf-8'
    return func.HttpResponse(
            result_str,
            status_code=200
    )

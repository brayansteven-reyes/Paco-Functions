import logging
import azure.functions as func
import json
from .. import connection


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    department = req.route_params.get('department')
    municipality = req.route_params.get('municipality')
    year = req.params.get('year')
    if not year:
        year = '1990'
    logging.info('year:'+year)
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
                                    group by DEPARTAMENTO,MUNICIPIO,year(FECHA_FIRMA_CONTRATO) 
                                    order by DEPARTAMENTO,MUNICIPIO, year asc;""")
        
        else: 
            logging.info('departments:'+department)
            query_result =  connection.query_db(f"""Select DEPARTAMENTO as department, MUNICIPIO as municipality,
                                    year(FECHA_FIRMA_CONTRATO) as year, 
                                    count(*) as count,
                                    CAST(sum(VALOR_TOTAL_CONTRATO) AS UNSIGNED) as total
                                    from contratos 
                                    where DEPARTAMENTO = '{department.upper()}' 
                                    and year(FECHA_FIRMA_CONTRATO)>'{year}'
                                    group by DEPARTAMENTO,MUNICIPIO,year(FECHA_FIRMA_CONTRATO) 
                                    order by DEPARTAMENTO,MUNICIPIO, year asc;""")
        
    else:
        query_result =  connection.query_db(f"""Select DEPARTAMENTO as department, MUNICIPIO as municipality,
                                year(FECHA_FIRMA_CONTRATO) as year, 
                                count(*) as count,
                                CAST(sum(VALOR_TOTAL_CONTRATO) AS UNSIGNED) as total
                                from contratos 
                                where  year(FECHA_FIRMA_CONTRATO)>'{year}'
                                group by DEPARTAMENTO,MUNICIPIO,year(FECHA_FIRMA_CONTRATO) 
                                order by DEPARTAMENTO,MUNICIPIO,year asc;""")
    result_str = json.dumps(query_result)
    func.HttpResponse.mimetype = 'application/json'
    func.HttpResponse.charset = 'utf-8'
    return func.HttpResponse(
            result_str,
            status_code=200
    )

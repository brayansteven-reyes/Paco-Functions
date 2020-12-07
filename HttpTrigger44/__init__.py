import logging
import azure.functions as func
import simplejson as json
from .. import connection
from .. import query_utils


def main(req: func.HttpRequest) -> func.HttpResponse:
    query = f"""select 'antecedentes_siri_sanciones' as 'origin',
                count(*)  as count
                from antecedentes_siri_sanciones
                union
                select 'colusiones_contratacion_sic' as 'origin',
                count(*)  as count
                from colusiones_contratacion_sic
                union
                select 'multas_secop' as 'origin',
                count(*)  as count
                from multas_secop
                union
                select 'responsabilidades_fiscales' as 'origin',
                count(*)  as count
                from responsabilidades_fiscales
                union
                select 'sanciones_penales_fgn' as 'origin',
                count(*)  as count
                from sanciones_penales_fgn;"""
    
    query_result = connection.query_db(query)
    result_str = json.dumps(query_result)

    func.HttpResponse.mimetype = 'application/json'
    func.HttpResponse.charset = 'utf-8'
    return func.HttpResponse(
        result_str,
        status_code=200
    )
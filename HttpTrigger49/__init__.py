import logging
import azure.functions as func
import simplejson as json
from .. import connection
from .. import query_utils


def main(req: func.HttpRequest) -> func.HttpResponse:
    sort_values = ['YEAR', 'COUNT', 'TOTAL']

    conditions = query_utils.getCommonCondtions(req)
    order_by = query_utils.getCommontOrderBy(req, sort_values, sort_values[1])
    limits = query_utils.getCommontLimit(req)

    query = f"""select BASE_DE_DATOS,
                year,
                TIPO_CONTRATO,
                sum(count) as count,
                sum(total) as total
                from v_secop_year
                where 1=1
                {conditions}
                group by 1,2,3 
                {order_by} 
                {limits};"""
                
    query_result = connection.query_db(query)
    result_str = json.dumps(query_result)

    func.HttpResponse.mimetype = 'application/json'
    func.HttpResponse.charset = 'utf-8'
    return func.HttpResponse(
        result_str,
        status_code=200
    )

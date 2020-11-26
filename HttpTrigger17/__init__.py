import logging
import azure.functions as func
import json
from .. import connection
from .. import query_utils


def main(req: func.HttpRequest) -> func.HttpResponse:
    department = req.route_params.get('department')
    sort_values = ['CONTRACT_TYPE', 'COUNT', 'TOTAL']

    conditions = query_utils.getCommonCondtions(req)
    order_by = query_utils.getCommontOrderBy(req, sort_values, sort_values[1])
    limits = query_utils.getCommontLimit(req)

    if department:
        conditions += f" AND DEPARTAMENTO = '{department.upper()}'"

    query = f"""Select TIPO_CONTRATO as contract_type,
                count(*) as count,
                CAST(sum(VALOR_TOTAL_CONTRATO) AS UNSIGNED) as total
                from contratos 
                where 1=1
                {conditions}
                group by 1
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

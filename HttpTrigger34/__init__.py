import logging
import azure.functions as func
import simplejson as json
from .. import connection
from .. import query_utils


def main(req: func.HttpRequest) -> func.HttpResponse:
    contractor_id = req.route_params.get('contractor')
    sort_values = ['CONTRACTOR_ID','ENTITY_ID','ENTITY', 'COUNT','TOTAL']

    conditions = query_utils.getCommonCondtions(req)
    order_by = query_utils.getCommontOrderBy(req, sort_values, sort_values[1])
    limits = query_utils.getCommontLimit(req)

    if contractor_id:
        conditions += f" AND ID_CONTRATISTA = '{contractor_id.upper()}' "

    query = f"""Select ID_CONTRATISTA AS contractor_id,
                nit_entidad AS entity_id,
                NOMBRE_ENTIDAD as entity,
                count(*) as count,
                CAST(sum(VALOR_TOTAL_CONTRATO) AS UNSIGNED) as total
                from contratos 
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

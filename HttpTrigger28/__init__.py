import logging
import azure.functions as func
import simplejson as json
from .. import connection
from .. import query_utils


def main(req: func.HttpRequest) -> func.HttpResponse:
    entity = req.route_params.get('entity')
    sort_values = ['CONTRACT_REFERENCE', 'CONTRACTOR_ID',
                   'CONTRACTOR', 'ENTITY_ID', 'ENTITY', 'VALUE']

    conditions = query_utils.getCommonCondtions(req)
    order_by = query_utils.getCommontOrderBy(req, sort_values, sort_values[1])
    limits = query_utils.getCommontLimit(req)

    if entity:
        conditions += f" AND NOMBRE_ENTIDAD = '{entity.upper()}'"

    query = f"""Select REFERENCIA_CONTRATO as contract_reference,
                ID_CONTRATISTA as contractor_id,
                RAZON_SOCIAL_CONTRATISTA as contractor,
                NIT_ENTIDAD AS entity_id,
                NOMBRE_ENTIDAD as entity,
                URL_PROCESO as url,
                VALOR_TOTAL_CONTRATO as value
                from contratos 
                where VALOR_ADICIONES > (VALOR_TOTAL_CONTRATO*0.4)
                {conditions}
                group by 1,2,3,4
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

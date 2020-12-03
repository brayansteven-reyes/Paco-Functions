import logging
import azure.functions as func
import simplejson as json
from .. import connection
from .. import query_utils


def main(req: func.HttpRequest) -> func.HttpResponse:
    # contractor
    contractor_id = req.route_params.get('contractor')
    sort_values = ['CONTRACTOR','CONTRACTOR_NAME']
    conditions = query_utils.getCommonCondtions(req)
    order_by = query_utils.getCommontOrderBy(req, sort_values, sort_values[1])
    limits = query_utils.getCommontLimit(req)

    if contractor_id:
        conditions += f" AND ID_CONTRATISTA = '{contractor_id.upper()}' "

    query = f"""Select ID_CONTRATISTA as contractor,
                RAZON_SOCIAL_CONTRATISTA as contractor_name
                from  v_list_contractors
                where 1=1 
                {conditions}
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

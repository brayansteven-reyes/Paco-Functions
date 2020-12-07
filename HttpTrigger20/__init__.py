import logging
import azure.functions as func
import simplejson as json
from .. import connection
from .. import query_utils


def main(req: func.HttpRequest) -> func.HttpResponse:
    entity = req.route_params.get('entity')
    sort_values = ['ENTITY', 'COUNT', 'TOTAL']

    conditions = query_utils.getCommonCondtions(req)
    order_by = query_utils.getCommontOrderBy(req, sort_values, sort_values[1])
    limits = query_utils.getCommontLimit(req)

    if entity:
        conditions += f" AND NOMBRE_ENTIDAD = '{entity.upper()}' "

    query = f"""select IFNULL(a.entity,'{entity.upper()}') as entity, b.year,  IFNULL(count,0) as count,IFNULL(total,0) as total  
				from (select NOMBRE_ENTIDAD as entity,
                year,
                sum(count)  as count,
                sum(total)  as total
                from v_secop_entities
                where 1=1
                {conditions}
                group by 1,2) a 
                right join
                (select  year from v_secop_entities group by 1) b
                on a.year=b.year
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

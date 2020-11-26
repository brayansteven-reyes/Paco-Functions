import logging
import azure.functions as func
import json
from .. import connection
from .. import query_utils


def main(req: func.HttpRequest) -> func.HttpResponse:
    contractor_id = req.route_params.get('contractor')
    sort_values = ['MONTH']
    months = ''
    query_years = """select year 
                    from v_secop_count_month_contractor
                    group by year;
                    """
    query_years_result = connection.query_db(query_years)
    for year in query_years_result:
        tmp_year = year['year']
        months += f" CAST(sum(case when year = {tmp_year} then count else 0 end) AS UNSIGNED) as 'year_{tmp_year}',"

    months = months[:len(months)-1]

    conditions = query_utils.getCommonCondtions(req)
    order_by = query_utils.getCommontOrderBy(req, sort_values, sort_values[0])
    limits = query_utils.getCommontLimit(req)

    if contractor_id:
        conditions += f" AND ID_CONTRATISTA = '{contractor_id.upper()}' "

    query = f"""select  month,
                {months}
                from v_secop_count_month_contractor 
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

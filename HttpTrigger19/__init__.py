import logging
import azure.functions as func
import simplejson as json
from .. import connection
from .. import query_utils


def main(req: func.HttpRequest) -> func.HttpResponse:
    department = req.route_params.get('department')
    sort_values = ['MONTH']
    months = ''
    select_year = ''
    query_years = """select year 
                    from v_secop_count_month_departments
                    group by year;
                    """
    
    query_years_result = connection.query_db(query_years)
    for year in query_years_result:
        tmp_year = year['year']
        select_year += f"IFNULL(year_{tmp_year},0) as year_{tmp_year}," 
        months += f" sum(case when year = {tmp_year} then count else 0 end)  as 'year_{tmp_year}',"
    
    select_year = select_year[:len(select_year)-1]
    months = months[:len(months)-1]

    conditions = query_utils.getCommonCondtions(req)
    order_by = query_utils.getCommontOrderBy(req, sort_values, sort_values[0])
    limits = query_utils.getCommontLimit(req)

    if department:
        conditions += f" AND DEPARTAMENTO = '{department.upper()}'"

    query = f"""select b.month, {select_year}
                from (select  month,
                {months}
                from v_secop_count_month_departments 
                where 1=1
                {conditions}
                group by 1 )a
                right join 
                (select  month from v_secop_count_month_departments group by 1) b 
                 on b.month=a.month
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

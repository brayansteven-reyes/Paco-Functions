import logging
def getCommonCondtions(req):
    # Params
    start_year = req.params.get('start_year')
    end_year = req.params.get('end_year')
    
    # Conditions 
    conditions = ''
    conditions = includeAnotherCondtion(
        req, conditions, 'BASE_DE_DATOS', 'database', ['SECOP I', 'SECOP II'])
    if start_year:
        conditions += f" AND year >= {start_year}"
    if end_year:
        conditions += f" AND year <= {end_year}"
    
    return conditions


def includeAnotherCondtion(req,conditions,column,new_condition,new_condition_values):
    # Params
    new_condition_value = req.params.get(new_condition)
    if new_condition_value: 
        if new_condition_value.upper() in new_condition_values:
            conditions += f" AND {column} = '{new_condition_value.upper()}' "    
    return conditions

def getCommontOrderBy(req,sort_values,sort_default='1'):
    order = req.params.get('order')
    sort = req.params.get('sort')
    order_values = ['ASC','DESC']
    if sort: 
        if sort.upper() not in sort_values:
            sort = sort_default
    else:
        sort = sort_default
    if order: 
        if order.upper() not in order_values:
            order = 'DESC'
    else:
        order = 'DESC' 
    if sort.upper() == 'DATABASE':
        return f" ORDER BY '{sort.upper()}' {order}"
    else:
        return f" ORDER BY {sort.upper()} {order}"

def getCommontLimit(req):
    limit = req.params.get('limit')
    limits = ''
    if limit:
        limits = f" LIMIT {limit}"
    return limits
    
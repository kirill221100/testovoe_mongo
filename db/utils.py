from .db_setup import sample_collection
from pandas import date_range
import datetime


async def aggregate(dt_from: str, dt_upto: str, group_type: str):
    answer = {"dataset": [], "labels": []}
    dt_from = datetime.datetime.strptime(dt_from, '%Y-%m-%dT%H:%M:%S')
    dt_upto = datetime.datetime.strptime(dt_upto, '%Y-%m-%dT%H:%M:%S')
    date_format = {'month': "%Y-%m", 'day': "%Y-%m-%d", 'hour': "%Y-%m-%dT%H"}
    freq = {'month': "MS", 'day': "D", 'hour': "H"}
    q = sample_collection.aggregate(
        [
            {"$match": {
                "dt": {
                    "$gte": dt_from,
                    "$lte": dt_upto
                }
            }},
            {'$group': {
                '_id': {'$dateToString': {'format': date_format[group_type], 'date': "$dt"}},
                "Amount": {'$sum': '$value'},
                'date': {"$first": "$dt"}
            }},
            {'$sort': {'_id': 1}}
        ])
    if group_type == 'month':
        res = {datetime.datetime(i['date'].year, i['date'].month, 1, 0, 0): i['Amount'] async for i in q}
    elif group_type == 'day':
        res = {datetime.datetime(i['date'].year, i['date'].month, i['date'].day, 0, 0): i['Amount'] async for i in q}
    else:
        res = {datetime.datetime(i['date'].year, i['date'].month, i['date'].day, i['date'].hour, 0): i['Amount'] async for i in q}
    for i in date_range(dt_from, dt_upto, freq=freq[group_type]):
        if i.to_pydatetime() in res.keys():
            answer['dataset'].append(res[i.to_pydatetime()])
        else:
            answer['dataset'].append(0)
        answer['labels'].append(i.to_pydatetime().strftime('%Y-%m-%dT%H:%M:%S'))
    return answer

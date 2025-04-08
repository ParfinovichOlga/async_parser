import datetime as dt
import asyncio
from aiohttp import ClientSession
from gather_links import get_file_date
import pandas as pd
import numpy as np
import os

START_URL = "https://spimex.com/markets/oil_products/trades/results/"
START_FROM = dt.datetime(2023, 1, 1).date()

links = ['https://spimex.com/upload/reports/oil_xls/oil_xls_20250407162000.xls?r=7396',
         'https://spimex.com/upload/reports/oil_xls/oil_xls_20250404162000.xls?r=4622',
         'https://spimex.com/upload/reports/oil_xls/oil_xls_20250403162000.xls?r=7879',
         'https://spimex.com/upload/reports/oil_xls/oil_xls_20250402162000.xls?r=2859',
         'https://spimex.com/upload/reports/oil_xls/oil_xls_20250401162000.xls?r=9028',
         'https://spimex.com/upload/reports/oil_xls/oil_xls_20250331162000.xls?r=4864']


async def download_file(work_queue, session):
    url = await work_queue.get()
    query_params = {'downloadformat': 'xlsx'}
    async with session.get(url, params=query_params) as response:
        print(response.status)
        date = get_file_date(url)
        name = f'{date}.xlsx'
        data = await response.read()
        with open(name, "wb") as f:
            f.write(data)
        return name, date


async def retrieve_requested_data_from_file(work_queue, session):
    """Retrieve requested data from file."""
    file_name, date = await download_file(work_queue, session)
    pre_data = pd.read_excel(file_name, usecols='B', nrows=20)

    row, col = np.where(pre_data == 'Единица измерения: Метрическая тонна')

    data = pd.read_excel(file_name, usecols='B:F,O', skiprows=int(row[0])+2)

    data.rename(columns={'Код\nИнструмента': 'exchange_product_id',
                         'Наименование\nИнструмента': 'exchange_product_name',
                         'Базис\nпоставки': 'delivery_basis_name',
                         'Объем\nДоговоров\nв единицах\nизмерения': 'volume',
                         'Обьем\nДоговоров,\nруб.': 'total',
                         'Количество\nДоговоров,\nшт.': 'count'
                         }, inplace=True)

    data = data.dropna()
    data.insert(2, 'oil_id', data['exchange_product_id'].str[:4])
    data.insert(3, 'delivery_basis_id', data['exchange_product_id'].str[4:7])
    data.insert(5, 'delivery_type_id', data['exchange_product_id'].str[-1:])
    data['date'] = date
    clean_data = data[data['count'] != '-']
    return clean_data


def delete_file(file):
    """Delete file from server."""
    os.remove(file)


async def main():
    work_queue = asyncio.Queue()
    for url in links:
        await work_queue.put(url)
    async with ClientSession() as session:
        await asyncio.gather(*[asyncio.create_task(retrieve_requested_data_from_file(work_queue, session)) for n in range(work_queue.qsize())])

if __name__ == "__main__":
    s = dt.datetime.now()
    asyncio.run(main())
    print(dt.datetime.now() - s)
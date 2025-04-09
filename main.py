from dateutil import parser
from selenium import webdriver
from selenium.webdriver.common.by import By
import datetime as dt
import asyncio
from aiohttp import ClientSession
from dateutil import parser
import pandas as pd
import numpy as np
import os
import orm


START_URL = "https://spimex.com/markets/oil_products/trades/results/"
START_FROM = dt.datetime(2023, 1, 1).date()

NEXT_CSS_SEL = 'li.bx-pag-next a'
LINK_CSS_SEL = '#comp_d609bce6ada86eff0b6f7e49e6bae904 div.accordeon-inner__wrap-item a'


chrome_options = webdriver.ChromeOptions()
chrome_options.add_experimental_option('excludeSwitches', ['disable-popup-blocking'])
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

driver = webdriver.Chrome(options=chrome_options)


def get_file_date(file_url: str):
    """Retrieve date from file name."""
    date = parser.parse(file_url.split('_')[-1][:8]).date()
    return date


async def produce(urls: list, q: asyncio.Queue):
    """Collect links: Add urls to queue for processing"""
    for url in urls:
        await q.put(url)


async def _get_links():
    """Collect links: Get download links from the page."""
    link_obj = driver.find_elements(By.CSS_SELECTOR, LINK_CSS_SEL)
    return [
            t.get_attribute('href') for t in link_obj
            if get_file_date(t.get_attribute('href')) >= START_FROM
            ]


async def grab_links(q: asyncio.Queue, url=START_URL):
    """Collect links from url"""
    driver.get(url)
    str_obj = await _get_links()
    if str_obj:
        await produce(str_obj, q)
        next_page = driver.find_element(By.CSS_SELECTOR, NEXT_CSS_SEL)
        url = next_page.get_attribute('href')
        await grab_links(q, url)
    return q


async def download_file(url: str, session: ClientSession):
    """Download file from url to server."""
    query_params = {'downloadformat': 'xlsx'}
    async with session.get(url, params=query_params) as response:
        date = get_file_date(url)
        name = f'{date}.xlsx'
        data = await response.read()
        with open(name, "wb") as f:
            f.write(data)
        return name, date


async def retrieve_requested_data_from_file(url: str, session: ClientSession):
    """Retrieve requested data from file."""
    file_name, date = await download_file(url, session)
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
    await delete_file(file_name)
    return clean_data


async def delete_file(file):
    """Delete file from server."""
    os.remove(file)


async def add_to_db(work_queue: asyncio.Queue, session: ClientSession):
    """Add data to database"""
    while not work_queue.empty():
        url = await work_queue.get()
        data = await retrieve_requested_data_from_file(url, session)
        await orm.insert_data_pull_to_db(data)


async def main(n=10):
    work_queue = asyncio.Queue()
    await grab_links(work_queue)
    await orm.create_tables()
    async with ClientSession() as session:
        await asyncio.gather(*[asyncio.create_task(add_to_db(work_queue, session)) for i in range(n)])

if __name__ == "__main__":
    s = dt.datetime.now()
    asyncio.run(main())
    print(dt.datetime.now() - s)

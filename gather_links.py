from dateutil import parser
from selenium import webdriver
from selenium.webdriver.common.by import By
import datetime as dt
import asyncio

START_URL = "https://spimex.com/markets/oil_products/trades/results/"
START_FROM = dt.datetime(2023, 1, 1).date()

next_css_sel = 'li.bx-pag-next a'
link_css_sel = '#comp_d609bce6ada86eff0b6f7e49e6bae904 div.accordeon-inner__wrap-item a'

chrome_options = webdriver.ChromeOptions()
chrome_options.add_experimental_option('excludeSwitches', ['disable-popup-blocking'])
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

driver = webdriver.Chrome(options=chrome_options)


async def produce(urls, q: asyncio.Queue):
    for url in urls:
        await q.put(url)


def get_file_date(file_url: str):
    """Retrieve date from file name."""
    date = parser.parse(file_url.split('_')[-1][:8]).date()
    return date


async def _get_links():
    """Get download links from the page."""
    link_obj = driver.find_elements(By.CSS_SELECTOR, link_css_sel)
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
        next_page = driver.find_element(By.CSS_SELECTOR, next_css_sel)
        url = next_page.get_attribute('href')
        await grab_links(q, url)
    return q

async def main():
    q = asyncio.Queue()
    links = await grab_links(q)
    print(links.qsize(), links)

if __name__ == "__main__":
    s = dt.datetime.now()
    asyncio.run(main())
    print(dt.datetime.now() - s)
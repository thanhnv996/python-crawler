import requests
import json
import pandas as pd
import math

from bs4 import BeautifulSoup as bs
from selenium import webdriver
from db import db_engine
from log import get_logger

MAX_RECORD_ON_PAGE = 500  # because maximum each page is 500 record
FIRST_INDEX = ONE_VALUE = 1
logger = get_logger(__name__)


def ctCrawl():
    logger.info('Begin crawl data...')
    try:
        originUrl = "https://nha.chotot.com/toan-quoc/mua-ban-bat-dong-san"
        extractColumn = ['subject', 'region_name', 'area_name', 'ward_name', 'address', 'phone', 'price',
                         'price_string', 'body']
        df = pd.DataFrame(columns=extractColumn)

        driver = webdriver.Chrome(executable_path='chromedriver.exe')
        driver.get(originUrl)
        soup = bs(driver.page_source, 'html.parser')
        driver.quit()

        provinceInfoDiv = soup.find('div', {'class': 'hidden'})
        provinceATag = provinceInfoDiv.find_all('a')

        for link in provinceATag:
            provinceLink = link.get('href')
            provinceName = provinceLink.replace('https://nha.chotot.com/', '');
            provinceName = provinceName.replace('/mua-ban-bat-dong-san', '');

            provinceCode = getProvinceCode(provinceName)

            totalRecordOfProvince = getTotalRecordOfProvince(provinceCode)

            page = FIRST_INDEX
            numberOfPage = math.ceil(totalRecordOfProvince / MAX_RECORD_ON_PAGE)
            while page <= numberOfPage:
                dt = getProvinceOnePage('https://gateway.chotot.com/v1/public/ad-listing?' +
                                        'region_v2=' + str(provinceCode) +
                                        '&cg=1000&st=s,k&o=' + str((page - 1) * MAX_RECORD_ON_PAGE) +
                                        '&page=' + str(page) +
                                        '&limit=' + str(MAX_RECORD_ON_PAGE) + '&w=1&key_param_included=true')
                extractDt = dt[extractColumn]

                df = df.append(extractDt)
                page += ONE_VALUE
                # break
            break

        logger.info('Writing data to database...')
        df.reset_index()
        df.to_sql('areas', db_engine(), if_exists='replace')
        logger.info('Write data to database has done!')

        logger.info('Crawl data success!')

    except Exception as e:
        logger.error('Failed to crawler: ' + str(e))



def getProvinceCode(provinceName):
    """
    getProvinceCode does get province code.

    :param provinceName: Name of province.
    :return: province code.
    """
    infoProvinceApiResponse = getJSONData('https://gateway.chotot.com/v1/public/deeplink-resolver?siteId=1&url=%2F' +
                                          provinceName +
                                          '%2Fmua-ban-bat-dong-san%3Fpage%3D1')

    return infoProvinceApiResponse.get('regionObjV2').get('regionValue')


def getTotalRecordOfProvince(provinceCode):
    """
    getTotalRecordOfProvince does get total record - object of province.

    :param provinceCode: Code of province.
    :return: total record of province.
    """
    pageApiResponse = getJSONData('https://gateway.chotot.com/v1/public/ad-listing?region_v2=' +
                                  str(provinceCode) +
                                  '&cg=1000&st=s,k&o=40&page=1&limit=20&w=1&key_param_included=true')

    return pageApiResponse.get('total')


def getProvinceOnePage(allRecordApiUrl):
    """
    getProvinceOnePage does get 500 record each page of project.

    :param allRecordApiUrl: Api have information of province: total record, 500 items of province.
    :return: 500 items of province in dataframe.
    """
    allRecordApiResponse = getJSONData(allRecordApiUrl)
    allRecordOfProvince = allRecordApiResponse.get('ads')

    return pd.DataFrame(allRecordOfProvince)


def getJSONData(url):
    """
    getData excute call api with http method.

    :param url: CDN or IP address.
    :return: json response.
    """
    req = requests.get(url)
    if not req.ok:
        logger.error('GET url: ' + url)
        return {}

    logger.info('GET url: ' + url)
    return json.loads(req.text)

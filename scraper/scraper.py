from typing import List, Dict
import time
import logging
import io
import base64
from statistics import mean
import zlib
import json
from datetime import date, timedelta, datetime
from langdetect import detect
import requests
from bs4 import BeautifulSoup
import requests_html
from requests_html import HTMLSession
from requests import status_codes
import pdfplumber
import camelot

headers = {
    'user-agent': requests_html.user_agent(),
}

start = time.perf_counter()

class ExtractPdf:
    def __init__(self, pdf_content) -> None:
        self.pdf_content = pdf_content

    def extract_pdf_info(self) -> dict:
        """
        Extracts and Returns text from pdf using pdfplumber library
        """
        text = ''
        title = ''
        author = ''
        tables = []
        with pdfplumber.open(self.pdf_content) as pdf_file:
            for page in pdf_file.pages:
                # print(pdf_file.metadata)
                text += page.extract_text()
                tables.append(page.extract_tables())
            
            bytes = self.pdf_content.read()
            title = pdf_file.metadata['Title']
            author = pdf_file.metadata['Author']
        
        return (text,title,author, [tbl for tbl in tables if len(tbl) > 0], self.encode(bytes))

    def extract_table(self) -> list:
        """
        Extracts table from pdf using pdfplumber library
        """
        tables = []
        with pdfplumber.open(self.pdf_content) as pdf_file:
            for page in pdf_file.pages:
                tables.append(page.extract_tables())

        return [tbl for tbl in tables if len(tbl) > 0]
    
    def extract_table(self):
        """
        Extracts table from pdf using camelot library
        """
        tables = camelot.read_pdf(self.pdf_content)
        return [t.df.to_csv(sep='\t') for t in tables]


    def get_pdf_data_encoded(self):
        """
        Open pdf file in binary mode,
        return a hex of a string encoded in base-64.
        """
        with open(self.pdf_content, 'rb') as file:
            bytes = file.read()
            return self.encode(bytes)
        
    def encode(self, data: bytes) -> str:
        """
        Return compressed base-64 encoded value of binary data.
        """
        return str(base64.b64encode(zlib.compress(data)))[2:-1]
    
    def decode(self, data: str) -> bytes:
        """
        Return decoded value of a compressed base-64 encoded string
        """
        return zlib.decompress(base64.b64decode(data))

class ScrapeWebsite:
    def __init__(self, delay) -> None:
        self.delay = delay
        self.success_delay_history = [1]
        self.successes = []
        # self.avg_delay = mean(self.success_delay_history)

    def get_articles(self, url:str, response, start_date, end_date) -> list:
        """
        Returns the title, text and links to all relevant articles.
        """
        domain = "https://www.eestipank.ee"
        infos = []
        soup = BeautifulSoup(response.text, 'lxml')
        start_date_obj = date(start_date['year'],start_date['month'], start_date['day'])
        end_date_obj = date(end_date['year'],end_date['month'], end_date['day'])
        
        def blogi(url:str, soup):
            max_pages = soup.find_all('a', {'tabindex':'0'})[-2].get_text()
            max_pages = int(max_pages)
            session = HTMLSession()

            num = 1
            while num <= max_pages:
                
                article_divs = soup.find_all('div', class_='MiniPost_post__content__17BRw')
                for div in article_divs:
                    # split the dates (eg 22.10.02 -> [22] [10] [02]) and convert them to type int
                    dt_txt = [int(dt) for dt in div.find('p').get_text().split('.')]

                    # Gets parent element which contains the press link
                    parent_link = div.parent.parent['href']
                    article_title = div.find('h2').get_text()

                    # Continue loop if the specified starting date is greater than press date
                    if start_date_obj > date(dt_txt[-1],dt_txt[1],dt_txt[0]):
                        continue

                    # Continue loop if the press date is greater than the ending date 
                    if end_date_obj < date(dt_txt[-1],dt_txt[1],dt_txt[0]):
                        continue
                    
                    # Get the current article publish date
                    article_date = div.find('p').get_text()

                    # Append link
                    infos.append((parent_link, article_date, article_title))

                num +=1
                new_url = url + f'?page={num}'
                response = self.synchronous_request(new_url, session)
                if response.status_code != 200:
                    continue
                response.html.render(timeout=20000)
                soup = BeautifulSoup(response.text, 'lxml')

            return infos

        def press():
            # Get all elements containing the press dates
            press_dates = soup.find_all('dt')

            for press_dt in press_dates:
                # split the dates (eg 22.10.02 -> [22] [10] [02]) and convert them to type int
                dt_txt = [int(dt) for dt in press_dt.get_text().split('.')]

                # Continue loop if the specified starting date is greater than press date
                if start_date_obj > date(dt_txt[-1],dt_txt[1],dt_txt[0]):
                    continue

                # Continue loop if the press date is greater than the ending date 
                if end_date_obj < date(dt_txt[-1],dt_txt[1],dt_txt[0]):
                    continue

                # Gets parent element which contains the press link
                parent_link = press_dt.parent['href']
                article_title = press_dt.find_next_sibling('dd').get_text()

                # Get the current article publish date
                article_date = press_dt.get_text()

                # Append link
                infos.append((parent_link, article_date, article_title))

            return infos
        
        if f'{domain}/blogi' == url:
            infos = blogi(url, soup)
            return infos
        
        infos = press()
        return infos

    def download_pdf(self, all_links, error_handler) -> list:
        """
        Verifies if a pdf link exists on webpage, then stores the content

        Returns a list of tuples containing the link and content.
        """
        pdf_link_content = []

        pdf_links = [
            link['href'] for link in all_links 
            if '.pdf' in link['href'][-5:] 
            or '.pdf' in link.get_text()[-5:]
        ]
        
        for pdf_link in pdf_links:
            try:
                pdf = requests.get(pdf_link, headers)

                # Logs status of the requests
                error_handler.log_result(pdf.status_code, pdf_link)
                
                # Stores pdf content in memory
                pdf_content = io.BytesIO(pdf.content)

                # Appends link and pdf content
                pdf_link_content.append((pdf_link, pdf_content))

            except Exception as err:
                error_handler.pdf_errors(pdf_link, f"{err}")

        return pdf_link_content

    def synchronous_request(self, url, session):
        time.sleep(self.delay)
        response = session.request(method="GET", url=url, headers=headers)
        return response

    def rate_limit(self, status_code: int) -> int:
        """
        Accepts a HTTP status code.

        Calculates mean of all previous delay rates that resulted in a status code of 200. 
        Appends current delay rate to delay history if current status code is 200.

        Returns new delay rate of website
        """
        delays_mean = mean(self.success_delay_history)

        if status_code == 429:
            # 0.1 is used in order to not skip or jump over the smallest delay as possible
            self.delay = delays_mean + 0.1 / (len(self.success_delay_history) * 0.1)

        elif status_code == 200 and self.delay <= 0.1:
            return
        
        elif status_code == 200:
            # 0.2 is used in order to quickly reach the smallest delay possible
            self.success_delay_history.append(self.delay)
            self.delay = round(delays_mean - 0.2 / (len(self.success_delay_history) * 0.2), 3)

        return self.delay

    def add_successful_scrape(
            self, 
            datetime_accessed,
            _type,
            url,
            pdf_encode='', 
            tables='', 
            text='', 
            _date='',
            language='', 
            title='', 
            html='',
            author=''
        ):
        date_arr = _date.split('.')
        year, day, month = date_arr[-1], date_arr[0], date_arr[1]
        success_obj = {
                "datetime_accessed": f"{datetime_accessed}",
                "language": f"{language}",
                "document_type": f"{_type.lower()}",
                "document_author": f"{author}",
                "document_date": f"{year}-{month}-{day}",
                "document_title": f"{title}",
                "document_text": f"{text}",
                "document_html": f"{html}",
                "document_url": f"{url}",
                "document_pdf_encoded": f"{pdf_encode}",
                "document_tables": f"{tables}",
            }
        self.successes.append(success_obj)

class ErrorHandling:

    def __init__(self) -> None:
        self.date = date.today().strftime("%Y-%m-%d")
        self.errors = []

    def log_result(self, code: int, url: str) -> None:
        """
        Logs failed and successful scrapes
        """
        timestamp = date.today()
        logging.basicConfig(filename=f'{self.date}.log', level=logging.DEBUG)

        # Get the respective status code message
        code_msg = status_codes._codes[code][0]

        if code >= 200 and code <= 299:
            logging.info(f"{timestamp} ==> {code_msg}")
            return

        if code == 429:
            logging.warning(f"{timestamp} ==> {code_msg}")

        else:
            logging.error(f"{timestamp} ==> {code_msg}")

        err_obj = {
            "datetime_accessed":datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "document_url":url,
            "processing_error": f"{code_msg}"
        }
        self.errors.append(err_obj)
        
        raise Exception((code_msg, url))
    
    def pdf_errors(self, pdf_url='', err_msg='pdf error'):
        timestamp = date.today()
        logging.basicConfig(filename=f'{self.date}.log', level=logging.DEBUG)
        logging.error(f"{timestamp} ==> {err_msg}")

        err_obj = {
            "datetime_accessed":datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "document_url":pdf_url,
            "processing_error": f"{err_msg}"
        }
        self.errors.append(err_obj)


def get_countries() -> List[Dict[str, str]]:
    """
    Return a list of countries obtained from a RestAPI via the requests library.

    This is a sample piece of code demonstrating how to return a scrape result.
    You may delete this function in your final code.
    """

    document_url = 'https://restcountries.com/v3.1/all'
    response = requests.get('https://restcountries.com/v3.1/all', verify=False).json()
    
    countries = [{
        'datetime_accessed': datetime.now().isoformat(),
        'language': 'en',
        'document_type': 'speech',
        'document_author': '',
        'document_date': datetime.utcnow().strftime('%Y-%m-%d'),
        'document_title': record.get('cca2'),
        'document_text': record.get('name').get('official') + record.get('region'),
        'document_html': '',
        'document_url': document_url,
        'document_pdf_encoded': '',
        'document_tables': []
    } for record in response]
    return {
        'metadata': {
            "query_start_date": "2022-01-01", 
            "query_end_date": "2022-12-31",
            "run_start_datetime": "2023-11-02T13:33:14Z",
            "schema": "v2"
        },
        'errors': [],
        'successes': countries
    }

def run_scrape(start: dict, end: dict, document_types: List[str]):

    domain = "https://www.eestipank.ee"
    urls = [
        "https://www.eestipank.ee/blogi",
        f"https://www.eestipank.ee/press/majanduskommentaarid/{start['year']}",
        f"https://www.eestipank.ee/en/press/{start['year']}"
    ]
    run_start_datetime = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    error_handler = ErrorHandling()
    extract_website = ScrapeWebsite(delay=1)

    session = HTMLSession()

    for url in urls:
        # Send get request to website
        response = extract_website.synchronous_request(url,session)
        
        # Log the date, status code and status message into log file
        error_handler.log_result(response.status_code, url)

        # Get the websites rate limit
        extract_website.rate_limit(response.status_code)
        
        # Render Javascript Elements
        response.html.render(timeout=20000)
        
        article_info = extract_website.get_articles(url=url, response=response, start_date=start, end_date=end)
        for link, article_date, article_title in article_info:
            try:
                pdfs = extract_website.download_pdf(all_links, error_handler)
                if len(pdfs) > 0:
                    for pdf_link, pdf_content in pdfs:
                        extract_pdf = ExtractPdf(pdf_content)
                        try:
                            # Extracts text & tables from pdfs if one exist
                            (
                                pdf_txt, 
                                pdf_title, 
                                pdf_author, 
                                table, 
                                encoded_data
                            ) = extract_pdf.extract_pdf_info()
                            # Append scrape results from pdf to successes
                            extract_website.add_successful_scrape(
                                datetime_accessed=run_start_datetime,
                                language=detect(pdf_txt),
                                pdf_encode=encoded_data,
                                tables=table,
                                text=pdf_txt,
                                author=pdf_author,
                                title=pdf_title,
                                _date = article_date,
                                _type=document_types[1],
                                url=pdf_link,
                            )
                
                        except Exception as err:
                            # Catches errors and logs to file & errors list
                            error_handler.pdf_errors(pdf_link, f"{err}")
            # Catches an error if one encountered when downloading pdf
            except Exception as err:
                # Catches errors and logs to file & errors list
                error_handler.pdf_errors(f"{err}")
                print(err,'pdf error')
            # appends scrape results to successes
            extract_website.add_successful_scrape(
                datetime_accessed=run_start_datetime,
                _date = article_date,
                _type=document_types[1],
                url=f"{domain}{link}",
                title=article_title
            )

    return {
        'metadata': {
            "query_start_date": f"{start['year']}-{start['month']}-{start['day']}", 
            "query_end_date": f"{end['year']}-{end['month']}-{end['day']}",
            "run_start_datetime": run_start_datetime,
            "schema": "v2"
        },
        'errors': error_handler.errors,
        'successes': extract_website.successes
    }

def run(filename: str):
    """
    This function will be the main entrypoint to your code and will be called with a filename.
    """

    document_types = ['SPEECH', 'PRESS_RELEASE', 'INTERVIEWS']
    today = date.today()
    start_date = today + timedelta(days=-100)
    end_date = today + timedelta(days=1)

    start_dt_dict = {
        "year":start_date.year,
        "month":start_date.month,
        "day":start_date.day
    }

    end_dt_dict = {
        "year":end_date.year,
        "month":end_date.month,
        "day":end_date.day
    }

    output_json = run_scrape(start_dt_dict, end_dt_dict, document_types)

    with open(filename, 'w', encoding='utf-8') as json_file:
        json.dump(output_json, json_file, ensure_ascii = False, indent=4)

if __name__ == '__main__':
    run("sample")
    finish = time.perf_counter()
    print(round(finish - start))

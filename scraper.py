import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
from concurrent.futures import ThreadPoolExecutor, as_completed


def list_files(root_url, path_url):
    url = root_url + path_url
    files = []
    req = requests.get(url)
    parsed = BeautifulSoup(req.text, 'html.parser')
    for row in parsed.find_all('tr', ['odd', 'even']):
        for cell in row.find_all('td'):
            for link in cell.find_all('a', href=True):
                files.append({'filename': link.get_text(),
                              'link': root_url + link['href'],
                             'date': link.get_text().split('_')[1].split('.')[0]})
                print('Returning the files ' + str(len(files)))
    return files


def download_files(files, path, max_trheads=8):
    if not os.path.exists(path):
        os.makedirs(path)

    with ThreadPoolExecutor() as executor:
        futures = []
        for file in files:
            #print('Generating file: '+ file['filename'])
            futures.append(executor.submit(download_file, path=path, file=file))
        for future in as_completed(futures):
            print(future.result())


def download_file(path, file):
    filename = path + file['filename']
    print('Downloading ' + filename + ' ...')
    with requests.get(file['link']) as req:
        with open(filename, 'wb') as f:
            for chunk in req.iter_content(chunk_size=8192):
                if chunk:
                    print(chunk)
                    f.write(chunk)
    return ('Saved the file ' + filename + ' from ' + file['link'])


def gen_big_csv(path_in, path_out):
    column_names = ['year', 'month', 'day', 'hour', 'price']
    df = pd.DataFrame(columns=column_names)
    for root, dirs, files, in os.walk(path_in):
        for file in files:
            if file.endswith('.1'):
                df = pd.concat([df, pd.read_csv(os.path.join(root, file), sep=';', names=column_names, header=None).dropna(how='any', axis=0)])
    df.to_csv(path_out)

if __name__ == '__main__':
    root_url = 'https://www.omie.es'
    path_url = '/es/file-access-list?parents%5B0%5D=/&parents%5B1%5D=Mercado%20Diario&parents%5B2%5D=1.%20Precios&dir' \
               '=Precios%20horarios%20del%20mercado%20diario%20en%20Espa%C3%B1a&realdir=marginalpdbc'

    path = './data/raw/'

    files = list_files(root_url, path_url)
    #print(files)

    gen_big_csv(path, './data/final_dataset.csv')

    download_files(files, path)

    #download_file(path, {'filename': 'marginalpdbc_20210401.1', 'link': 'https://www.omie.es/es/file-download?parents%5B0%5D=marginalpdbc&filename=marginalpdbc_20210428.1'})

    gen_big_csv(path, './data/final_dataset.csv')

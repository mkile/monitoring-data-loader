from requests import get, RequestException
from datetime import datetime, timedelta
from os import listdir, path, unlink
from zipfile import ZipFile
from sys import argv
import json

FOLDERS = ['./days/', './hours/', './nominations/']
INDICATORS = ['Nomination', 'Physical%20Flow', 'GCV', 'Allocation', 'Renomination']
PERIODTYPE = 'hour'
POINTS = ['de-tso-0001itp-00096exit', 'pl-tso-0001itp-00096entry']
BAD_LINKS_FILE = 'bad_links.txt'
ARCHIVE_FILE = 'data_archive.zip'
DIVIDER = '-----------------------'


class EntsogLink:
    # Класс для формирования раздельного списка ссылок
    def __init__(self, end_date, load_depth, folder, points=None, indicators=None, periodtype='day', type='xlsx'):
        if points is None:
            points = []
        if indicators is None:
            indicators = ['Physical Flow']
        load_depth += 1
        self.start_dates = [(end_date - timedelta(days=x + 1)).strftime('%Y-%m-%d')
                            for x in range(load_depth)]
        self.end_dates = [(end_date - timedelta(days=x)).strftime('%Y-%m-%d')
                          for x in range(load_depth)]
        if len(points) > 0:
            self.points = f'&pointDirection={",".join(points)}'
        else:
            self.points = ''
        self.indicators = indicators
        self.periodtype = periodtype
        self.folder = folder
        self.type = type
        if type != 'xlsx':
            self.delimiter = '&delimiter=semicolon'
        else:
            self.delimiter = ''

    def get_links(self):
        result = []
        index = 0
        for start_date, end_date in zip(self.start_dates, self.end_dates):
            for indicator in self.indicators:
                result.append({'link': f'https://transparency.entsog.eu/api/v1/operationaldata.{self.type}'
                                       f'?forceDownload=true&isTransportData=true&dataset=1&from={start_date}'
                                       f'&to={end_date}&indicator={indicator}&periodType={self.periodtype}{self.points}'
                                       f'&timezone=CET&periodize=0&limit=-1{self.delimiter}',
                               'folder': self.folder,
                               'filename': str(index)})
                index += 1
        return result


def delete_file(file_path):
    if path.isfile(file_path) or path.islink(file_path):
        unlink(file_path)
        

def delete_files_in_dirs(folders):
    # Очистка папок
    for folder_name in folders:
        print('Очищаем папку:', folder_name)
        for filename in listdir(folder_name):
            file_path = path.join(folder_name, filename)
            try:
                delete_file(file_path)
            except OSError as e:
                print(f'Удалить не удалось {file_path}. Причина: {e}')


def write_files(links, clear):
    # Загрузка файлов по ссылкам и сохранение их в папки
    bad_links = []
    if clear:
        delete_files_in_dirs(list(set([x['folder'] for x in links])))
    for index, line in enumerate(links):
        print('Загрузка по ссылке:', line['link'])
        # NOTE the stream=True parameter below
        try:
            file_type = 'xlsx' if '.xlsx' in line['link'].lower() else 'csv'
            with get(line['link'], stream=True) as r:
                r.raise_for_status()
                with open(f'{line["folder"]}{line["filename"]}.{file_type}', 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        # If you have chunk encoded response uncomment if
                        # and set chunk_size parameter to None.
                        # if chunk:
                        f.write(chunk)
                        print('|', end='')
                print('')
                print(f'Файл номер {index + 1} из {len(links)}, сохранен '
                      f'под именем {line["folder"]}{line["filename"]}.{file_type}')
        except RequestException as E:
            print(f'!!! Файл не загружен, возникла ошибка {E}. Ссылка сохранена.')
            bad_links.append(line)
    return bad_links


def archive_data():
    # Архивация собранных файлов
    delete_file(ARCHIVE_FILE)
    with ZipFile(ARCHIVE_FILE, 'w') as zipObj:
        for folder_name in FOLDERS:
            for filename in listdir(folder_name):
                file_path = path.join(folder_name, filename)
                zipObj.write(file_path, file_path)


def main():
    # Процедура загрузки данных
    # Конечная дата = день + 1
    end_date = (datetime.today() + timedelta(days=1))
    # Проверим наличие файла с недозагруженными ссылками
    if path.isfile(BAD_LINKS_FILE) or path.islink(BAD_LINKS_FILE):
        with open(BAD_LINKS_FILE, 'r') as json_file:
            try:
                bad_links = json.load(json_file)
            except (OSError, json.JSONDecodeError) as e:
                bad_links = ''
                delete_file(BAD_LINKS_FILE)
                print('Файл с незагруженными ссылками поврежден, загружаем с начала.')
    else:
        bad_links = []
        print('Незагруженные ссылки отсутствуют.')
    print(DIVIDER)
    # Если есть ссылки в файле, то грузим по ссылкам вместо обычного набора
    if len(bad_links) == 0:
        file_type = 'xlsx'
        if len(argv) > 1:
            if argv[1].lower() == 'c':
                print('Загружаем файлы в формате CSV')
                file_type = 'csv'
        else:
            print('Загружаем файлы в формате XLSX')
        print('Загрузка данных...')
        links = EntsogLink(end_date=end_date, load_depth=11, indicators=INDICATORS,
                           folder=FOLDERS[0], type=file_type).get_links()
        links += EntsogLink(end_date=end_date, load_depth=2, periodtype=PERIODTYPE,
                            folder=FOLDERS[1], type=file_type).get_links()
        links += EntsogLink(end_date=end_date, load_depth=2, indicators=INDICATORS,
                            points=POINTS,
                            folder=FOLDERS[2],
                            type=file_type).get_links()
        clear = True
    else:
        print('Обнаружены незагруженные данные. Попытаемся их дозагрузить...')
        links = bad_links
        clear = False
    bad_links = write_files(links, clear)

    print(DIVIDER)
    print('Загрузка завершена')
    if len(bad_links) > 0:
        try:
            with open(BAD_LINKS_FILE, 'w') as json_file:
                json.dump(bad_links, json_file)
            print(f'В файл {BAD_LINKS_FILE} сохранено {len(bad_links)} незагруженных ссылок.')
        except OSError as e:
            print('Ошибка записи не загруженных ссылок в файл.')
            print('Список не загруженных ссылок:', bad_links, sep='\n')
    else:
        print('Незагруженных ссылок нет.')
        delete_file(BAD_LINKS_FILE)
        print('Архивируем данные...')
        archive_data()
        print('Данные заархивированы.')
        print('')
    input()


if __name__ == "__main__":
    main()

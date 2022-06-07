import time

from requests import get, RequestException
from datetime import datetime, timedelta
from os import listdir, path, unlink, makedirs
from zipfile import ZipFile
from sys import argv
from json import loads
import smtplib
from smtplib import SMTPHeloError, SMTPAuthenticationError, SMTPNotSupportedError, SMTPException
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


FOLDERS = ['./days/', './hours/', './nominations/']
INDICATORS = ['Nomination', 'Physical%20Flow', 'GCV', 'Allocation', 'Renomination']
PERIODTYPE = 'hour'
POINTS = ['de-tso-0001itp-00096exit', 'pl-tso-0001itp-00096entry']
ARCHIVE_FOLDER = ['./archives/']
EMAIL_CREDS = 'email_creds.txt'
MAX_ARCHIVE_SIZE = 7000000
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
    delete_files_in_dirs(ARCHIVE_FOLDER)
    archive_number = 0
    for folder_name in FOLDERS:
        for filename in listdir(folder_name):
            file_path = path.join(folder_name, filename)
            with ZipFile(f'{ARCHIVE_FOLDER[0]}{archive_number}.zip', 'a') as zipObj:
                zipObj.write(file_path, file_path)
                if sum([zinfo.file_size for zinfo in zipObj.filelist]) >= MAX_ARCHIVE_SIZE:
                    archive_number += 1


def ensure_no_bad_links(bad_links):
    # Дозагрузка недогруженных ссылок
    print(f'Попытаемся дозагрузить {len(bad_links)} незагруженных ссылок')
    attempts = 5
    while len(bad_links) > 0 and attempts > 0:
        print(f'Осталось {attempts} попыток')
        bad_links = write_files(bad_links, False)
        if len(bad_links) > 0:
            time.sleep(30)
            attempts -= 1
    if attempts <= 0 and len(bad_links) > 0:
        print(f'За 5 попыток загрузить данные не удалось, недозагружено {len(bad_links)} ссылок.')
    else:
        print('Недозагруженных ссылок не осталось.')
    return


def send_email():
    # Отправка сформированных архивов
    with open("email_creds.txt", 'r') as jsonfile:
        email_creds = loads(jsonfile.read())
    smtp = smtplib.SMTP(f'smtp.{email_creds["server"]}', email_creds["port"])
    smtp.starttls()
    try:
        smtp.login(f'{email_creds["name"]}@{email_creds["server"]}', email_creds['password'])
    except (SMTPHeloError, SMTPAuthenticationError, SMTPNotSupportedError, SMTPException) as err:
        print(f'При попытке подключиться к почтовому ящику возникла ошибка {err}')
        print('Отправка файлов по почте прервана.')
        return
    for f in listdir(ARCHIVE_FOLDER[0]):
        msg = MIMEMultipart()
        msg['From'] = f'{email_creds["name"]}@{email_creds["server"]}'
        msg['To'] = email_creds["to"]
        # msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = f"Данные ENTSOG за {datetime.today().strftime('%Y-%m-%d')}"
        msg.attach(MIMEText(f"Отправляем архив с файлом {[path.basename(f)]}"))
        with open(path.join(ARCHIVE_FOLDER[0], f), "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=path.basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % path.basename(f)
        msg.attach(part)
        smtp.sendmail(msg['From'], msg['To'], msg.as_bytes())
        print(f'Файл {path.basename(f)} отправлен.')
    smtp.close()
    return


def check_or_create_folders(folders):
    for folder_name in folders:
        makedirs(folder_name, exist_ok=True)


def main():
    # Процедура загрузки данных
    check_or_create_folders(FOLDERS + ARCHIVE_FOLDER)
    # Конечная дата = день + 1
    end_date = (datetime.today() + timedelta(days=1))
    print(DIVIDER)
    # Если есть ссылки в файле, то грузим по ссылкам вместо обычного набора
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
    bad_links = write_files(links, True)
    print(DIVIDER)
    ensure_no_bad_links(bad_links)
    print('Загрузка завершена')
    print('Архивируем данные...')
    archive_data()
    print('Данные заархивированы.')
    send_email()
    print('')


if __name__ == "__main__":
    main()

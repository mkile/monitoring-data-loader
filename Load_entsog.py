# -*- coding: utf8 -*-

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
# from ftplib import FTP
import logging
# Пути к файлам и папкам
FOLDERS = ['./days/', './hours/', './nominations/']
ARCHIVE_FOLDER = ['./archives/']
EMAIL_CREDS = './email_creds.txt'
LOG_FILE_PATH = './entsog.log'
# Прочие константы
INDICATORS = ['Nomination', 'Physical%20Flow', 'GCV', 'Allocation', 'Renomination']
PERIODTYPE = 'hour'
POINTS = ['de-tso-0001itp-00096exit', 'pl-tso-0001itp-00096entry']
MAX_ARCHIVE_SIZE = 7000000
DIVIDER = '-----------------------'


class EntsogLink:
    # Класс для формирования раздельного списка ссылок
    def __init__(self, end_date, load_depth, folder, points=None, indicators=None, periodtype='day', ftype='xlsx'):
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
        self.type = ftype
        if ftype != 'xlsx':
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
    logger = logging.getLogger(__name__)
    if path.isfile(file_path) or path.islink(file_path):
        unlink(file_path)
        logger.info(f"Удален файл {file_path}")


def delete_files_in_dirs(folders):
    # Очистка папок
    logger = logging.getLogger(__name__)
    for folder_name in folders:
        logger.info(f'Очищаем папку:{folder_name}')
        for filename in listdir(folder_name):
            file_path = path.join(folder_name, filename)
            try:
                delete_file(file_path)
            except OSError as e:
                logger.warning(f'Удалить не удалось {file_path}. Причина: {e}')


def write_files(links, clear):
    # Загрузка файлов по ссылкам и сохранение их в папки
    logger = logging.getLogger(__name__)
    bad_links = []
    if clear:
        delete_files_in_dirs(list(set([x['folder'] for x in links])))
    for index, line in enumerate(links):
        logger.info(f'Загрузка по ссылке:{line["link"]}')
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
                logger.info(f'Файл номер {index + 1} из {len(links)}, сохранен '
                            f'под именем {line["folder"]}{line["filename"]}.{file_type}')
        except RequestException as E:
            logger.warning(f'!!! Файл не загружен, возникла ошибка {E}. Ссылка сохранена.')
            bad_links.append(line)
    return bad_links


def archive_data():
    # Архивация собранных файлов
    logger = logging.getLogger(__name__)
    delete_files_in_dirs(ARCHIVE_FOLDER)
    archive_number = 0
    try:
        for folder_name in FOLDERS:
            for filename in listdir(folder_name):
                file_path = path.join(folder_name, filename)
                with ZipFile(f'{ARCHIVE_FOLDER[0]}{archive_number}.zip', 'a') as zipObj:
                    zipObj.write(file_path, file_path)
                    if sum([zinfo.file_size for zinfo in zipObj.filelist]) >= MAX_ARCHIVE_SIZE:
                        archive_number += 1
                        logger.info('Формируется новый архив')
    except IOError as Err:
        logger.error(f'При формировании архивов возникла ошибка {Err}')


def ensure_no_bad_links(bad_links):
    # Дозагрузка недогруженных ссылок
    logger = logging.getLogger(__name__)
    logger.info(f'Попытаемся дозагрузить {len(bad_links)} незагруженных ссылок')
    attempts = 5
    while len(bad_links) > 0 and attempts > 0:
        logger.info(f'Осталось {attempts} попыток')
        bad_links = write_files(bad_links, False)
        if len(bad_links) > 0:
            time.sleep(30)
            attempts -= 1
    if attempts <= 0 and len(bad_links) > 0:
        logger.error(f'За 5 попыток загрузить данные не удалось, недозагружено {len(bad_links)} ссылок.')
    else:
        logger.info('Недозагруженных ссылок не осталось.')
    return


def send_email():
    # Отправка сформированных архивов
    logger = logging.getLogger(__name__)
    logger.info('Отправим архивы по почте...')
    try:
        with open(EMAIL_CREDS, 'r') as jsonfile:
            email_creds = loads(jsonfile.read())
    except IOError as Err:
        logger.error(f'Ошибка загрузки данных для отправки почты: {Err}. Отправка почты прервана.')
        return
    smtp = smtplib.SMTP(f'smtp.{email_creds["server"]}', email_creds["port"])
    smtp.starttls()
    logger.info('Авторизация на почтовом сервере ...')
    try:
        smtp.login(f'{email_creds["name"]}@{email_creds["server"]}', email_creds['password'])
    except (SMTPHeloError, SMTPAuthenticationError, SMTPNotSupportedError, SMTPException) as err:
        logger.error(f'При попытке подключиться к почтовому ящику возникла ошибка {err}')
        logger.error('Отправка файлов по почте прервана.')
        return
    for f in listdir(ARCHIVE_FOLDER[0]):
        logger.info('Готовимся к отправке файла...')
        msg = MIMEMultipart()
        msg['From'] = f'{email_creds["name"]}@{email_creds["server"]}'
        msg['To'] = email_creds["to"]
        msg['Subject'] = f"Данные ENTSOG за {datetime.today().strftime('%Y-%m-%d')}"
        msg.attach(MIMEText(f"Отправляем архив с файлом {[path.basename(f)]}"))
        logger.info(f'Читаем файл: {path.basename(f)}')
        try:
            with open(path.join(ARCHIVE_FOLDER[0], f), "rb") as fil:
                part = MIMEApplication(
                    fil.read(),
                    Name=path.basename(f)
                )
                # After the file is closed
            part['Content-Disposition'] = 'attachment; filename="%s"' % path.basename(f)
            msg.attach(part)
            smtp.sendmail(msg['From'], msg['To'], msg.as_bytes())
            logger.info('Файл {path.basename(f)} отправлен.')
        except IOError as Err:
            logger.error(f'Отправка файла {path.basename(f)} прервана, он будет пропущен. Ошибка {Err}')
    smtp.close()
    logger.info('Отправка по почте завершена.')
    return


def check_or_create_folders(folders):
    logger = logging.getLogger(__name__)
    logger.info('Проверяем наличие нужных папок.')
    for folder_name in folders:
        logger.info(f'Проверка папки {folder_name}')
        makedirs(folder_name, exist_ok=True)


def main():
    # Процедура загрузки данных
    logger = logging.getLogger(__name__)
    check_or_create_folders(FOLDERS + ARCHIVE_FOLDER)
    # Конечная дата = день + 1
    end_date = (datetime.today() + timedelta(days=1))
    logger.info(DIVIDER)
    # Если есть ссылки в файле, то грузим по ссылкам вместо обычного набора
    file_type = 'xlsx'
    if len(argv) > 1:
        logger.info(f'Файл запущен с параметром: {argv[1]}')
        if argv[1].lower() == 'c':
            logger.info('Загружаем файлы в формате CSV')
            file_type = 'csv'
    else:
        logger.info('Загружаем файлы в формате XLSX')
    logger.info('Загрузка данных...')
    links = EntsogLink(end_date=end_date, load_depth=11, indicators=INDICATORS,
                       folder=FOLDERS[0], ftype=file_type).get_links()
    links += EntsogLink(end_date=end_date, load_depth=2, periodtype=PERIODTYPE,
                        folder=FOLDERS[1], ftype=file_type).get_links()
    links += EntsogLink(end_date=end_date, load_depth=2, indicators=INDICATORS,
                        points=POINTS,
                        folder=FOLDERS[2],
                        ftype=file_type).get_links()
    bad_links = write_files(links, True)
    print(DIVIDER)
    ensure_no_bad_links(bad_links)
    logger.info('Загрузка завершена')
    logger.info('Архивируем данные...')
    archive_data()
    logger.info('Данные заархивированы.')
    send_email()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, filename=LOG_FILE_PATH, format='[%(asctime)s] %(message)s')
    logging.FileHandler.mode = 'a'
    logger = logging.getLogger(__name__)
    logger.info("Начинаем работу...")
    main()
    logger.info("Заканчиваем работу.")

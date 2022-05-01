from requests import get, RequestException
from datetime import datetime, timedelta

end_date = (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d')
start_date_days = (datetime.today() - timedelta(days=11)).strftime('%Y-%m-%d')
start_date_hours = (datetime.today() - timedelta(days=3)).strftime('%Y-%m-%d')
start_date_nominations = start_date_hours

print(end_date, start_date_days, start_date_hours, start_date_nominations, sep='\n')

link_days = f'https://transparency.entsog.eu/api/v1/operationalData.xlsx?forceDownload=true&' \
            f'isTransportData=true&dataset=1&from={start_date_days}&to={end_date}&indicator=Nomination,Renomination,' \
            f'Allocation,Physical%20Flow&periodType=day&timezone=CET&periodize=0&limit=-1'
link_hours = f'https://transparency.entsog.eu/api/v1/operationaldata.xlsx?forceDownload=true&' \
             f'isTransportData=true&dataset=1&from={start_date_hours}&to={end_date}&indicator=Physical%20Flow&' \
             f'periodType=hour&timezone=CET&periodize=0&limit=-1'
link_hours_nominations = f'https://transparency.entsog.eu/api/v1/operationaldata.xlsx?forceDownload=true&' \
                         f'pointDirection=de-tso-0001itp-00096exit,pl-tso-0001itp-00096entry&' \
                         f'from={start_date_nominations}&' \
                         f'to={end_date}&indicator=Nomination,Renomination,Allocation&periodType=day&timezone=' \
                         f'&periodize=0&limit=-1&isTransportData=true&dataset=1'


def write_file(link, filename):
    # NOTE the stream=True parameter below
    try:
        with get(link, stream=True) as r:
            r.raise_for_status()
            with open(filename + '.xlsx', 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    # If you have chunk encoded response uncomment if
                    # and set chunk_size parameter to None.
                    # if chunk:
                    f.write(chunk)
                    print('|', end='')
    except E as RequestException:
        print(E)
        input()
    print('')
    print(f'Файл {filename} сохранен')


print('Загружаем файл по дням')
write_file(link_days, f'entsog_days{end_date}')
print('Загружаем файл по часам')
write_file(link_hours, f'entsog_hours{end_date}')
print('Загружаем номинации по часам')
write_file(link_hours_nominations, f'entsog_hours_nominations{end_date}')

print('Загрузка завершена')
input()

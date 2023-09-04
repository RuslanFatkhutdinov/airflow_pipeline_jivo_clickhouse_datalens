import requests
import pandas as pd
from pandas.io.json import json_normalize
import json
import boto3

API_TOKEN = '<ТОКЕН АВТОРИЗАЦИИ>'                                     # Добавить токен авторизации
URL = '<ENDPOINT>'                                                    # Добавить эндопинт API


def get_jivo_events():
    """ Получает данные по событиям Jivo из API
    """
    headers = {
        'Api-key': API_TOKEN
    }

    response = requests.get(f'{URL}?limit=1000', headers=headers)

    json_object = json.dumps(response.json()['webhooks'], ensure_ascii=False)

    with open('events.json', "w", encoding='utf8') as f:
        f.write(json_object)


if __name__ == "__main__":
    print('Начинаем')
    get_jivo_events()
    print('Закончили')
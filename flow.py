from prefect import flow, task
from database import Database
import requests
from bs4 import BeautifulSoup
import re

@task
def collect_disasters(max_disasters: int):
    req = requests.get("https://www.airdisaster.ru/database.php?y=all")
    src = req.text
    soup = BeautifulSoup(src, features="html.parser")
    result = []
    for link in soup.find_all('a'):
        if len(result) == max_disasters:
            break
        data = dict()
        if 'class' in link.parent.attrs.keys() and link.parent.attrs['class'][0] == 'tdh2':
            data['link'] = f"https://www.airdisaster.ru/{link.get('href')}"
            td_list = link.parent.parent.find_all(attrs={"class": "tdh2"})
            data['aircraft'] = td_list[1]
            data['country'] = td_list[2]
            data['location'] = td_list[3]
            result.append(data)
    return result

@task()
def process_disasters(disasters):
    for data in disasters:
        match = re.search('<center>(.*)<br/><b>(.*)</b></center>', str(data['aircraft']))
        if match:
            data['aircraft'] = match.group(1)
            data['registration_number'] = match.group(2)

        match = re.search('<center>(.*)</center>', str(data['country']))
        if match:
            data['country'] = match.group(1)

        match = re.search('>(.*)<', str(data['location']))
        if match:
            data['location'] = match.group(1)

    return disasters


@task()
def save_to_database(disasters):
    db = Database()
    db.save_disasters(disasters)

@flow
def air_disaster_flow(max_disasters: int):
    disasters = collect_disasters(max_disasters)
    processed_disasters = process_disasters(disasters)
    save_to_database(processed_disasters)
    return processed_disasters


import datetime
import logging
import requests
import json
# import threading
import time
from settings import NAME_DB, URL, QUERY_COLLECTION, ICY_TOKEN
from db import BotDB
from TGbot import send_msg_all, send_msg_developer
from urllib3.exceptions import NewConnectionError

logger = logging.getLogger('CollectionWorkerScript')


def text_for_message(address, stats, first=True):
    return (f"""Алгоритм {1 if first else 2} / {f"5мин ({stats['checkpoint']})" if not first else "30 мин"}\n"""
            f"Проект: {stats['name'].replace('&', '')}\n"
            f"Количество продаж: {stats['sale']}\n"
            f"Адрес: {address}\n"
            f"Supply: {stats['supply']}\n"
            f"https://icy.tools/collections/{address}/overview\n"
            )


def start_database():
    db = BotDB(NAME_DB)
    db.create_table()
    db.close()


def main():
    db = BotDB(NAME_DB)
    now_utc = datetime.datetime.utcnow()
    five_ago = now_utc - datetime.timedelta(minutes=5)
    json_param = {
        'query': QUERY_COLLECTION,
        'variables': {
            "timeRange": {
                "gte": f"{five_ago}"
            }
        }
    }
    headers = {"x-api-key": ICY_TOKEN}
    try:
        response = requests.post(URL, headers=headers, json=json_param)
        if response.status_code == 200:
            json_data = response.json()
            edges = json_data['data']['trendingCollections']['edges']
            collection(edges, db)
            collection_count(edges, db)
        else:
            send_msg_developer(f"{response}")
            logger.error(f"ERROR RESPONSE: {response}")

    except Exception as e:
        print(f"Fatal ERROR: {e}")
        logger.error(f"Fatal ERROR: {e}")

    db.close()
    # Для локального запуска в цикле
    # threading.Timer(15, main).start()


def collection(sales, db):
    # algo 1
    active_address = db.get_all_address()
    rows_create = []
    rows_create_history = []
    row_delete_address = []
    text = []
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for edge in sales:
        node = edge['node']
        address = node['address']
        total_sale = node['stats']['totalSales']
        name = node['name']
        supply = node['circulatingSupply']
        if not active_address.get(address) and total_sale >= 20:
            # 2 кейс
            rows_create.append((node['name'], address, now, total_sale))
        elif active_address.get(address) and active_address[address] * 2 < total_sale:
            # 3 кейс
            text.append(text_for_message(address, {'name': name, 'sale': total_sale, 'supply': supply}))
            rows_create_history.append((node['name'], address, now, total_sale))
            row_delete_address.append(address)
    db.create_new_inset(rows_create)
    db.create_new_history_inset(rows_create_history)
    if text:
        print(text)
        logger.info(f"NEW TOP ALGO 1 {text}")
    else:
        print("Алгоритм 1 нет новых топов", datetime.datetime.now())
        logger.info(f"NOT TOP ALGO 1 {datetime.datetime.now()}")
    send_msg_all(text)
    # delete old
    time_delta = (datetime.datetime.now() - datetime.timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
    db.delete_collections(row_delete_address, time_delta)


def collection_count(sales, db):
    # algo 2
    top_sale = {}
    for edge in sales:
        node = edge['node']
        if node['stats']['totalSales'] >= 50:
            total_sale = node['stats']['totalSales']
            address = node['address']
            name = node['name']
            supply = node['circulatingSupply']
            top_sale.update({address: {'name': name, 'sale': total_sale, 'supply': supply}})
    if top_sale:
        bd_address_dict = db.get_all_address_and_checkpoint()
        final_list_create, final_list_update = {}, {}
        for address, stats in top_sale.items():
            count_sale = stats['sale']
            checkpoint = 50
            if count_sale >= 180:
                checkpoint = 180
            elif count_sale >= 130:
                checkpoint = 130
            elif count_sale >= 80:
                checkpoint = 80
            stats.update({"checkpoint": checkpoint})

            if address not in bd_address_dict.keys():
                final_list_create.update({address: stats})
            else:
                if checkpoint > bd_address_dict[address]:
                    final_list_update.update({address: stats})

        if final_list_create or final_list_update:
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            text = []
            if final_list_create:
                rows_create = []
                for address, stats in final_list_create.items():
                    rows_create.append((stats['name'], address, stats['checkpoint'], now))
                    text.append(text_for_message(address, stats, False))
                db.create_new_count_inset(rows_create)
                
            if final_list_update:
                for address, stats in final_list_update.items():
                    text.append(text_for_message(address, stats, False))
                    db.update_checkpoint_inset(stats['checkpoint'], address, now)
            print(text)
            logger.info(f"NEW TOP ALGO 2 {text}")
            send_msg_all(text)

    else:
        print("Алгоритм 2 нет новых топов", datetime.datetime.now())
        logger.info(f"NOT TOP ALGO 2 {datetime.datetime.now()}")

    time_delta = (datetime.datetime.now() - datetime.timedelta(minutes=60)).strftime('%Y-%m-%d %H:%M:%S')
    db.delete_collections_count(time_delta)

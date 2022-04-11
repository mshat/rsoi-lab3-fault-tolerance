import redis
import requests
import threading
from time import time, sleep
from requests.exceptions import ConnectionError
from env import get_uri
import my_requests


REDIS_URL = 'redis://redistogo:c1c4907a8e76ab844e2b50672864245f@sole.redistogo.com:9830/'
#REDIS_URL = 'redis://:p12c5751508cd695a37a1811b048cdaccbe68ad03c62aefb7efb35bbac8044e38@ec2-44-196-64-205.compute-1.amazonaws.com:21370'

OPEN, CLOSED = 0, 1


def delete_reservation_worker(redis_instance):
    tag = "delete_reservation_worker"
    while True:
        if redis_instance.llen('str:delete_reservation_usernames:1') > 0:
            print(f"[{tag}] len of delete_reservation_usernames > 0!!!")
            username = redis_instance.lpop('str:delete_reservation_usernames:1').decode('UTF-8')

            redis_keys = list(map(bytes.decode, redis_instance.keys()))
            if not (f'str:delete_reservation_uid:{username}' in redis_keys and
                    f'str:delete_reservation_time:{username}' in redis_keys):
                redis_instance.rpush('str:delete_reservation_usernames:1', username)
                continue

            uid = redis_instance.get(f'str:delete_reservation_uid:{username}').decode('UTF-8')
            last_time = redis_instance.get(f'str:delete_reservation_time:{username}').decode('UTF-8')

            print(f"[{tag}] USERNAME: " + username)
            print(f"[{tag}] UID: " + uid)
            print(f"[{tag}] LAST_TIME: " + last_time)

            print(f"[{tag}] time passed: " + str(time() - float(last_time)))
            if time() - float(last_time) > 10:
                try:
                    print(f'[{tag}] Trying to execute request')
                    delete_response = requests.delete(str(get_uri('loyalty')), headers={"X-User-Name": username})
                except ConnectionError as e:
                    redis_instance.rpush('str:delete_reservation_usernames:1', username)
                    redis_instance.set(f'str:delete_reservation_time:{username}', str(time()))
                    print(f"[{tag}] request failed error: {e}")
                    continue
                if delete_response.status_code == 200:
                    print(f"[{tag}] delete loyalty request succsessful")
                else:
                    redis_instance.rpush('str:delete_reservation_usernames:1', username)
                    redis_instance.set(f'str:delete_reservation_time:{username}', str(time()))
                    print(f"[{tag}] request failed: {delete_response.status_code} code")
                    continue

                reservation_service_uri = get_uri('reservation')
                reservation_service_uri.path = f"reservation/{uid}"
                reservation_get_request = my_requests.GetRequest(reservation_service_uri)
                reservation_response = reservation_get_request.send()
                reservation = reservation_response.json()

                print(f"[{tag}] reservation get request succsessful")

                payment_service_uri = get_uri('payment')
                payment_service_uri.path = f"payment/{reservation['payment_uid']}"
                payment_status_patch_request = my_requests.PatchRequest(payment_service_uri,
                                                                        data={"status": "CANCELED"})
                payment_status_patch_request.send()

                print(f"[{tag}] payment status patch request succsessful")

                reservation_service_uri.path = f"reservation/{uid}"
                reservation_status_patch_request = my_requests.PatchRequest(reservation_service_uri,
                                                                            data={"status": "CANCELED"})
                reservation_status_patch_request.send()

                print(f"[{tag}] reservation status patch request succsessful")

                redis_instance.delete(f'str:delete_reservation_uid:{username}',
                                      f'str:delete_reservation_time:{username}')
            else:
                redis_instance.rpush('str:delete_reservation_usernames:1', username)
                sleep(1)
                print(f'[{tag}] Sleep 1 second because 10 seconds not less')
        else:
            sleep(1)
            print(f'[{tag}] Sleep 1 second because queue is empty')
            keys = redis_instance.keys()
            print(f'\nredis_instance.keys:\n{keys}\n\n')
            
            
def circuit_breaker_worker(redis_instance):
    tag = 'circuit_breaker_worker'
    while True:
        if redis_instance.llen('str:circuit_breaker_items:1') > 0:
            print(f"[{tag}] len of circuit_breaker_items > 0!!!")
            url = redis_instance.lpop('str:circuit_breaker_items:1').decode('UTF-8')

            redis_keys = list(map(bytes.decode, redis_instance.keys()))
            if not (f'int:circuit_breaker_state:{url}' in redis_keys and
                    f'str:circuit_breaker_time:{url}' in redis_keys):
                redis_instance.rpush('str:circuit_breaker_items:1', url)
                continue

            state = redis_instance.get(f'int:circuit_breaker_state:{url}').decode('UTF-8')
            last_time = redis_instance.get(f'str:circuit_breaker_time:{url}').decode('UTF-8')

            if int(state) == CLOSED:
                redis_instance.rpush('str:circuit_breaker_items:1', url)
                continue

            print(f"[{tag}] URL: {url}")
            print(f"[{tag}] STATE: {state}")
            print(f"[{tag}] LAST_TIME: {last_time}")

            print(f"[{tag}] time passed: " + str(time() - float(last_time)))
            if time() - float(last_time) > 5:  # TODO потом получать из редиса
                try:
                    print(f'[{tag}] Health check request')
                    response = requests.get(url)
                except ConnectionError as e:
                    redis_instance.rpush('str:circuit_breaker_items:1', url)
                    redis_instance.set(f'str:circuit_breaker_time:{url}', str(time()))
                    print(f"[{tag}] request failed error: {e}")
                    continue
                if response.status_code // 100 == 5:
                    redis_instance.rpush('str:circuit_breaker_items:1', url)
                    redis_instance.set(f'str:circuit_breaker_time:{url}', str(time()))
                    print(f"[{tag}] request failed: {response.status_code} code")
                    continue
                else:
                    redis_instance.set(f'int:circuit_breaker_state:{url}', CLOSED)
                    redis_instance.rpush('str:circuit_breaker_items:1', url)
                    print(f"[{tag}] Health check request succeeded")
                    continue
            else:
                redis_instance.rpush('str:circuit_breaker_items:1', url)
                sleep(1)
                print(f'[{tag}] Sleep 1 second because 10 seconds not less')
        else:
            sleep(1)
            print(f'[{tag}] Sleep 1 second because queue is empty')


if __name__ == '__main__':
    redis_conn = redis.from_url(REDIS_URL)
    redis_conn .flushdb()
    print("REDIS STARTED")

    th1 = threading.Thread(target=delete_reservation_worker, args=(redis_conn,))
    th2 = threading.Thread(target=circuit_breaker_worker, args=(redis_conn,))

    th1.start()
    th2.start()
    th1.join()
    th2.join()

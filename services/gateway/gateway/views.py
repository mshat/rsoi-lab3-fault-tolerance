import json
import sys
import time
import requests
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import status
from redis import Redis, from_url
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from .env import get_uri
sys.path.append("..")
from my_modules.circuit_breaker import CircuitBreaker, Fallback

# REDIS_URL = 'redis://redis:6379'
REDIS_URL = 'redis://redistogo:c1c4907a8e76ab844e2b50672864245f@sole.redistogo.com:9830/'
#REDIS_URL = 'redis://:p12c5751508cd695a37a1811b048cdaccbe68ad03c62aefb7efb35bbac8044e38@ec2-44-196-64-205.compute-1.amazonaws.com:21370'
redis_instance = from_url(REDIS_URL)

circuit_breaker = CircuitBreaker(redis_instance, errors_num_before_opening=10)


class PersonView(APIView):
    def get(self, request):
        reservation_service_uri = get_uri('reservation')
        reservation_service_uri.path = 'reservations'
        reservations_response = circuit_breaker.get(reservation_service_uri)
        reservations_data = reservations_response.json()

        loyalty_service_uri = get_uri('loyalty')
        loyalty_response = circuit_breaker.get(loyalty_service_uri, headers={"X-User-Name": "Test Max"})

        if loyalty_response.status_code == 200:
            loyalty_data = loyalty_response.json()
        else:
            loyalty_data = ''

        data = {
            "reservations": reservations_data,
            "loyalty": loyalty_data,
        }
        return Response(status=status.HTTP_200_OK, data=data)


class HotelsListView(APIView):
    def get(self, request):
        reservation_service_uri = get_uri('reservation')
        reservation_service_uri.path = 'hotels'
        if request.META['QUERY_STRING']:
            reservation_service_uri.query = request.META['QUERY_STRING']
        else:
            raise Exception()

        response = circuit_breaker.get(reservation_service_uri)
        return Response(status=response.status_code, data=response.json())


class ReservationsListView(APIView):
    def get(self, request):
        reservation_service_uri = get_uri('reservation')
        reservation_service_uri.path = 'reservations'
        response = circuit_breaker.get(reservation_service_uri)
        return Response(status=response.status_code, data=response.json())

    def post(self, request):
        reservation_service_uri = get_uri('reservation')
        loyalty_service_uri = get_uri('loyalty')

        username = request.headers["x-user-name"]
        request_data = request.data
        hotel_uid = request_data["hotelUid"]
        start_date = request_data["startDate"]
        end_date = request_data["endDate"]

        # Запрос к Reservation Service для проверки, что такой отель существует
        try:
            reservation_service_uri.path = 'hotels'
            hotels_get_response = requests.get(str(reservation_service_uri))
        except Exception as e:
            return Response(status=status.HTTP_503_SERVICE_UNAVAILABLE,
                            data={'message': 'Reservation Service unavailable'})
        if hotels_get_response.status_code == 503:
            return Response(status=status.HTTP_503_SERVICE_UNAVAILABLE,
                            data={'message': 'Reservation Service unavailable'})
        elif hotels_get_response.status_code == 200:
            hotels_get_response_data = hotels_get_response.json()

            hotel_found = False
            for hotel in hotels_get_response_data:
                if hotel_uid == hotel['hotelUid']:
                    hotel_found = True
                    break
            if not hotel_found:
                return Response(status=status.HTTP_404_NOT_FOUND, data={'message': 'Hotel not found'})

        # Запрос к Loyalty Service для увеличения счетчика бронирований
        try:
            loyalty_patch_response = requests.patch(str(loyalty_service_uri), headers={"X-User-Name": username})
        except Exception as e:
            return Response(status=status.HTTP_503_SERVICE_UNAVAILABLE, data={'message': 'Loyalty Service unavailable'})
        if loyalty_patch_response.status_code == 503:
            return Response(status=status.HTTP_503_SERVICE_UNAVAILABLE, data={'message': 'Loyalty Service unavailable'})

        reservation_service_uri.path = 'reservations'
        reservation_post_response = requests.post(
            str(reservation_service_uri),
            data=json.dumps({"hotelUid": hotel_uid, "startDate": start_date, "endDate": end_date}),
            headers={"Content-Type": "application/json", "x-user-name": username}
        )

        if reservation_post_response.status_code == 503:
            return Response(status=status.HTTP_503_SERVICE_UNAVAILABLE, data={'message': 'Loyalty Service unavailable'})

        return Response(status=status.HTTP_200_OK, data=reservation_post_response.json())


class ReservationView(APIView):
    def get(self, request, uid):
        reservation_service_uri = get_uri('reservation')
        reservation_service_uri.path = f'reservation/{uid}'

        response = circuit_breaker.get(reservation_service_uri)
        return Response(status=response.status_code, data=response.json())

    def delete(self, request, uid):
        loyalty_service_uri = get_uri('loyalty')
        username = request.headers["x-user-name"]
        try:
            delete_loyalty_response = requests.delete(str(loyalty_service_uri), headers={"X-User-Name": username})
        except Exception as e:
            redis_instance.rpush('str:delete_reservation_usernames:1', username)
            redis_instance.set(f'str:delete_reservation_uid:{username}', uid)
            redis_instance.set(f'str:delete_reservation_time:{username}', str(time.time()))
            return Response(status=status.HTTP_204_NO_CONTENT)
        if delete_loyalty_response.status_code != 200:
            redis_instance.rpush('str:delete_reservation_usernames:1', username)
            redis_instance.set(f'str:delete_reservation_uid:{username}', uid)
            redis_instance.set(f'str:delete_reservation_time:{username}', str(time.time()))
            return Response(status=status.HTTP_204_NO_CONTENT)

        reservation_service_uri = get_uri('reservation')
        reservation_service_uri.path = f"reservation/{uid}"
        reservation_response = requests.get(reservation_service_uri.str)
        reservation = reservation_response.json()

        payment_service_uri = get_uri('payment')
        payment_service_uri.path = f"payment/{reservation['payment_uid']}"
        requests.patch(payment_service_uri.str, data={"status": "CANCELED"})

        reservation_service_uri.path = f"reservation/{uid}"
        requests.patch(reservation_service_uri.str, data={"status": "CANCELED"})

        return Response(status=status.HTTP_204_NO_CONTENT)


class LoyaltyView(APIView):
    def get(self, request):
        loyalty_service_uri = get_uri('loyalty')
        username = request.headers["x-user-name"]

        circuit_breaker.add_url_for_tracking(
            url=loyalty_service_uri,
            fallback=Fallback(status=status.HTTP_503_SERVICE_UNAVAILABLE,
                              data={'message': 'Loyalty Service unavailable'}))
        response = circuit_breaker.get(loyalty_service_uri, headers={"X-User-Name": username})
        if response.status_code != 200:  # TODO
            data = {'message': 'Loyalty Service unavailable'}
        else:
            data = response.json()
        return Response(status=response.status_code, data=data)


class PaymentsListView(APIView):
    def get(self, request):
        payment_service_uri = get_uri('payment')
        payment_service_uri.path = 'payments'

        response = circuit_breaker.get(payment_service_uri)
        return Response(status=response.status_code, data=response.json())

    def post(self, request):
        payment_service_uri = get_uri('payment')
        payment_service_uri.path = f'payments'

        price = request.data['price']
        response = requests.post(str(payment_service_uri), data={'price': price})
        if response.status_code == 200:
            return Response(status=status.HTTP_200_OK, data=response.json())
        else:
            return Response(status=status.HTTP_503_SERVICE_UNAVAILABLE, data={'message': 'Payment service unavailable'})


class PaymentView(APIView):
    def get(self, request, uid):
        payment_service_uri = get_uri('payment')
        payment_service_uri.path = f'payment/{uid}'

        circuit_breaker.add_url_for_tracking(
            url=payment_service_uri,
            fallback=Fallback(status=status.HTTP_503_SERVICE_UNAVAILABLE,
                              data={'message': 'Payment service unavailable'}))

        response = circuit_breaker.get(payment_service_uri)
        return Response(status=response.status_code, data=response.json())

        





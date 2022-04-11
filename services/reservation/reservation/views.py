import datetime
import requests
from rest_framework.response import Response
from django.http import Http404
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.generics import get_object_or_404, GenericAPIView
from .models import Hotel, Reservation
from .serializers import HotelSerializer, ReservationSerializer
from .pagination import CustomPagination
import sys
sys.path.append("..")
from .env import get_uri


class HotelsListView(GenericAPIView):
    pagination_class = CustomPagination
    serializer_class = HotelSerializer
    queryset = Hotel.objects.all()

    def get(self, request):
        queryset = self.filter_queryset(self.get_queryset())
        if request.META['QUERY_STRING']:
            page = self.paginate_queryset(queryset)
            serializer = self.get_serializer(page, many=True)
            result = self.get_paginated_response(serializer.data)
            data = result.data
        else:
            serializer = self.get_serializer(queryset, many=True)
            data = serializer.data
        return Response(status=status.HTTP_200_OK, data=data)


class ReservationsListView(APIView):
    def get(self, request):
        reservations = Reservation.objects.all()
        serializer = ReservationSerializer(reservations, many=True)

        gateway_service_uri = get_uri('gateway')
        gateway_service_uri.path = f'api/v1/payments'
        get_payments_response = requests.get(str(gateway_service_uri))
        if get_payments_response.status_code != 200:
            error_message = get_payments_response.json()
            payment_status = error_message
            payment_price = error_message
            payments = None
        else:
            payments_data = get_payments_response.json()

            payments = {}
            for payment in payments_data:
                payments.update({payment["paymentUid"]: payment})

        for reservation, serialized_reservation in zip(reservations, serializer.data):
            hotel = reservation.hotel_id
            payment_uid = str(reservation.payment_uid)
            if get_payments_response.status_code == 200:
                payment_status = payments[payment_uid]["status"]
                payment_price = payments[payment_uid]["price"]

            serialized_reservation.update({
                "hotel": {"hotelUid": hotel.hotelUid, "name": hotel.name,
                          "fullAddress": f"{hotel.country}, {hotel.city}, {hotel.address}", "stars": hotel.stars},
                "payment": {
                    "status": payment_status,
                    "price": payment_price
                }
            })
            serialized_reservation['status'] = reservation.status
        return Response(status=status.HTTP_200_OK, data=serializer.data)

    def post(self, request):
        username = request.headers['X-User-Name']

        gateway_service_uri = get_uri('gateway')
        gateway_service_uri.path = 'api/v1/loyalty'
        loyalty_get_response = requests.get(str(gateway_service_uri), headers={'X-User-Name': username})
        if loyalty_get_response.status_code != 200:
            return Response(status=loyalty_get_response.status_code, data=loyalty_get_response.json())
        loyalty_data = loyalty_get_response.json()
        if loyalty_data['status'] == 'B':
            discount = 0.05
        elif loyalty_data['status'] == 'S':
            discount = 0.07
        elif loyalty_data['status'] == 'G':
            discount = 0.1
        else:
            discount = 0

        start_date_list = list(map(int, request.data["startDate"].split('-')))
        start_date = datetime.date(start_date_list[0], start_date_list[1], start_date_list[2])
        end_date_list = list(map(int, request.data["endDate"].split('-')))
        end_date = datetime.date(end_date_list[0], end_date_list[1], end_date_list[2])
        booking_time = (end_date - start_date).days
        hotelUid = request.data["hotelUid"]
        hotel = get_object_or_404(Hotel.objects.all(), hotelUid=hotelUid)
        booking_cost = hotel.price * booking_time
        booking_cost *= 1 - discount
        booking_cost = int(booking_cost)

        gateway_service_uri = get_uri('gateway')
        gateway_service_uri.path = f'api/v1/payments'
        post_payment_response = requests.post(str(gateway_service_uri), data={"price": booking_cost})
        if post_payment_response.status_code != 200:
            return Response(status=post_payment_response.status_code, data=post_payment_response.json())
        new_payment_data = post_payment_response.json()

        new_reservation = Reservation()
        new_reservation.username = username
        new_reservation.hotel_id = hotel
        new_reservation.status = 'PAID'
        new_reservation.startDate = start_date
        new_reservation.endDate = end_date
        new_reservation.payment_uid = new_payment_data["paymentUid"]
        new_reservation.save()

        response_json = {
            "reservationUid": new_reservation.reservationUid,
            "hotelUid": hotelUid,
            "startDate": str(start_date),
            "endDate": str(end_date),
            "discount": int(discount * 100),
            "status": "PAID",
            "payment": {
                "status": "PAID",
                "price": booking_cost
            }
        }
        return Response(status=status.HTTP_200_OK, data=response_json)


class ReservationView(APIView):
    def get(self, request, uid):
        try:
            reservation = get_object_or_404(Reservation.objects.all(), reservationUid=uid)
            serializer = ReservationSerializer(reservation)

            hotel = reservation.hotel_id

            payment_service_uri = get_uri('gateway')
            payment_service_uri.path = f'api/v1/payment/{reservation.payment_uid}'
            payment_response = requests.get(str(payment_service_uri))
            if payment_response.status_code == 200:
                payment_data = payment_response.json()
                payment_status = payment_data["status"]
                payment_price = payment_data["price"]
            else:
                error_message = payment_response.json()
                payment_status = error_message
                payment_price = error_message

            data = serializer.data
            data.update({
                "hotel": {"hotelUid": hotel.hotelUid, "name": hotel.name,
                          "fullAddress": f"{hotel.country}, {hotel.city}, {hotel.address}", "stars": hotel.stars},
                "payment": {
                    "status": payment_status,
                    "price": payment_price
                }
            })

            return Response(status=status.HTTP_200_OK, data=data)
        except Http404:
            return Response(status=status.HTTP_404_NOT_FOUND, data="Not found Reservation for ID")

    def patch(self, request, uid):
        reservation = get_object_or_404(Reservation.objects.all(), reservationUid=uid)
        reservation.username = request.data["username"] if "username" in request.data.keys() else reservation.username
        reservation.hotel_id = request.data["hotel_id"] if "hotel_id" in request.data.keys() else reservation.hotel_id
        reservation.status = request.data["status"] if "status" in request.data.keys() else reservation.status
        reservation.startDate = request.data["startDate"] if "startDate" in request.data.keys() else reservation.startDate
        reservation.endDate = request.data["endDate"] if "endDate" in request.data.keys() else reservation.endDate
        reservation.save()
        serializer = ReservationSerializer(reservation)
        return Response(status=status.HTTP_200_OK, data=serializer.data)

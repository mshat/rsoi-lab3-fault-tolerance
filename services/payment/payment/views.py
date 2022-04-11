import requests
from django.shortcuts import render
from rest_framework.response import Response
from django.http import Http404
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.generics import get_object_or_404
from .models import Payment
from .serializers import PaymentSerializer


class PaymentsListView(APIView):
    def get(self, request):
        payments = Payment.objects.all()
        serializer = PaymentSerializer(payments, many=True)
        return Response(status=status.HTTP_200_OK, data=serializer.data)

    def post(self, request):
        new_payment = Payment()
        new_payment.price = request.data["price"]
        new_payment.status = "PAID"
        new_payment.save()
        return Response(status=status.HTTP_200_OK, data={"paymentUid": new_payment.paymentUid})


class PaymentView(APIView):
    def get(self, request, uid):
        payment = get_object_or_404(Payment.objects.all(), paymentUid=uid)
        serializer = PaymentSerializer(payment)
        return Response(status=status.HTTP_200_OK, data=serializer.data)

    def patch(self, request, uid):
        payment = get_object_or_404(Payment.objects.all(), paymentUid=uid)
        payment.status = request.data["status"] if "status" in request.data.keys() else payment.status
        payment.price = request.data["price"] if "price" in request.data.keys() else payment.price
        payment.save()
        serializer = PaymentSerializer(payment)
        return Response(status=status.HTTP_200_OK, data=serializer.data)



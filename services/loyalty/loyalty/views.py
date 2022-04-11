from django.shortcuts import render
from rest_framework.response import Response
from django.http import Http404
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.generics import get_object_or_404
from .models import Loyalty
from .serializers import LoyaltySerializer


class LoyaltyView(APIView):
    def get(self, request):
        try:
            loyalty = get_object_or_404(Loyalty.objects.all(), username=request.headers['X-User-Name'])
            serializer = LoyaltySerializer(loyalty)
            return Response(status=status.HTTP_200_OK, data=serializer.data)
        except Http404:
            return Response(status=status.HTTP_404_NOT_FOUND, data="Not found loyalty for this user")

    def patch(self, request):
        loyalty = get_object_or_404(Loyalty.objects.all(), username=request.headers['X-User-Name'])
        loyalty.reservationCount += 1
        loyalty.save()
        serializer = LoyaltySerializer(loyalty)
        return Response(status=status.HTTP_200_OK, data=serializer.data)

    def delete(self, request):
        loyalty = get_object_or_404(Loyalty.objects.all(), username=request.headers['X-User-Name'])
        loyalty.reservationCount -= 1
        loyalty.save()
        serializer = LoyaltySerializer(loyalty)
        return Response(status=status.HTTP_200_OK, data=serializer.data)

from rest_framework import serializers
from .models import Loyalty


class LoyaltySerializer(serializers.ModelSerializer):
    class Meta:
        model = Loyalty
        fields = '__all__'

from django.urls import path
from . import views

urlpatterns = [
    path('loyalty', views.LoyaltyView.as_view()),
]
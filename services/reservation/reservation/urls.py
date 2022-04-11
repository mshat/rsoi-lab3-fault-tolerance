from django.urls import path
from . import views


urlpatterns = [
    path('hotels', views.HotelsListView.as_view()),
    path('reservations', views.ReservationsListView.as_view()),
    path('reservation/<str:uid>', views.ReservationView.as_view())
]

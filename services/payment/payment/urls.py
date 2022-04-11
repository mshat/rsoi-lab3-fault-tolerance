from django.urls import path
from . import views


urlpatterns = [
    path('payments', views.PaymentsListView.as_view()),
    path('payment/<str:uid>', views.PaymentView.as_view())
]
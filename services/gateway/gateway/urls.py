from django.urls import path
from . import views


# urlpatterns = [
#     path('persons', views.PersonListView.as_view()),
#     path('persons/<int:pk>', views.PersonView.as_view())
# ]

urlpatterns = [
    path('hotels', views.HotelsListView.as_view()),
    path('me', views.PersonView.as_view()),
    path('reservations', views.ReservationsListView.as_view()),
    path('reservations/<str:uid>', views.ReservationView.as_view()),
    path('loyalty', views.LoyaltyView.as_view()),
    path('payment/<str:uid>', views.PaymentView.as_view()),
    path('payments', views.PaymentsListView.as_view()),
]
"""
1. Получить список отелей
GET {{baesUrl}}/api/v1/hotels&page={{page}}&size={{size}}

2. Получить полную информацию о пользователе
GET {{baesUrl}}/api/v1/me
X-User-Name: {{username}}

3. Информация по всем бронированиям пользователя
GET {{baesUrl}}/api/v1/reservations
X-User-Name: {{username}}

4. Информация по конкретному бронированию
При запросе требуется проверить, что бронирование принадлежит пользователю.
GET {{baesUrl}}/api/v1/reservations/{{reservationUid}}
X-User-Name: {{username}}

5.Забронировать отель
Пользователь вызывает метод GET {{baseUrl}}/api/v1/hotels и выбирает нужный отель и в запросе на бронирование передает:

hotelUid (UUID отеля) – берется из запроса /hotels;
startDate и endDate (дата начала и конца бронирования) – задается пользователем.
Система проверяет, что отель с таким hotelUid существует. Считаем что в отеле бесконечное количество мест.

Считается количество ночей (endDate – startDate), вычисляется общая сумма бронирования, выполняется обращение в Loyalty Service и получается скидка в зависимости от статуса клиента:

BRONZE – 5%
SILVER – 7%
GOLD – 10%
После применения скидки выполняется запрос в Payment Service и создается новая запись об оплате. После этого выполняется обращение в сервис Loyalty Service, увеличивается счетчик бронирований. По-умолчанию у клиента статус BRONZE, статус SILVER присваивается после 10 бронирований, GOLD после 20.

POST {{baesUrl}}/api/v1/reservations
Content-Type: application/json
X-User-Name: {{username}}

{
  "hotelUid": "049161bb-badd-4fa8-9d90-87c9a82b0668",
  "startDate": "2021-10-08",
  "endDate": "2021-10-11"
}

6. Отменить бронирование
Статус бронирования помечается как CANCELED.
В Payment Service запись об оплате помечается отмененной (статус CANCELED).
Loyalty Service уменьшается счетчик бронирований. Так же возможно понижение статуса лояльности, если счетчик стал ниже границы уровня.
DELETE {{baesUrl}}/api/v1/reservations/{{reservationUid}}
X-User-Name: {{username}}

7. Получить информацию о статусе в программе лояльности
GET {{baesUrl}}/api/v1/loyalty
X-User-Name: {{username}}
"""
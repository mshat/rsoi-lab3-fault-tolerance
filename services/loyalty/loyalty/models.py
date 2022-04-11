from django.db import models


class Loyalty(models.Model):
    username = models.CharField(max_length=80)
    reservationCount = models.IntegerField(default=0)
    STATUS_CHOICES = (
        ('B', 'BRONZE'),
        ('S', 'SILVER'),
        ('G', 'GOLD'),
    )
    status = models.CharField(max_length=2, choices=STATUS_CHOICES)
    discount = models.IntegerField()

    def __str__(self):
        return str(self.username)

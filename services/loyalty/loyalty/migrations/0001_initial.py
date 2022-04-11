# Generated by Django 3.2.7 on 2022-01-02 13:37

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Loyalty',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('username', models.CharField(max_length=80)),
                ('reservationCount', models.IntegerField(default=0)),
                ('status', models.CharField(choices=[('B', 'BRONZE'), ('S', 'SILVER'), ('G', 'GOLD')], max_length=2)),
                ('discount', models.IntegerField()),
            ],
        ),
    ]
# Generated by Django 3.2.25 on 2024-07-09 03:01

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0344_merge_0339_auto_20240708_1344_0343_auto_20240708_1509'),
    ]

    operations = [
        migrations.AddField(
            model_name='occurrencereportgeometry',
            name='show_on_occ_map',
            field=models.BooleanField(default=False),
        ),
    ]
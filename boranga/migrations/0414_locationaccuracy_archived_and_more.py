# Generated by Django 5.0.8 on 2024-08-08 08:29

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0413_observationmethod_archived_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="locationaccuracy",
            name="archived",
            field=models.BooleanField(default=False),
        ),
    ]
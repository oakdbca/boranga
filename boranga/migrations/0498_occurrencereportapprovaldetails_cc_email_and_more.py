# Generated by Django 5.0.9 on 2024-11-15 03:24

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0497_alter_commonwealthconservationlist_options_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="occurrencereportapprovaldetails",
            name="cc_email",
            field=models.TextField(null=True),
        ),
    ]

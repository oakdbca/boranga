# Generated by Django 3.2.25 on 2024-06-05 05:52

from django.db import migrations
import multiselectfield.db.fields


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0278_auto_20240604_1340'),
    ]

    operations = [
        migrations.AlterField(
            model_name='species',
            name='regions',
            field=multiselectfield.db.fields.MultiSelectField(blank=True, choices=[(1, 'Goldfields'), (2, 'Kimberley'), (3, 'Midwest'), (4, 'Pilbara'), (5, 'South Coast'), (6, 'South West'), (7, 'Swan'), (8, 'Warren'), (9, 'Wheatbelt')], max_length=250, null=True),
        ),
    ]

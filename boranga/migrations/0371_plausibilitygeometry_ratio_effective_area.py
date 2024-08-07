# Generated by Django 3.2.25 on 2024-08-02 07:25

import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0370_plausibilitygeometry_check_for_geometry'),
    ]

    operations = [
        migrations.AddField(
            model_name='plausibilitygeometry',
            name='ratio_effective_area',
            field=models.FloatField(default=1.0, validators=[django.core.validators.MinValueValidator(0.0), django.core.validators.MaxValueValidator(1.0)]),
        ),
    ]

# Generated by Django 3.2.25 on 2024-05-30 01:23

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0269_auto_20240529_1656"),
    ]

    operations = [
        migrations.AddField(
            model_name="conservationstatus",
            name="submitter_information",
            field=models.OneToOneField(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="conservation_status",
                to="boranga.submitterinformation",
            ),
        ),
    ]

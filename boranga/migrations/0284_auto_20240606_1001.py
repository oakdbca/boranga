# Generated by Django 3.2.25 on 2024-06-06 02:01

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0283_merge_20240606_0911"),
    ]

    operations = [
        migrations.AddField(
            model_name="conservationstatusdocument",
            name="can_submitter_access",
            field=models.BooleanField(default=True),
        ),
    ]
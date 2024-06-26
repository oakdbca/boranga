# Generated by Django 3.2.25 on 2024-05-29 08:54

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("boranga", "0267_merge_20240529_1522"),
    ]

    operations = [
        migrations.RenameModel(
            old_name="UserCategory",
            new_name="SubmitterCategory",
        ),
        migrations.RenameModel(
            old_name="Profile",
            new_name="SubmitterInformation",
        ),
        migrations.AlterModelOptions(
            name="submittercategory",
            options={"verbose_name_plural": "Submitter Categories"},
        ),
        migrations.RenameField(
            model_name="submitterinformation",
            old_name="user_category",
            new_name="submitter_category",
        ),
    ]

# Generated by Django 5.0.9 on 2024-09-17 08:14

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        (
            "boranga",
            "0461_delete_globalsettings_alter_communitydocument__file_and_more",
        ),
    ]

    operations = [
        migrations.RemoveField(
            model_name="conservationstatus",
            name="recommended_commonwealth_conservation_list",
        ),
        migrations.RemoveField(
            model_name="conservationstatus",
            name="recommended_conservation_criteria",
        ),
        migrations.RemoveField(
            model_name="conservationstatus",
            name="recommended_international_conservation",
        ),
        migrations.RemoveField(
            model_name="conservationstatus",
            name="recommended_wa_legislative_category",
        ),
        migrations.RemoveField(
            model_name="conservationstatus",
            name="recommended_wa_legislative_list",
        ),
        migrations.RemoveField(
            model_name="conservationstatus",
            name="recommended_wa_priority_category",
        ),
        migrations.RemoveField(
            model_name="conservationstatus",
            name="recommended_wa_priority_list",
        ),
    ]
# Generated by Django 3.2.25 on 2024-06-19 01:04

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('boranga', '0316_merge_20240619_0836'),
    ]

    operations = [
        migrations.RenameField(
            model_name='occvegetationstructure',
            old_name='free_text_field_four',
            new_name='vegetation_structure_layer_four',
        ),
        migrations.RenameField(
            model_name='occvegetationstructure',
            old_name='free_text_field_one',
            new_name='vegetation_structure_layer_one',
        ),
        migrations.RenameField(
            model_name='occvegetationstructure',
            old_name='free_text_field_three',
            new_name='vegetation_structure_layer_three',
        ),
        migrations.RenameField(
            model_name='occvegetationstructure',
            old_name='free_text_field_two',
            new_name='vegetation_structure_layer_two',
        ),
        migrations.RenameField(
            model_name='ocrvegetationstructure',
            old_name='free_text_field_four',
            new_name='vegetation_structure_layer_four',
        ),
        migrations.RenameField(
            model_name='ocrvegetationstructure',
            old_name='free_text_field_one',
            new_name='vegetation_structure_layer_one',
        ),
        migrations.RenameField(
            model_name='ocrvegetationstructure',
            old_name='free_text_field_three',
            new_name='vegetation_structure_layer_three',
        ),
        migrations.RenameField(
            model_name='ocrvegetationstructure',
            old_name='free_text_field_two',
            new_name='vegetation_structure_layer_two',
        ),
    ]
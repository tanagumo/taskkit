from django.apps.config import AppConfig


class TaskkitConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'taskkit.contrib.django'
    label = 'taskkit'

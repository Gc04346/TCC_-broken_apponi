from django.contrib.auth.models import User
from django.core.management.base import BaseCommand
from django.urls import reverse

from music_system.apps.clients_and_profiles.models.notifications import SystemNotification, notify_users


class Command(BaseCommand):
    help = 'Temp'

    def handle(self, **other):
        notification_code = SystemNotification.get_system_updated_code()
        recipients = User.objects.filter(
            user_user_profile__profilesystemnotification__notification__code=notification_code)
        notify_users(notification_code, recipients, url=reverse('dashboard:changelog'))

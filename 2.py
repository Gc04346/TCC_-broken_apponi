from django.urls import reverse

from celery import shared_task
from django.contrib.auth.models import User

from music_system.apps.clients_and_profiles.models.notifications import SystemNotification, notify_users


@shared_task
def campaign_importation_status_notification_sender(success, campaign_id):
    """Envia notificações sobre produtos que terminaram de ser gerados por label"""
    notification_code = SystemNotification.get_campaign_importation_status_code()
    recipients = User.objects.filter(
        user_user_profile__profilesystemnotification__notification__code=notification_code)
    extra_info = 'com sucesso.' if success else 'com erro.'
    notify_users(notification_code, recipients, extra_info=extra_info, url=f'/ads/campaigns/{campaign_id}')

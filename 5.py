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
        try:
            notification = SystemNotification.objects.get(code=notification_code)
        except SystemNotification.DoesNotExist as e:
            print(e)
        verb = notification.verb + f' {extra_info}' if extra_info else notification.verb
        try:
            email_url = '{}{}'.format('SITE_URL', url)
            if len(recipients) > 0:
                email_logo = recipients[0].user_user_profile.get_master_client_email_logo_url()
                try:
                    email_master_client_name = recipients[0].user_user_profile.get_master_client().name
                except AttributeError:
                    email_master_client_name = 'FRONT_END__SITE_NAME'
                if author is None:
                    author = recipients[0].user_user_profile.get_default_system_master_client()
                    # No caso extremo de não haver um master client no sistema, colocamos um autor qualquer
                    if not author:
                        print('Não há um master client no sistema. Favor corrigir.')
                        author = recipients[0]
                email_description = f'{author} - {verb}: {action_object}' if action_object else f'{author} - {verb}'
                # bell notification
                notify.send(sender=author, recipient=recipients, verb=verb, action_object=action_object, url=url,
                            emailed=True, level=level)
                # todo quando o ator da notificação for um usuário, colocar o nome dele como ator pra melhorar a legibilidade

                # email notification management
                email_support = 'Any questions? Email us!'
                email_support_mail = 'SUPPORT_MAIL'
                email_site_name = 'FRONT_END__SITE_NAME'
                context = {
                    'url': email_url,
                    'email_title': email_site_name,
                    'email_subject': f'{email_site_name} - {notification.description}',
                    'email_description': email_description,
                    'email_button_text': 'Go',
                    'email_support': email_support,
                    'email_support_mail': email_support_mail,
                    'email_site_name': email_site_name,
                    'publisher_logo_path': email_site_name,
                    'email_logo': email_logo,
                    'email_master_client_name': email_master_client_name,
                }
                email_recipients = []
                for recipient in recipients:
                    email_recipients.append(recipient.email)

                    if recipient.email is not None and recipient.email != '':
                        email_recipients.append(recipient.email)
                try:
                    mail.send(
                        email_recipients,
                        # subject=email_subject,
                        template=level or notification.level,
                        context=context,
                    )
                except ValidationError as e:
                    log_error(f'Erro ao enviar email de notificação: {e}\n')
            else:
                print(f'A notificação: "{verb}" não possui recipientes, e por isso não foi enviada.')

        except Exception as e:
            print(e)

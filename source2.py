from django.core.exceptions import ValidationError
from django.utils.translation import gettext_lazy as _
from notifications.signals import notify
from post_office import mail

from music_system.apps.contrib.log_helper import log_error
from music_system.settings.base import FRONT_END__SITE_NAME
from music_system.settings.local import SITE_URL


def send_notification(author, recipients, verb, action_object, url, send_email=True, email_template='info',
                      level='info', email_subject=_('New notification') + ' - ' + FRONT_END__SITE_NAME,
                      email_title=FRONT_END__SITE_NAME, email_description='', email_button_text=_('Go')) -> bool:
    """
    Interface para acesso ao envio de email por filas.
    """
    try:
        email_url = '{}{}'.format(SITE_URL, url)
        if len(recipients) > 0:
            email_logo = recipients[0].user_user_profile.get_master_client_email_logo_url()
            try:
                email_master_client_name = recipients[0].user_user_profile.get_master_client().name
            except AttributeError:
                email_master_client_name = FRONT_END__SITE_NAME
            if author is None:
                author = recipients[0].user_user_profile.get_default_system_master_client()
                # No caso extremo de não haver um master client no sistema, colocamos um autor qualquer
                if not author:
                    log_error('Não há um master client no sistema. Favor corrigir.')
                    author = recipients[0]
            if email_description == '':
                email_description = f'{author} - {verb}: {action_object}' if action_object else f'{author} - {verb}'
                # bell notification
                notify.send(sender=author, recipient=recipients, verb=verb, action_object=action_object, url=url,
                            emailed=send_email, level=level)
                # todo quando o ator da notificação for um usuário, colocar o nome dele como ator pra melhorar a legibilidade

                # email notification management
                if send_email:
                    email_support = _('Any questions? Email us!')
                    email_support_mail = 'SUPPORT_MAIL'
                    email_site_name = FRONT_END__SITE_NAME
                    context = {
                        'url': email_url,
                        'email_title': email_title,
                        'email_subject': email_subject,
                        'email_description': email_description,
                        'email_button_text': email_button_text,
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
                            template=email_template,
                            context=context,
                        )
                    except ValidationError as e:
                        log_error(f'Erro ao enviar email de notificação: {e}\n')
        else:
            # log_error(f'A notificação: "{verb}" não possui recipientes, e por isso não foi enviada.')
            return False

    except Exception as e:
        log_error(e)
        return False

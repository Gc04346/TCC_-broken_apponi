from django.core.exceptions import ValidationError
from django.db import transaction
from notifications.signals import notify
from post_office import mail
from django.utils.translation import gettext_lazy as _

from music_system.apps.contrib.log_helper import log_error, log_notification, log_tests
from music_system.apps.notifications_helper.onitificator_telegram import send_message
from music_system.settings.base import FRONT_END__SITE_NAME, SUPPORT_MAIL
from music_system.settings.local import SEND_TELEGRAM_NOTIFICATIONS, FINANCEIRO_TELEGRAM_CHAT_ID, \
    CONTEUDO_TELEGRAM_CHAT_ID, COMUNICACAO_TELEGRAM_CHAT_ID, ATENDIEMENTO_TELEGRAM_CHAT_ID, DEV_TELEGRAM_CHAT_ID, \
    LIDER_ATENDIEMENTO_TELEGRAM_CHAT_ID, TELEGRAM_ONITIFICATOR_BOT_TOKEN


def notify_on_telegram(chat_id, text, bot_token=TELEGRAM_ONITIFICATOR_BOT_TOKEN):
    """
    Chama o Oni Services pra fazer envio de notificações pelo Telegram. Encoda o texto passado como param.
    Chat_ids disponíveis:
        financeiro - setor Financeiro
        comunicacao - setor Comunicação
        conteudo - setor Conteúdo
        atendimento - setor Atendimento
        dev - setor Desenvolvimento
    Args:
        chat_id: um de: ['atendimento','comunicacao','conteudo','financeiro'].
        text: texto que será enviado como mensagem. passar como parâmetro o texto NÃO ENCODADO.
        bot_token: token do bot que irá disparar a msg. padrão: ONItificador
    """
    chat_ids = {
        'lider_atendimento': LIDER_ATENDIEMENTO_TELEGRAM_CHAT_ID,
        'atendimento': ATENDIEMENTO_TELEGRAM_CHAT_ID,
        'comunicacao': COMUNICACAO_TELEGRAM_CHAT_ID,
        'conteudo': CONTEUDO_TELEGRAM_CHAT_ID,
        'financeiro': FINANCEIRO_TELEGRAM_CHAT_ID,
        'dev': DEV_TELEGRAM_CHAT_ID,
    }
    if not SEND_TELEGRAM_NOTIFICATIONS:
        log_notification('nenhuma notificação foi enviada porque SEND_TELEGRAM_NOTIFICATIONS está definida como False.')
        return
    import requests
    import urllib.parse
    data = {
        'bot_token': bot_token,
        'chat_id': chat_ids.get(chat_id),
        'text': urllib.parse.quote(text)
    }
    try:
        response = send_message(**data)
        log_notification(response.json())
    except Exception as e:
        log_error(e)


@transaction.atomic
def process_notification(author, recipients, verb, action_object, url,
                         send_email,
                         email_template,
                         email_subject,
                         email_title,
                         email_description,
                         email_button_text,
                         email_url,
                         email_logo,
                         email_master_client_name, level) -> bool:
    """Default system notification sender
    Sends bell notifications and email notifications, if specified. The atomic transaction decorator
    makes sure that email notifications are sent along with the bell ones. After an email is sent, the
    'emailed' field on the notification model should be set to True.
    Args:
        author: obj
        recipients: QuerySet
        verb: str
        action_object: obj
        url: str
        send_email: bool
        email_template: str(EmailTemplate object name)
        email_title
        email_subject
        email_description
        email_button_text
        email_url
        email_logo
        email_master_client_name
        level
    """

    # bell notification
    notify.send(sender=author, recipient=recipients, verb=verb, action_object=action_object, url=url,
                emailed=send_email, level=level)
    # todo quando o ator da notificação for um usuário, colocar o nome dele como ator pra melhorar a legibilidade

    # email notification management
    if send_email:
        context = email_context_builder(email_url, email_title, email_subject, email_description, email_button_text,
                                        email_logo, email_master_client_name)
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
    return True


def email_context_builder(email_url, email_title, email_subject, email_description, email_button_text, email_logo,
                          email_master_client_name):
    email_support = _('Any questions? Email us!')
    email_support_mail = SUPPORT_MAIL
    email_site_name = FRONT_END__SITE_NAME
    return {
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

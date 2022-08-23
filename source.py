from typing import Any

from auditlog.registry import auditlog
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.db.models import QuerySet
from django.utils.translation import gettext_lazy as _
from django.db import models
from notifications.models import Notification
from notifications.signals import notify
from post_office import mail

from music_system.apps.contrib.log_helper import log_error

NOTIFICATION_CODES = (
    ('new_task', _('New Task')),
    ('seven_days_late_task', _('Late Task')),
    ('task_commented', _('Task Comment')),
    ('task_edited', _('Task Edited')),
    ('task_status_updated', _('Task Status Updated')),
    ('coworker_birthday', _('Coworker Birthday')),
    ('new_associated_entry', _('New Associated Entry')),
    ('new_big_product_entry', _('Big Product Entry')),
    ('new_common_product_entry', _('Common Product Entry')),
    ('system_updated', _('System Update')),
    ('holder_contract_about_to_expire', _('Holder Contract Expiration Date')),
    ('worker_company_anniversary', _('Worker Company Anniversary')),
    ('product_generated', _('Product Generation by Label Finished')),
    ('campaign_importation_status', _('Campaign Importation Status')),
    ('product_alteration', _('Product Alteration')),
)


class SystemNotification(models.Model):
    code = models.CharField(verbose_name=_('ID Code'), max_length=40, choices=NOTIFICATION_CODES)
    description = models.CharField(verbose_name=_('Description'), max_length=100,
                                   help_text=_('What the user will be notified about'))
    verb = models.CharField(verbose_name=_('Verb'), max_length=50,
                            help_text=_('Text that will show up in the notification'))
    level = models.CharField(verbose_name=_('Level'), max_length=10, choices=Notification.LEVELS,
                             default='info')

    class Meta:
        verbose_name = _('System Notification')
        verbose_name_plural = _('System Notifications')

    def __str__(self):
        return self.description

    @staticmethod
    def get_new_task_code() -> str:
        """Retorna o código da notificação"""
        return 'new_task'

    @staticmethod
    def get_seven_days_late_task_code() -> str:
        """Retorna o código da notificação"""
        return 'seven_days_late_task'

    @staticmethod
    def get_task_commented_code() -> str:
        """Retorna o código da notificação"""
        return 'task_commented'

    @staticmethod
    def get_task_edited_code() -> str:
        """Retorna o código da notificação"""
        return 'task_edited'

    @staticmethod
    def get_task_status_updated_code() -> str:
        """Retorna o código da notificação"""
        return 'task_status_updated'

    @staticmethod
    def get_coworker_birthday_code() -> str:
        """Retorna o código da notificação"""
        return 'coworker_birthday'

    @staticmethod
    def get_new_associated_entry_code() -> str:
        """Retorna o código da notificação"""
        return 'new_associated_entry'

    @staticmethod
    def get_new_big_product_entry_code() -> str:
        """Retorna o código da notificação"""
        return 'new_big_product_entry'

    @staticmethod
    def get_new_common_product_entry_code() -> str:
        """Retorna o código da notificação"""
        return 'new_common_product_entry'

    @staticmethod
    def get_system_updated_code() -> str:
        """Retorna o código da notificação"""
        return 'system_updated'

    @staticmethod
    def get_holder_contract_about_to_expire_code() -> str:
        """Retorna o código da notificação"""
        return 'holder_contract_about_to_expire'

    @staticmethod
    def get_worker_company_anniversary_code() -> str:
        """Retorna o código da notificação"""
        return 'worker_company_anniversary'

    @staticmethod
    def get_product_generated_code() -> str:
        """Retorna o código da notificação"""
        return 'product_generated'

    @staticmethod
    def get_campaign_importation_status_code() -> str:
        """Retorna o código da notificação"""
        return 'campaign_importation_status'

    @staticmethod
    def get_product_alteration_code() -> str:
        """Retorna o código da notificação"""
        return 'product_alteration'


def get_recipients_of_notification(notification: SystemNotification) -> QuerySet(User):
    """
    Retorna todos os recipientes daquela notificação, ou seja, todos os usuários cujo perfil tem uma entrada na tabela
        ProfileSystemNotification cuja notificação corresponde à passada por parâmetro.
    Args:
        notification: Objeto SystemNotification da qual se querem os recipientes

    Returns: QuerySet de User
    """
    return User.objects.filter(user_user_profile__profilesystemnotification__notification=notification)


def notify_users(notification_code: str, recipients: QuerySet(User), action_object: Any = None, author: Any = None,
                 url: str = 'javascript:void(0)', extra_info: str = None, level: str = None) -> bool:
    """
    Pega a notificação com base no código passado e os recipientes dela, e chama o método que dispara as notificações no
     sistema
    Args:
        author: objeto que executou a ação que disparou a notificação
        action_object: objeto que sofreu a modificação
        recipients: queryset de usuários que receberão a notificação
        url: url de destino
        notification_code: código da notificação a ser disparada
        extra_info: string opcional que complementa o verbo da notificação. Ex: uma data (o contrato vencerá em dd/mm)
        level: string opcional que indica a importância da notificação. Se não for fornecida, será usado o level do ob-
                jeto SystemNotification

    Returns: True se correr tudo bem, False caso contrário

    """
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


auditlog.register(SystemNotification)

from __future__ import absolute_import, unicode_literals
from celery import shared_task
from django.db.models import Q
from django.urls import reverse
from django.utils import timezone
from notifications.models import Notification

from .models import Task
from ..clients_and_profiles.models.notifications import SystemNotification, notify_users


@shared_task
def notify_about_late_tasks():
    notification_code = SystemNotification.get_seven_days_late_task_code()

    today = timezone.now().date()

    # Tarefas atrasadas que ja foram notificadas nao sao notificadas novamente. A query para descobrir se uma tarefa
    #  ja foi notificada verifica todas as notificações com nível warning (usado pelas notificações de tarefa atra-
    #  sada) disparadas nos últimos sete dias, cujo action object nao eh nulo (pois estas notificações sempre passam
    #  o action object). Pegamos então a lista de ids de tarefas que ja foram notificadas e fazemos o cast para int,
    #  pois por default, o values_list retorna esses ids como string. precisamos destes valores como int para usar
    #  no exclude da query de tarefas atrasadas.
    tasks_already_notified = list(map(int,
                                      Notification.objects.filter(timestamp__gte=today - timezone.timedelta(days=7),
                                                                  level='warning',
                                                                  action_object_object_id__isnull=False).distinct().values_list(
                                          'action_object_object_id', flat=True)))

    # Pega as tarefas atrasadas deste mes, com a exceção das que ja foram notificadas. Tarefas atrasadas são as que (não
    # estão arquivadas e passaram do prazo) E (cujo status não é terminada OU não tem data de conclusão)
    late_tasks = Task.objects.filter(
        Q(archived=False, due_date__lt=today - timezone.timedelta(days=7), due_date__month=today.month) & Q(
            Q(status__in=Task.get_all_statuses_code_except_finished()) | Q(
                Q(done_date__isnull=True) & ~Q(status__in=[Task.get_canceled_status_code()])))).exclude(
        id__in=tasks_already_notified)
    notification_amount = 0
    for task in late_tasks:
        recipients = task.get_relevant_task_notification_recipients(
            Q(user_user_profile__profilesystemnotification__notification__code=notification_code))
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
        notification_amount += recipients.count()
    return notification_amount

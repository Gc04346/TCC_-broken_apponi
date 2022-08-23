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
        notify_users(notification_code, recipients, action_object=task,
                     url=reverse('dashboard:tasks.schedule', args=[task.id]))
        notification_amount += recipients.count()
    return notification_amount

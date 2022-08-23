from __future__ import absolute_import, unicode_literals

from datetime import timedelta

from celery import shared_task, Celery, chord
import celery
from abc import ABC
from django.contrib.auth.models import User
from django.db.models import Q
from django.urls import reverse
from django.utils import timezone
from notifications.models import Notification

from .models import Holder, LabelProduct, Product
from .models.bulk import YoutubeAssetBulk
from ..clients_and_profiles.models.notifications import SystemNotification, notify_users
from ..contrib.log_helper import log_error, log_tests
from ...settings.local import USE_S3


class BaseLabelTaskClass(celery.Task, ABC):
    def on_success(self, retval, task_id, args, kwargs) -> None:
        label_id: int = args[0]
        label = LabelProduct.objects.get(id=label_id)
        label.product_generation_status = 'suc'
        label.save()

    def on_failure(self, exc, task_id, args, kwargs, einfo) -> None:
        label_id: int = args[0]
        label = LabelProduct.objects.get(id=label_id)
        label.product_generation_status = 'fai'
        label.save()


@shared_task
def validate_youtube_asset_bulk(item_id: int):
    """Validate youtube asset bulks """
    bulk = YoutubeAssetBulk.objects.get(id=item_id)
    bulk.validate_file()


@shared_task
def process_youtube_asset_bulk(item_id: int):
    """Process youtube asset bulks """
    bulk = YoutubeAssetBulk.objects.get(id=item_id)
    bulk.process_file()


@shared_task
def clean_files_youtube_asset_bulk():
    """Process youtube asset bulks """
    bulks = YoutubeAssetBulk.objects.filter(file__isnull=False,
                                            created_at__lte=timezone.now() + timezone.timedelta(days=60)).exclude(
        file='')
    from django.core.files.storage import default_storage
    storage = default_storage
    for bulk in bulks:
        if bulk.should_delete_file:
            storage.delete(bulk.file.name)
            bulk.file = ''
            bulk.save()


@shared_task
def product_generated_notification_sender(label_id):
    """Envia notificações sobre produtos que terminaram de ser gerados por label"""
    notification_code = SystemNotification.get_product_generated_code()
    recipients = User.objects.filter(
        user_user_profile__profilesystemnotification__notification__code=notification_code)

    label = LabelProduct.objects.get(id=label_id)
    if label.product_generation_status == 'suc':
        product = label.product
        notify_users(notification_code, recipients, action_object=product, extra_info='com sucesso.',
                     url=f"{reverse('label_catalog:product.list')}{product.id}")
    else:
        notify_users(notification_code, recipients, extra_info='com erro.', url=label.get_admin_url())


@shared_task(base=BaseLabelTaskClass)
def label_make_product(label_id):
    label = LabelProduct.objects.get(id=label_id)
    LabelProduct.make_product(label)


@shared_task
def generate_product_from_label(label_id):
    # callback, tarefa que será executada após o processamento dos relatórios
    callback = product_generated_notification_sender.si(label_id)
    # criando o grupo de tarefas. TEM QUE SER EM FORMATO DE LISTA
    task = [label_make_product.s(label_id)]
    # preparando a chord (tarefa que só irá executar ao final de todas as tarefas do grupo)
    chord(task)(callback)


@shared_task
def check_coworker_birthdays():
    """Envia notificações sobre aniversários de colaboradores"""
    now_day = timezone.now().day
    now_month = timezone.now().month
    birthday_coworkers = User.objects.filter(
        Q(Q(is_staff=True) | Q(is_superuser=True)) & Q(user_user_profile__birthday__day=now_day,
                                                       user_user_profile__birthday__month=now_month))
    if birthday_coworkers.exists():
        for birthday_coworker in birthday_coworkers:
            notification_code = SystemNotification.get_coworker_birthday_code()
            recipients = User.objects.filter(
                user_user_profile__profilesystemnotification__notification__code=notification_code)
            notify_users(notification_code, recipients, action_object=birthday_coworker)


@shared_task
def check_worker_company_anniversaries():
    """Envia notificações sobre aniversários de colaboradores"""
    now_day = timezone.now().day
    now_month = timezone.now().month
    company_anniversary_coworkers = User.objects.filter(
        Q(Q(is_staff=True) | Q(is_superuser=True)) & Q(user_user_profile__company_anniversary__day=now_day,
                                                       user_user_profile__company_anniversary__month=now_month))
    if company_anniversary_coworkers.exists():
        for company_anniversary_coworker in company_anniversary_coworkers:
            notification_code = SystemNotification.get_worker_company_anniversary_code()
            recipients = User.objects.filter(
                user_user_profile__profilesystemnotification__notification__code=notification_code)
            notify_users(notification_code, recipients, action_object=company_anniversary_coworker)


@shared_task
def get_holder_contracts_near_expiration():
    notification_code = SystemNotification.get_holder_contract_about_to_expire_code()
    now = timezone.now()
    seven_days_ahead = now + timezone.timedelta(days=7)
    fourteen_days_ahead = now + timezone.timedelta(days=14)
    twenty_one_days_ahead = now + timezone.timedelta(days=21)
    thirty_days_ahead = now + timezone.timedelta(days=30)
    holders_with_contract_near_expiration = Holder.objects.filter(Q(contract_end__isnull=False) & Q(
        Q(contract_end=seven_days_ahead) | Q(contract_end=fourteen_days_ahead) | Q(
            contract_end=twenty_one_days_ahead) | Q(contract_end=thirty_days_ahead)))
    recipients = User.objects.filter(
        user_user_profile__profilesystemnotification__notification__code=notification_code)
    for holder in holders_with_contract_near_expiration:
        notify_users(notification_code, recipients, url=holder.get_admin_url(), author=holder,
                     extra_info=holder.contract_end.strftime("%d/%m/%Y"))


@shared_task
def clean_unread_notifications():
    """Limpa as notificações naõ lidas mais velhas do que 7 dias"""
    Notification.objects.filter(unread=True, timestamp__lte=timezone.now() - timezone.timedelta(days=7)).delete()


@shared_task
def send_product_to_fuga_ftp(product_id):
    """Envia arquivos para tp do fuga"""
    try:
        product = Product.objects.get(id=product_id)
        product.upload_fuga_miss_files()
    except Product.DoesNotExist:
        log_error(
            f'Erro ao tentar fazer upload de produto para o FUGA MISS. Produto com o id {product_id} não encontrado.')


@shared_task
def notify_about_labels_without_project():
    from music_system.apps.notifications_helper.notification_helpers import notify_on_telegram
    projectless_labels = LabelProduct.objects.filter(project_model__isnull=True)
    today = timezone.now().date()
    exclamation = bytes.decode(b'\xE2\x9D\x97', 'utf8')
    double_exclamation = bytes.decode(b'\xE2\x80\xBC', 'utf8')
    for label in projectless_labels:
        if label.created_at.date() == today - timedelta(days=4):
            notify_on_telegram('lider_atendimento',
                               f'{double_exclamation}A label **{label}** está há 4 ou mais dias cadastrada sem ter sido atribuída a um projeto.{double_exclamation}')
        elif label.created_at.date() == today - timedelta(days=2):
            notify_on_telegram('lider_atendimento',
                               f'A label **{label}** está há 2 ou mais dias cadastrada sem ter sido atribuída a um projeto.{exclamation}')


@shared_task
def check_for_similar_products_within_the_release_week(product_id: int):
    """ Tarefa para conferir se há algum produto com nome parecido ao passado como parâmetro programado pra ser lançado
        na mesma semana.
        Args:
            product_id: id do produto que acabou de ser criado
        Returns:
            None
    """
    from fuzzysearch import find_near_matches
    from music_system.apps.notifications_helper.notification_helpers import notify_on_telegram
    try:
        product = Product.objects.get(id=product_id)
        if not product.date_release:
            raise Product.DoesNotExist
        title = product.title
    except Product.DoesNotExist:
        log_error(f'Produto com id {product_id} não encontrado ou não possui data de lançamento.')
        return
    products_within_the_week = Product.objects.filter(
        date_release__gte=product.date_release - timezone.timedelta(days=4),
        date_release__lte=product.date_release + timezone.timedelta(days=4)).exclude(id=product.id)
    for potential_similar_product in products_within_the_week:
        if find_near_matches(title, potential_similar_product.title, max_l_dist=3):
            # Se achar algum, notifica e encerra a busca
            notify_on_telegram('atendimento',
                               f'Há produtos com título similar programados para lançamento próximo um do outro. Favor conferir.\n-> Título do produto: {title}\n->Data lançamento: {product.date_release.strftime("%d/%m/%Y")}')
            break


@shared_task
def check_for_release_date_on_holidays(product_id: int):
    """ Confere se o produto está programado pra ser lançado em fim de semana ou feriado
        Args:
            product_id: id do produto que acabou de ser criado
        Returns:
            None
    """
    from holidays import country_holidays
    from music_system.apps.notifications_helper.notification_helpers import notify_on_telegram
    try:
        product = Product.objects.get(id=product_id)
        release_date = product.date_release
        if not release_date:
            raise Product.DoesNotExist
    except Product.DoesNotExist:
        return
    holidays = country_holidays('BR', subdiv='MG')
    if release_date.strftime("%Y-%m-%d") in holidays:
        notify_on_telegram('atendimento',
                           f'O produto **{product} - {product.main_holder}** está programado para ser lançado em feriado ({release_date.strftime("%d/%m/%Y")}). Favor conferir.')
    elif release_date.weekday() > 4:
        notify_on_telegram('atendimento',
                           f'O produto **{product} - {product.main_holder}** está programado para ser lançado em um fim de semana. Favor conferir.')

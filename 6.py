import ftplib
from datetime import datetime
from typing import List, Any

from celery import shared_task
from django.conf import settings
from django.contrib.auth.models import User
from django.templatetags.static import static
from django.core.exceptions import ObjectDoesNotExist
from django.core.validators import RegexValidator
from django.db import models
from django.db.models import Q, QuerySet, Count
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from django.urls import reverse
from django.utils import timezone
from django.utils.html import format_html
from django.utils.translation import gettext_lazy as _
from docutils.parsers import null

from auditlog.registry import auditlog
from rest_framework.permissions import BasePermission

from music_system.apps.contrib.image_helpers import make_thumbnail_and_set_for_model
from music_system.apps.contrib.models.admin_helpers import GetAdminUrl
from music_system.apps.contrib.string_helpers import return_mark_safe
from music_system.apps.tasks.models.base import Project, ProjectModel, Task
from .base import Artist, Holder
from music_system.apps.contrib.models.base_model import BaseModel, BaseApiDataClass, DSPSIdFieldsModel, get_file_path, \
    ModelDiffMixin
from ..helpers import get_audio_track_for_humans_from_filefield, get_thumb_with_image_download_url, \
    helper_get_artists_names, helper_get_holders_names, default_query_assets_by_args, default_get_youtube_embedded, \
    helper_get_composers_names, write_csv, write_xlsx, DEFAULT_NO_GRAVATAR_IMAGE
from ...clients_and_profiles.models.notifications import SystemNotification, notify_users
from ...clients_and_profiles.models.base import get_gravatar, Profile
from ...contrib.log_helper import log_error, log_tests
from music_system.apps.notifications_helper.notification_helpers import notify_on_telegram
from ...contrib.models.object_filterer import ObjectFilterer
from ...contrib.validators import validate_image_format, validate_audio_format, \
    validate_file_max_15000, validate_file_max_300000, validate_file_max_10000
from ...contrib.views.base import get_user_profile_from_request

PRODUCT_LABEL_STATUS = (('PEN', _('Pending')), ('COM', _('Complete')))
PRODUCT_VERSIONS = [
    ('NON', ''),
    ('LIV', _('Live')),
    ('ACO', _('Acoustic')),
    ('PLB', _('Playback')),
]
PRODUCT_MEDIAS = (('AUVD', _('Audio and Video')), ('AUDI', _('Audio Only')), ('VIDE', _('Video Only')),)
PRODUCT_FORMATS = (('ALB', _('Album')), ('EP', _('EP')), ('SIN', _('Single')),)
ASSET_PUBLISHING_STATUS = (
    ('PEN', _('Pending')), ('APP', _('Approved')), ('APP', _('Approved')), ('DEC', _('Declined')),)
YOUTUBE_ASSET_TYPES = (
    ('MV', _('Music Video')), ('SR', _('Sound Recording')), ('WEB', _('Web')), ('AT', _('Art Track')),
    ('MO', _('Movie')), ('TV', _('TV Episode')))

PRODUCT_COVER_PATH = 'products/covers'
ASSET_AUDIO_PATH = 'products/assets/tracks'
ASSET_COVER_PATH = 'products/assets/track_covers'
ASSET_COVER_THUMBS_PATH = 'products/tracks_covers_thumbs'
PRODUCT_COVER_THUMBS_PATH = PRODUCT_COVER_PATH + '_thumbs'
PRODUCT_ORDER_COLUMN_CHOICES = ['upc', 'release_type', 'title', 'primary_artists',
                                'date_release',
                                'date_divulgation']  # lista que corresponde a ordem das colunas no datatables
ASSET_ORDER_COLUMN_CHOICES = ['isrc', 'title', 'media']

LEGACY_ISRC_TYPES = (
    ('AMV', _('Amazon Video')),
    ('FBV', _('Facebook Video')),
    ('TIK', _('TikTok')),
    ('TSR', _('Teaser')),
    ('LEG', _('Legacy')),
)

LEGACY_UPC_TYPES = LEGACY_ISRC_TYPES

RELEASE_TYPES = (
    ('REL', _('RLSE')),
    ('DPC', _('DPRC')),
    ('CMT', _('CHMO')),
    ('CHV', _('CHVI')),
    ('AMP', _('APMP')),
    ('FRM', _('FREM')),
    ('CRG', _('CIRIG')),
    ('SQA', _('SQA')),
    ('PIN', _('PIN')),
)

AUDIO_LANGUAGES = [
    ('PT', _('Portuguese')),
    ('EN', _('English')),
    ('SP', _('Spanish'))
]


def default_get_artists_card_info(primary_artists, featuring_artists) -> List[dict]:
    """Retorna as informacoes dos artistas para serem colocadas nos cards do front
    """
    from music_system.apps.artists.models import HolderUser
    artists_card_info = []
    for primary_artist in primary_artists.all():
        artist_info_dict = {'name': primary_artist.name, 'mode': _('Primary')}
        try:
            artist_info_dict['gravatar'] = get_gravatar(
                HolderUser.objects.filter(holder__name=primary_artist.name).first().user, 155)
        except Exception:
            artist_info_dict['gravatar'] = DEFAULT_NO_GRAVATAR_IMAGE
        artists_card_info.append(artist_info_dict)
    for featuring_artist in featuring_artists.all():
        artist_info_dict = {'name': featuring_artist.name, 'mode': _('Feat.')}
        try:
            artist_info_dict['gravatar'] = get_gravatar(
                HolderUser.objects.filter(holder__name=featuring_artist.name).first().user, 155)
        except Exception:
            artist_info_dict['gravatar'] = DEFAULT_NO_GRAVATAR_IMAGE
        artists_card_info.append(artist_info_dict)
    return artists_card_info


def get_cover_file_path(instance, filename):
    """Define o file_path do arquivo usando um nome aleatorio para o filename, impedindo conflitos de nome igual"""
    return get_file_path(instance, filename, PRODUCT_COVER_PATH)


def get_cover_thumbs_file_path(instance, filename):
    """Define o file_path do arquivo usando um nome aleatorio para o filename, impedindo conflitos de nome igual"""
    return get_file_path(instance, filename, PRODUCT_COVER_THUMBS_PATH)


def get_asset_audio_file_path(instance, filename):
    """Define o file_path do arquivo usando um nome aleatorio para o filename, impedindo conflitos de nome igual"""
    return get_file_path(instance, filename, ASSET_AUDIO_PATH)


def get_asset_cover_file_path(instance, filename):
    """Define o file_path do arquivo usando um nome aleatorio para o filename, impedindo conflitos de nome igual"""
    return get_file_path(instance, filename, ASSET_COVER_PATH)


def get_asset_cover_thumbs_file_path(instance, filename):
    """Define o file_path do arquivo usando um nome aleatorio para o filename, impedindo conflitos de nome igual"""
    return get_file_path(instance, filename, ASSET_COVER_THUMBS_PATH)


def get_sticker_teaser_cover_file_path(instance, filename):
    """Define o file_path do arquivo usando um nome aleatorio para o filename, impedindo conflitos de nome igual"""
    return get_file_path(instance, filename, 'products/covers/stickers')


def get_sticker_teaser_audio_file_path(instance, filename):
    """Define o file_path do arquivo usando um nome aleatorio para o filename, impedindo conflitos de nome igual"""
    return get_file_path(instance, filename, 'products/assets/tracks/stickers')


def get_default_release_type_code() -> str:
    """Retorna o codigo de release type padrao (lancamento)"""
    return 'REL'


def get_mdc_release_type_code() -> str:
    """Retorna o codigo de release type de mdc (movim. de canal)"""
    return 'MDC'


def get_migration_release_type_code() -> str:
    """Retorna o codigo de release type de migracao"""
    return 'MIG'


def get_profile_release_type_code() -> str:
    """Retorna o codigo de release type de perfil"""
    return 'PRF'


def get_all_product_media_codes() -> List[str]:
    """Retorna todos os códigos de midia"""
    media_codes = [media[0] for media in PRODUCT_MEDIAS]
    return media_codes


def get_audio_and_video_product_media_code() -> str:
    """Retorna o codigo da midia "audio e video" """
    return 'AUVD'


def get_audio_only_product_media_code() -> str:
    """Retorna o codigo da midia "apenas audio" """
    return 'AUDI'


def get_video_only_product_media_code() -> str:
    """Retorna o codigo da midia "apenas video" """
    return 'VIDE'


class IsProductOrAssetOwner(BasePermission):
    def has_object_permission(self, request, view, obj):
        try:
            user_profile: Profile = get_user_profile_from_request(request)
            # Se for admin, pode ver se tiver permissão pra ver produtos
            if user_profile.user_is_staff():
                return user_profile.has_permission('label_catalog.view_product')
            # Se não for admin, só pode ver se o objeto pertencer a ele
            else:
                return obj.main_holder.id in user_profile.get_user_owner_holder_ids()
        except Exception:
            return False


class Product(DSPSIdFieldsModel, GetAdminUrl, BaseApiDataClass, ModelDiffMixin):
    """Product is the mother class of this file. It represents"""

    custom_id = models.CharField(verbose_name=_('Custom ID'), help_text=_('Leave blank to use UPC'), max_length=20,
                                 unique=True)
    upc = models.CharField(verbose_name=_('UPC/EAN'), max_length=20, unique=True,
        #                    validators=[
        # RegexValidator(regex='^(?!0)[0-9]{13}$',
        #                message=_('UPC must have 13 digits, not start with zero (0) and is digit-only'))]
                           )
    date_release = models.DateField(verbose_name=_('Release Date'), blank=True, null=True)
    date_divulgation = models.DateField(verbose_name=_('Disclosure Date'
                                                       ''), blank=True, null=True)
    time_release = models.TimeField(verbose_name=_('Release Time'), blank=True, null=True)
    audio_release_time = models.TimeField(verbose_name=_('Audio Release Time'), blank=True, null=True,
                                          help_text=_('Works only at Google Music, Amazon Music, Deezer and Spotify'))
    date_recording = models.DateField(verbose_name=_('Recording Date'), blank=True, null=True)
    date_video_received = models.DateField(verbose_name=_('Video Receival Date'), blank=True, null=True)
    date_audio_received = models.DateField(verbose_name=_('Audio Receival Date'), blank=True, null=True)
    title = models.CharField(verbose_name=_('Title'), max_length=100)
    assets_link = models.TextField(verbose_name=_('Product Files (link)'), max_length=250, blank=True, null=True)
    version = models.CharField(verbose_name=_('Product Version'), max_length=40, blank=True, null=True)
    media = models.CharField(verbose_name=_('Product Media'), max_length=4, choices=PRODUCT_MEDIAS)
    format = models.CharField(verbose_name=_('Product Format'), max_length=4, choices=PRODUCT_FORMATS)
    audio_language = models.CharField(verbose_name=_('Audio Language'), max_length=4, choices=AUDIO_LANGUAGES,
                                      default='PT')
    gender = models.CharField(verbose_name=_('Gender'), max_length=25, blank=True, null=True)
    subgender = models.CharField(verbose_name=_('Subgender'), max_length=25, blank=True, null=True)
    copyright_text_label = models.CharField(verbose_name=_('(c) Label'), max_length=25, blank=True, null=True)
    primary_artists = models.ManyToManyField(verbose_name=_('Primary Artists'), to=Artist, blank=True,
                                             related_name='product_primary')
    featuring_artists = models.ManyToManyField(verbose_name=_('Feat.'), to=Artist, blank=True,
                                               related_name='product_feat')

    ready_for_delivery = models.BooleanField(default=False, verbose_name=_('Is it ready for delivery?'))
    delivery_started = models.BooleanField(verbose_name=_('Delivery started?'), default=False)
    delivery_finished = models.BooleanField(verbose_name=_('Delivery finished?'), default=False)
    delivery_notes = models.TextField(verbose_name=_('Delivery Notes'), blank=True, null=True)

    active = models.BooleanField(verbose_name=_('Is Active'), default=True)
    notes = models.TextField(verbose_name=_('Notes'), blank=True, null=True)
    notes_ads = models.TextField(verbose_name=_('ADs Notes'), blank=True, null=True)

    projects = models.ManyToManyField(verbose_name=_('Product'), to=Project, through='ProductProject')

    cover = models.ImageField(verbose_name=_('Cover'), upload_to=get_cover_file_path,
                              validators=[validate_file_max_15000, validate_image_format],
                              blank=True, null=True,
                              help_text=_(
                                  'Recommended size: 1500x1500 or 3000x3000. You can enter a url for the cover on the Medis URls field instead.'))

    cover_thumbnail = models.ImageField(verbose_name=_('Cover Thumb'), upload_to=PRODUCT_COVER_THUMBS_PATH,
                                        blank=True,
                                        null=True, )

    # preview_start_time = models.TimeField(verbose_name=_('Preview Start Time'), blank=True, null=True)
    main_holder = models.ForeignKey(verbose_name=_('Main Holder'), to=Holder, on_delete=models.PROTECT)
    release_type = models.CharField(verbose_name=_('Release Type'), max_length=5, choices=RELEASE_TYPES,
                                    default=get_default_release_type_code())
    onimusic_network_comm_date = models.DateField(verbose_name=_('Onimusic Network Communication Date'), null=True,
                                                  blank=True)
    fuga_ftp_log = models.TextField(_('FUGA FTP Upload Log'), blank=True)
    sticker_teaser_cover = models.ImageField(verbose_name=_('Sticker Teaser Cover'),
                                             upload_to=get_sticker_teaser_cover_file_path,
                                             validators=[validate_file_max_15000, validate_image_format],
                                             blank=True, null=True,
                                             help_text=_('Recommended size: 1500x1500 or 3000x3000.'))
    sticker_teaser_audio_track = models.FileField(upload_to=get_sticker_teaser_audio_file_path,
                                                  verbose_name=_('Sticker Teaser Audio Track'),
                                                  validators=[validate_audio_format, validate_file_max_300000],
                                                  blank=True, null=True)

    class Meta:
        """Meta options for the model"""
        verbose_name = _('Product')
        verbose_name_plural = _('Products')
        permissions = [('can_admin_products_all_clients', _(
            'Can Admin Products for All Clients'))]  # controla quem tem acesso ao front para admin produtos tbm.
        ordering = ['-id']

    def __str__(self):
        """str method"""
        product_format = _('N/A')
        artists = self.get_artists_names()
        for item in PRODUCT_FORMATS:
            product_format = item[1] if item[0] == self.format else product_format
        # Se version não for vazio/nulo, o valor é a versão entre parêntesis. Do contrário, é uma string vazia
        version = f" ({str(self.version)})" if self.version else ""
        return f'({str(product_format)}) {str(self.title)}{version} - {artists} - {str(self.upc)}'

    def save(self, *args, **kwargs):
        """ Extendendo para salvar a Thumb
        """
        try:
            self.notify_changes()  # Envia notificações sobre a mudança do produto
        except Exception as e:
            log_error(e)
        make_thumbnail_and_set_for_model(self, 'cover', 'cover_thumbnail')
        super().save(*args, **kwargs)  # Tem que salvar antes de fazer as verificações pra ter disponível o campo id
        from music_system.apps.label_catalog.tasks import check_for_similar_products_within_the_release_week, check_for_release_date_on_holidays
        check_for_similar_products_within_the_release_week.apply_async((self.id,), countdown=1)
        check_for_release_date_on_holidays.apply_async((self.id,), countdown=3)

    def notify_changes(self):
        """ Notifica sobre mudanças feitas no modelo.
        """
        release_date = self.get_field_diff('date_release') or self.date_release
        if type(release_date) == tuple:  # Nesse caso, pegamos a data de lçto do field_diff
            release_date = release_date[1]  # Pega a nova data de lçto
        try:
            notifiable_fields = ['date_release', 'upc']  # Campos que, se sofrerem mudanças, devem disparar notificação.
            if any(field in self.changed_fields for field in notifiable_fields):
                green_check_emoji = bytes.decode(b'\xE2\x9C\x85', 'utf8')
                red_times_emoji = bytes.decode(b'\xE2\x9D\x8C', 'utf8')
                pointing_arrow_emoji = bytes.decode(b'\xE2\x9E\xA1', 'utf8')
                changes = ''
                str1 = _('The release date on')
                str2 = _('has been altered to')
                str3 = _('has been altered. These are the changes:')
                for field, change in self.diff.items():
                    if field not in notifiable_fields:
                        continue
                    last_status, current_status = change
                    if field == 'date_release':  # Realiza a formatação desse campo
                        last_status = last_status.strftime('%d/%m/%Y')
                        current_status = current_status.strftime('%d/%m/%Y')
                        # Notifica a Comunicação de mudança na data de lançamento
                        chat_ids = {
                            'lider_atendimento': LIDER_ATENDIEMENTO_TELEGRAM_CHAT_ID,
                            'atendimento': ATENDIEMENTO_TELEGRAM_CHAT_ID,
                            'comunicacao': COMUNICACAO_TELEGRAM_CHAT_ID,
                            'conteudo': CONTEUDO_TELEGRAM_CHAT_ID,
                            'financeiro': FINANCEIRO_TELEGRAM_CHAT_ID,
                            'dev': DEV_TELEGRAM_CHAT_ID,
                        }
                        if not SEND_TELEGRAM_NOTIFICATIONS:
                            log_notification(
                                'nenhuma notificação foi enviada porque SEND_TELEGRAM_NOTIFICATIONS está definida como False.')
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
                            chat_ids = {
                                'lider_atendimento': LIDER_ATENDIEMENTO_TELEGRAM_CHAT_ID,
                                'atendimento': ATENDIEMENTO_TELEGRAM_CHAT_ID,
                                'comunicacao': COMUNICACAO_TELEGRAM_CHAT_ID,
                                'conteudo': CONTEUDO_TELEGRAM_CHAT_ID,
                                'financeiro': FINANCEIRO_TELEGRAM_CHAT_ID,
                                'dev': DEV_TELEGRAM_CHAT_ID,
                            }
                            if not SEND_TELEGRAM_NOTIFICATIONS:
                                log_notification(
                                    'nenhuma notificação foi enviada porque SEND_TELEGRAM_NOTIFICATIONS está definida como False.')
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
                    changes += f'\n{pointing_arrow_emoji} {Product._meta.get_field(field).verbose_name}: {red_times_emoji} {last_status} {green_check_emoji} {current_status}'
                if self.projects:  # Só notifica o conteúdo se o produto tiver projeto atribuído
                    chat_ids = {
                        'lider_atendimento': LIDER_ATENDIEMENTO_TELEGRAM_CHAT_ID,
                        'atendimento': ATENDIEMENTO_TELEGRAM_CHAT_ID,
                        'comunicacao': COMUNICACAO_TELEGRAM_CHAT_ID,
                        'conteudo': CONTEUDO_TELEGRAM_CHAT_ID,
                        'financeiro': FINANCEIRO_TELEGRAM_CHAT_ID,
                        'dev': DEV_TELEGRAM_CHAT_ID,
                    }
                    if not SEND_TELEGRAM_NOTIFICATIONS:
                        log_notification(
                            'nenhuma notificação foi enviada porque SEND_TELEGRAM_NOTIFICATIONS está definida como False.')
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
                chat_ids = {
                    'lider_atendimento': LIDER_ATENDIEMENTO_TELEGRAM_CHAT_ID,
                    'atendimento': ATENDIEMENTO_TELEGRAM_CHAT_ID,
                    'comunicacao': COMUNICACAO_TELEGRAM_CHAT_ID,
                    'conteudo': CONTEUDO_TELEGRAM_CHAT_ID,
                    'financeiro': FINANCEIRO_TELEGRAM_CHAT_ID,
                    'dev': DEV_TELEGRAM_CHAT_ID,
                }
                if not SEND_TELEGRAM_NOTIFICATIONS:
                    log_notification(
                        'nenhuma notificação foi enviada porque SEND_TELEGRAM_NOTIFICATIONS está definida como False.')
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
                # Notificação por sininho e email
                notification_code = SystemNotification.get_product_alteration_code()
                recipients = User.objects.filter(
                    user_user_profile__profilesystemnotification__notification__code=notification_code)
                urgency = 'warning' if release_date - timezone.now().date() < timezone.timedelta(days=8) else 'info'
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
                        notify.send(sender=author, recipient=recipients, verb=verb, action_object=action_object,
                                    url=url,
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
        except Exception as e:
            log_error(e)

    @property
    def oni_id(self) -> str:
        """Retorna o custom oni id do produto
        """
        return f'ONI{self.id}' if self.id else 'N/A'

    @property
    def release_date(self):
        """ Retorna a data de lancamento do produto formatada como string
        """
        return self.date_release.strftime('%d/%m/%Y') if self.date_release else 'N/A'

    @property
    def release_status(self):
        """ Retorna o status de lançamento do produto
        """
        if self.delivery_finished:
            return _('Delivery finished')
        elif self.delivery_started:
            return _('Delivery started')
        elif self.ready_for_delivery:
            return _('Ready for delivery')
        else:
            return '-'

    @property
    def youtube_tags(self) -> str:
        """ Retorna as tags do youtube para o produto.
        """
        import random
        fixed_tags = ['Música Gospel', 'Worship', 'Gospel', 'Música Evangélica', 'Adoração', 'Louvores', 'Hinos',
                      'Louvor', 'Gospel Music', 'Louvores Gospel', 'Louvores de Adoração', 'Lançamento Gospel',
                      'Gospel Lançamentos']
        release_year = self.date_release.year if self.date_release else None
        product_tags = ['Som que Alimenta', self.title, self.main_holder.name, self.get_artists_names()]
        if release_year:
            product_tags.append(f'{self.title} {release_year}')
            product_tags.append(f'{self.main_holder.name} {release_year}')
        if self.version:
            product_tags.append(self.version)
        random.shuffle(product_tags)
        youtube_tags = []
        return ', '.join([*fixed_tags, *product_tags, *youtube_tags])

    def has_transfers(self):
        return _('Yes') if self.productholder_set.count() > 0 else _('No')

    def fuga_ftp_log_event(self, event: str) -> None:
        """
        Registra o acontecimento passado como parâmetro no campo de log do ftp do fuga
        Args:
            event: String que indica o evento

        Returns: None
        """
        self.fuga_ftp_log += f'-> {event}\n'
        self.save()

    @property
    def get_divulgation_date(self):
        today = timezone.now().date()
        color = 'red'
        try:
            if self.date_divulgation <= today:
                color = 'green'
            return format_html(f'<span style="color:{color}">{self.date_divulgation.strftime("%d/%m/%Y")}</span>')
        except TypeError:
            return 'N/A'

    get_divulgation_date.fget.short_description = _('Disclosure Date')

    @property
    def get_release_date(self):
        today = timezone.now().date()
        color = 'red'
        try:
            if self.date_release <= today:
                color = 'green'
            return format_html(f'<span style="color:{color}">{self.date_release.strftime("%d/%m/%Y")}</span>')
        except TypeError:
            return 'N/A'

    get_release_date.fget.short_description = _('Release Date')

    @property
    def get_release_dateandtime(self):
        date_release_element = 'N/A'
        time_release_element = '00:00:00'
        today = timezone.now().date()
        now = timezone.now().time()
        date_color = 'red'
        time_color = 'red'
        try:
            if self.date_release <= today:
                date_color = 'green'
            date_release_element = f'<span style="color:{date_color}">{self.date_release.strftime("%d/%m/%Y")}</span>'
        except TypeError:
            pass
        try:
            if self.time_release <= now:
                time_color = 'green'
            time_release_element = f'<span style="color:{time_color}">{self.time_release.strftime("%H:%M")}</span>'
        except TypeError:
            pass
        return format_html(f'{date_release_element} - {time_release_element}')

    @staticmethod
    def get_new_release_type_code() -> str:
        return 'REL'

    def count_transfers(self) -> int:
        """Retorna a quantidade de repasses que o objeto tem"""
        return self.productholder_set.count()

    @staticmethod
    def get_column_order_choices() -> List[str]:
        """Retorna o dicionario com as colunas do datatables em que os produtos podem ser ordenados"""
        return PRODUCT_ORDER_COLUMN_CHOICES

    @classmethod
    def filter_by_transfers_amount(cls, queryset: QuerySet('Product'), amount: str) -> QuerySet('Product'):
        """Realiza o filtro de acordo com a quantidade de repasses do produto"""
        count_dict = cls.get_transfers_count_dict(queryset)
        queryset = count_dict[amount]
        return queryset

    @staticmethod
    def get_transfers_count_dict(queryset: QuerySet('Product')) -> dict:
        """Apenas retorna o dict com os annotates para reuso de codigo"""
        return {
            'ZERO': queryset.annotate(productholder_count=Count('productholder')).filter(productholder_count=0),
            'ONE': queryset.annotate(productholder_count=Count('productholder')).filter(productholder_count=1),
            'TWO': queryset.annotate(productholder_count=Count('productholder')).filter(productholder_count=2),
            'TWOM': queryset.annotate(productholder_count=Count('productholder')).filter(productholder_count__gte=3),
        }

    @staticmethod
    def filter_objects_based_on_user(request_user_profile: 'Profile', queryset: QuerySet) -> QuerySet:
        """Filtra os objetos da classe para retornar somente os que pertencem ao usuario passado como parametro"""
        filters_dict = {
            'staff': Q(),
            'catalog': Q(productholder__holder__catalog=request_user_profile.get_user_catalog()),
            'holder': Q(productholder__holder_id__in=request_user_profile.get_user_owner_holder_ids()),
        }
        return ObjectFilterer.filter_objects_based_on_user(request_user_profile.get_user_type(), queryset, filters_dict)

    @classmethod
    def filter_objects(cls, searched_value: str, request_user: User, values_list_fields: list = None,
                       custom_query: Q = Q(), initial_queryset: QuerySet('Product') = None) -> QuerySet:
        """Filtra os objetos da classe com base na string passada como parâmetro. Vide docstring do método chamado."""
        queryset = initial_queryset or cls.objects.all()
        search_fields = ['title', 'upc', 'main_holder__name', 'primary_artists__name']
        # Caso o usuario tenha passado uma query como parametro, o filtro sera feito com base nela apenas
        if custom_query:
            # Converte o Q em dict pra ver se o usr esta tentando filtrar por qtd de repasses ou por data
            custom_query_dict = dict(custom_query.__dict__.get('children'))
            if 'transfers_amount' in custom_query_dict:
                queryset = cls.filter_by_transfers_amount(queryset, custom_query_dict.get('transfers_amount'))
            else:
                queryset = queryset.filter(custom_query)
        return ObjectFilterer.filter_objects(cls, searched_value, request_user.user_user_profile, search_fields,
                                             queryset, values_list_fields)

    @staticmethod
    def export_products_to_be_released(products: QuerySet) -> 'HttpResponse':
        """

        """
        titles = [str(_('Title')),
                  str(_('UPC/EAN')),
                  str(_('Release Date')),
                  str(_('Artist')),
                  str(_('Product Format')),
                  ]
        rows = [[
            product.title,
            product.upc,
            product.date_release,
            product.get_artists_names(),
            product.get_format_display(),
        ] for product in products]
        return write_xlsx(titles, rows, _('new_products_to_be_released'))

    @staticmethod
    def autocomplete_search_fields():
        return 'title',

    @staticmethod
    def query_products_by_args(request) -> dict:
        """
            Metodo usado pela api do DataTables para buscar dinamicamente por produtos com base na caixa de busca
            Args:
                request: request da api
            Returns:
                dict contendo a queryset de produtos e outras informacoes relevantes ao DataTables
        """
        return default_query_assets_by_args(request, Product)

    @staticmethod
    def make_new_product(label: dict) -> 'Product':
        """Makes a new product based on a label dict containing all the necessary information for so.
        Obs.: o campo cover do Produto eh gerado a partir do metodo make_product da propria label, apos
        o processo deste metodo terminar de criar o objeto Product
        Args:
            label: dict contendo as informacoes necessarias pra criacao do produto
        Returns:
            produto criado
        """
        product = Product(
            main_holder=label['holder'],
            custom_id=label['upc'],
            upc=label['upc'],
            title=label['title'],
            date_release=label['release_date'],
            time_release=label['video_release_time'],
            audio_release_time=label['audio_release_time'],
            audio_language=label['audio_language'],
            format=label['type'],
            media=label['product_media'],
            version=label['version'],
            notes=label['extras_notes'],
            copyright_text_label=label['copyright_text_label'],
            delivery_notes=label['extras_datasheet'],
            # preview_start_time=label['preview_start_time'],
            onimusic_network_comm_date=label['onimusic_network_comm_date'],
            gender=label['gender'],
            subgender=label['subgender'],
        )
        product.save()
        product.primary_artists.set(label['primary_artists'])
        product.featuring_artists.set(label['featuring_artists'])
        return product

    def get_data_for_api(self, include_assets: bool, include_id: bool = False, include_artists_names: bool = False,
                         include_holders_names: bool = False, include_notes: bool = False,
                         include_task_counter: bool = False, include_assets_extras: bool = False) -> dict:
        """Get product data for api responses
        Args:
            include_assets: includ sub_items - assets
            include_id
            include_notes
            include_artists_names
            include_holders_names
            include_task_counter: Include a task counter to show all tasks vs done. For all projects
            include_assets_extras
        Returns:
            A dict with product details for api usage
        """
        assets_dict = []
        if include_assets:
            assets = self.productasset_set.order_by('order').all()
            for asset in assets:
                asset_dict = asset.asset.get_data_for_api(include_assets_extras)
                asset_dict['order'] = asset.order
                asset_dict['work_song'] = asset.work_song
                assets_dict.append(asset_dict)
        primary_artists = "/".join([item['name'] for item in self.primary_artists.order_by('label_catalog_product_primary_artists.id').values('name')])
        featuring_artists = "/".join([item['name'] for item in self.featuring_artists.order_by('label_catalog_product_featuring_artists.id').values('name')])
        data = {
            # 'admin_url': self.get_admin_url(),
            'id': self.id,
            'main_holder': self.main_holder.name if self.main_holder else '-',
            'title': self.title,
            'release_type': self.get_release_type_display(),
            'upc': self.upc,
            'date_release': self.get_release_date,
            'date_release_no_color': self.date_release.strftime('%d/%m/%Y') if self.date_release else '-',
            'date_divulgation': self.date_divulgation.strftime('%d/%m/%Y') if self.date_divulgation else '-',
            'get_divulgation_date': self.get_divulgation_date,
            'time_release': self.time_release,
            'audio_language': self.get_audio_language_display(),
            'get_release_dateandtime': self.get_release_dateandtime,
            'release_dateandtime': "{} {}".format(self.date_release,
                                                  self.time_release if self.time_release else "00:00:00"),
            'date_recording': self.date_recording.strftime('%d/%m/%Y') if self.date_recording else '-',
            'assets_link': self.assets_link,
            'version': self.version,
            'media': self.get_media_display(),
            'format': self.get_format_display(),
            'gender': self.gender,
            'subgender': self.subgender,
            'copyright_text_label': self.copyright_text_label,
            'primary_artists': primary_artists,
            'featuring_artists': featuring_artists,
            'assets': assets_dict,
            'holders_names': "",
            'artists_names': "",
            'ready_for_delivery': self.ready_for_delivery,
            'label_status': self.get_label_status(),
            'cover_thumb_for_humans_small': self.cover_thumb_for_humans(),
            'oni_id': self.oni_id,
            'youtube_tags': self.youtube_tags,
            'onimusic_network_comm_date': self.onimusic_network_comm_date.strftime(
                '%d/%m/%Y') if self.onimusic_network_comm_date else '-',
        }
        if include_id:
            data['id'] = self.id
        if include_holders_names:
            data['holders_names'] = self.get_holders_names()
        if include_artists_names:
            data['artists_names'] = self.get_artists_names()

        if include_notes:
            data['notes'] = self.notes if self.notes != '' and self.notes is not None else _('N/A')
            data['notes_ads'] = self.notes_ads if self.notes_ads != '' and self.notes_ads is not None else _('N/A')
            data[
                'delivery_notes'] = self.delivery_notes if (
                    self.delivery_notes != '' and self.delivery_notes is not None) else _(
                'N/A')
            data['label_notes'] = self.get_label_extras_notes() if (
                    self.get_label_extras_notes() != '' and self.get_label_extras_notes() is not None) else _(
                'N/A')

        if include_task_counter:
            done_status = Task.get_done_status_code()
            all_tasks = Task.objects.filter(project__productproject__product_id=self.id).count()
            done_tasks = Task.objects.filter(project__productproject__product_id=self.id, status=done_status).count()
            data['task_counter'] = '{}/{}'.format(done_tasks, all_tasks)

        return data

    def get_fuga_miss_csv_data(self) -> list:
        """ Pega os dados do produto para ingestão no MISS do FUGA
        Returns:
            Um dict com os detalhes do produto relevantes para ingestão no FUGA
        """

        from music_system.apps.label_catalog.settings import FUGA_CSV_DICT
        from unidecode import unidecode
        csv_data = [[key for key in FUGA_CSV_DICT.keys()]]
        assets = self.productasset_set.order_by('order').all()
        # fazendo aqui fora do loop pra nao gerar N buscas no bd
        primary_artists = "|".join([item['name'] for item in self.primary_artists.values('name')])
        featuring_artists = "|".join([item['name'] for item in self.featuring_artists.values('name')])
        for asset in assets:
            asset_dict = asset.asset.get_fuga_miss_csv_data()
            asset_dict['Track sequence'] = asset.order
            asset_dict['Album title'] = self.title
            asset_dict['Album version'] = unidecode(self.version or '')
            asset_dict['UPC'] = self.upc
            asset_dict['Catalog number'] = self.oni_id
            asset_dict['Primary artists'] = primary_artists
            asset_dict['Featuring artists'] = featuring_artists
            asset_dict['Release date'] = self.date_release
            asset_dict['Original release date'] = self.date_release
            asset_dict['Label'] = self.copyright_text_label
            asset_dict['CLine year'] = self.date_release.year
            asset_dict['CLine name'] = self.copyright_text_label
            asset_dict['PLine year'] = self.date_release.year
            asset_dict['PLine name'] = self.copyright_text_label
            if self.date_recording:
                asset_dict['Recording year'] = self.date_recording.year
            else:
                asset_dict['Recording year'] = self.date_release.year
            asset_dict['Recording location'] = 'Brasil'
            asset_dict['Album format'] = unidecode(self.get_format_display())
            asset_dict['Original file name'] = asset.audio_track__filename_from_order()
            csv_data.append([value for value in asset_dict.values()])
        return csv_data

    def upload_fuga_miss_files(self):
        """Faz o upload dos dados do produto para o servidor FTP do FUGA"""
        # cria pasta
        from ftplib import FTP
        from ..settings import FUGA_FTP_HOST, FUGA_FTP_USER, FUGA_FTP_PASS
        from music_system.apps.contrib.file_helpers import get_extension
        self.fuga_ftp_log_event(
            f'({timezone.now().strftime("%d/%m/%Y - %H:%M:%S")}) Iniciando upload para o FUGA FTP...')
        try:
            ftp_connection = FTP(host=FUGA_FTP_HOST)
            ftp_connection.login(user=FUGA_FTP_USER, passwd=FUGA_FTP_PASS)
        except ftplib.all_errors as e:
            self.fuga_ftp_log_event('Erro de conexão com o FTP. Finalizando processo. Contacte suporte.')
            log_error(e)
            return
        folder_name = self.upc
        # criando pasta
        try:
            self.fuga_ftp_log_event(f'Criando diretório com nome "{folder_name}".')
            ftp_connection.mkd(folder_name)
            self.fuga_ftp_log_event(f'Diretório "{folder_name}" criado.')
        except Exception:
            # a pasta já existe
            self.fuga_ftp_log_event(f'Diretório "{folder_name}" já existente no FTP. Seu conteúdo será atualizado.')
            try:
                for file in ftp_connection.nlst(folder_name):
                    ftp_connection.delete(f'{folder_name}/{file}')
            except Exception as e:
                self.fuga_ftp_log_event(f'Diretório "{folder_name}" não pode ser alterado. Contacte suporte.')
                log_error(e)
                return
        try:
            # copia capa
            cover_filename = f"{folder_name}{get_extension(self.cover.name)}"
            ftp_connection.storbinary(f'STOR {folder_name}/{cover_filename}', self.cover.open(), 1024)
            self.fuga_ftp_log_event(f'Upload de capa concluído.')
        except Exception as e:
            self.fuga_ftp_log_event('Erro ao fazer upload da capa do produto. Finalizando. Contacte suporte.')
            log_error(e)
            ftp_connection.quit()
            return
        assets = self.productasset_set.order_by('order').all()
        for asset in assets:
            # copia audios
            if asset.asset.media != get_video_only_product_media_code():
                try:
                    audio_filename = asset.audio_track__filename_from_order()
                    ftp_connection.storbinary(f'STOR {folder_name}/{audio_filename}', asset.asset.audio_track.open(),
                                              1024)
                    self.fuga_ftp_log_event(f'Upload do fonograma {asset.asset.__str__()} concluído com sucesso.')
                except Exception as e:
                    self.fuga_ftp_log_event(
                        f'Falha no upload do fonograma {asset.asset.__str__()}. Confira o arquivo de áudio respectivo e contacte suporte.')
                    log_error(e)
            else:
                self.fuga_ftp_log_event(
                    f'O fonograma {asset.asset.__str__()} está marcado como {asset.asset.get_media_display()} e não será enviado.')
        self.fuga_ftp_log_event('Finalizando upload do produto...')
        ftp_connection.quit()

    def get_artists_names(self):
        """Concatenates all artists and feats in a string"""
        return helper_get_artists_names(self.primary_artists.all().order_by('label_catalog_product_primary_artists.id'),
                                        self.featuring_artists.all().order_by(
                                            'label_catalog_product_featuring_artists.id'))

    get_artists_names.short_description = _('Artist')

    def get_artists_card_info(self) -> List[dict]:
        """Retorna os artistas do produto no formato para serem usados nos cards de artista do front"""
        return default_get_artists_card_info(
            self.primary_artists.all().order_by('label_catalog_product_primary_artists.id'),
            self.featuring_artists.all().order_by('label_catalog_product_featuring_artists.id'))

    def get_holders_names(self):
        """Concatenates all transfers and its artists in a string"""
        return helper_get_holders_names(self.productholder_set.all().order_by('id'))

    get_holders_names.short_description = _('Royalty Splits')

    def get_main_holder_name(self):
        """Returns the main holder name"""
        return self.main_holder.name if self.main_holder else 'N/A'

    get_main_holder_name.short_description = _('Holder')

    @staticmethod
    def get_calendar_front_color(ready_for_delivery: bool = False) -> str:
        """Retorna uma string que corresponde a uma cor para representar se o produto esta ou nao pronto para entrega """
        color = 'success'
        if not ready_for_delivery:
            color = 'danger'
        return color

    def cover_for_humans(self):
        """
        Metodo retorna o mesmo que cover_thumb_for_humans. Mantido apenas para evitar dores de cabeca ao longo do codigo
        Existe porque anteriormente a logica desses dois metodos era diferente.
        """
        return self.cover_thumb_for_humans()

    cover_for_humans.short_description = _('Cover Image')

    def cover_thumb_for_humans(self):
        """Retorna o html da miniatura da capa"""
        return get_thumb_with_image_download_url(self.cover, self.cover_thumbnail, 150)

    cover_thumb_for_humans.short_description = cover_for_humans.short_description

    def cover_thumb_for_humans_large(self):
        """Retorna uma versao pequena da miniatura da capa"""
        return get_thumb_with_image_download_url(self.cover, self.cover_thumbnail, 200)

    cover_thumb_for_humans_large.short_description = cover_for_humans.short_description

    def cover_thumb_for_humans_small(self):
        """Retorna uma versao pequena da miniatura da capa"""
        return get_thumb_with_image_download_url(self.cover, self.cover_thumbnail, 60)

    cover_thumb_for_humans_small.short_description = cover_for_humans.short_description

    def get_label_docs(self) -> str:
        """
        Pega os docs da label do produto, caso haja
        """
        label = self.labelproduct_set.first()
        if label is not None:
            return label.docs_for_humans()
        else:
            return _('N/A')

    get_label_docs.short_description = _('Label Docs')

    def get_label_fields(self, field: str, should_format_html: bool, is_method: bool = False) -> Any:
        """
        Metodo default usado para pegar atributos da label do produto, caso exista
        Args:
            field: campo buscado
            should_format_html: indica se deve retornar o atributo formatado para html
            is_method: indica se o atributo eh calculado
        """
        label = self.labelproduct_set.first()
        if is_method:
            try:
                attribute = getattr(label, field, False)()
            except TypeError:
                attribute = False
        else:
            attribute = getattr(label, field, False)

        if label is not None and attribute:
            if should_format_html:
                return return_mark_safe(attribute)
            else:
                return attribute
        else:
            return _('N/A')

    def get_label_media_links(self) -> models.TextField:
        """Retorna o campo media_links da label do produto"""
        return self.get_label_fields('media_links', True)

    get_label_media_links.short_description = _('Label Media Links')

    def get_label_extras_datasheet(self) -> models.TextField:
        """Retorna o campo extras_datasheet da label do produto"""
        return self.get_label_fields('extras_datasheet', True)

    get_label_extras_datasheet.short_description = _('Label Datasheet')

    def get_label_extras_notes(self) -> models.TextField:
        """Retorna o campo extras_notes da label do produto"""
        return self.get_label_fields('extras_notes', True)

    get_label_extras_notes.short_description = _('Label Notes')

    def get_label_extras_bio(self) -> models.TextField:
        """Retorna o campo extras_bio da label do produto"""
        return self.get_label_fields('extras_bio', True)

    get_label_extras_bio.short_description = _('Label Bio')

    def get_label_status(self) -> str:
        """Retorna o campo status da label do produto"""
        return self.get_label_fields('status_for_humans', False, True)

    get_label_status.short_description = _('Label Status')

    def get_label_admin_link(self) -> str:
        """Retorna o link admin para a label do produto, caso exista"""
        label = self.labelproduct_set.first()
        if label is not None:
            return format_html('<a href="{}" target="_blank">{}</a>'.format(label.get_admin_url(), _('Check Label')))

    get_label_admin_link.short_description = _('Label Admin Link')

    def can_send_product_to_fuga_ftp(self) -> str:
        """
            Faz as validações necessárias no Produto para verificar se ele pode ser enviado para o Fuga via FTP
        Returns: String de erro ou string vazia, caso o produto esteja válido
        """
        if not self.upc:  # Se não tiver UPC, já dá pau
            return _('Product has no UPC')

        if not self.cover:  # Se não tiver capa, dá pau
            return _('Product has no cover.')

        invalid_assets = ''  # Se algum fonograma tiver mídia contendo áudio mas estiver sem arquivo, dá pau
        for asset in self.asset_set.all():
            if asset.media != get_video_only_product_media_code() and not asset.audio_track:
                invalid_assets += _(
                    f'The asset {asset.__str__()} has no audio. Upload an audio file or set the media field as "Only Video".')
        # Se entrar nesse if, significa que tem fonograma inválido
        if invalid_assets:
            return invalid_assets

    def download_fuga_csv(self) -> str:
        """Retorna o link para download do csv para ser enviado ao FUGA"""

        if not self.id:
            return ''
        errors = self.can_send_product_to_fuga_ftp()
        # Se houver erros de validação, retorne-os. Se não, retorne o botão correspondente à ação
        return errors if errors else format_html(
            '<a href="{}" target="_blank">{}</a>'.format(reverse('label_catalog:product.fugamiss', args=(self.id,)),
                                                         _('Download Fuga MISS CSV')))

    download_fuga_csv.short_description = _('Fuga MISS CSV')

    def upload_assets_to_fuga(self) -> str:
        """
        Retorna o link para upload dos asset para o ftp do FUGA caso o produto esteja válido, e uma mensagem de erro
            caso contrário.
        Returns: string
        """
        if not self.id:  # Se o objeto ainda não existir (formulário de criação do admin) retorne nada
            return ''

        errors = self.can_send_product_to_fuga_ftp()
        # Se houver erros de validação, retorne-os. Se não, retorne o botão correspondente à ação
        return errors if errors else format_html('<a href="{}" target="_blank">{}</a>'.format(
            reverse('label_catalog:product.fugamissupload', args=(self.id,)), _('Upload to Fuga FTP')))

    upload_assets_to_fuga.short_description = _('Fuga MISS Link')

    def can_have_colab(self) -> bool:
        """Indica se há possibilidade de colab neste produto.
        A regra é: se houver mais de um artista primário e todos os artistas primários forem da Oni, podemos ter colab.
        """
        oni_artists_in_this_product = self.primary_artists.all().values_list('is_onimusic_artist', flat=True)
        return bool(len(oni_artists_in_this_product) > 1 and all(oni_artists_in_this_product))


class ProductLegacyUPC(BaseModel):
    """Legacy UPCs. On save, check if is unique.
    Attrs:
        product: fk para Produto
        upc: cod upc
    """
    product = models.ForeignKey(verbose_name=_('Product'), to=Product, on_delete=models.CASCADE)
    upc = models.CharField(verbose_name=_('UPC/EAN'), max_length=20, unique=True,
        #                    validators=[
        # RegexValidator(regex='^(?!0)[0-9]{13}$',
        #                message=_('UPC must have 13 digits, not start with zero (0) and is digit-only'))]
                           )
    legacy_type = models.CharField(verbose_name=_('Type'), blank=True, null=True, choices=LEGACY_UPC_TYPES,
                                   max_length=3)
    legacy_type_other = models.CharField(verbose_name=_('Type (other)'), blank=True, null=True, max_length=15,
                                         help_text=_('Mutually exclusive with "Type" field.'))

    @property
    def get_type(self):
        return self.get_legacy_type_display() if self.legacy_type else self.legacy_type_other


class ProductProject(BaseModel):
    """Relation between product and projects
    Attrs:
        product: fk para Produto
        project: fk para Projeto
        project_model: fk para o modelo do projeto
    """
    product = models.ForeignKey(verbose_name=_('Product'), to=Product, on_delete=models.CASCADE)
    # assigned automatically on the save method.
    project = models.ForeignKey(verbose_name=_('Project'), to=Project, on_delete=models.CASCADE, blank=True)
    project_model = models.ForeignKey(verbose_name=_('Project Model'), to=ProjectModel, on_delete=models.CASCADE)

    def __str__(self):
        """str method"""
        return 'Connection'

    def project_url(self) -> str:
        """Retorna a url do admin do projeto"""
        return f"{settings.SITE_URL}{reverse('tasks:projects.list')}new-or-edit/{self.project.pk}"

    project_url.short_description = _('Project URL')

    def save(self, **kwargs):
        """Overriding the save method in order to add the `project` field to self."""
        if self.project_id is None:
            front_url = f"{settings.SITE_URL}{reverse('label_catalog:product.list')}{self.product_id}"
            date_release = self.product.date_release.strftime('%d/%m/%Y') if self.product.date_release else _('N/A')
            prefix_description = f'{_("Title")}: {self.product.__str__()} <br><br> Link: <a href="{front_url}" target' \
                                 f'="blank">{front_url}</a><br><br>Medias: {self.product.assets_link}<br><br>' \
                                 f'{_("Release Date")}: {date_release}<br><br>{_("Notes")}: {self.product.notes}'
            self.project_id = self.project_model.deploy_project(self.product.date_release, self.product.__str__(),
                                                                prefix_description)
        super().save()  # Call the "real" save() method.


class Asset(BaseModel, GetAdminUrl, BaseApiDataClass, ModelDiffMixin):
    """Product assets, represents tracks/videos"""
    products = models.ManyToManyField(verbose_name=_('Product'), to=Product, through='ProductAsset')
    isrc = models.CharField(verbose_name=_('ISRC'), max_length=20, unique=True,
                            validators=[RegexValidator(regex='^[A-Z]{2}-?\w{3}-?\d{2}-?\d{5}$',
                                                       message=_('This ISRC is invalid (format).'))])
    title = models.CharField(verbose_name=_('Title'), max_length=100)
    version = models.CharField(verbose_name=_('Product Version'), max_length=40, blank=True, null=True)
    media = models.CharField(verbose_name=_('Product Media'), max_length=4, choices=PRODUCT_MEDIAS)
    audio_language = models.CharField(verbose_name=_('Audio Language'), max_length=4, choices=AUDIO_LANGUAGES,
                                      default='PT')
    gender = models.CharField(verbose_name=_('Gender'), max_length=25, blank=True, null=True)
    subgender = models.CharField(verbose_name=_('Subgender'), max_length=25, blank=True, null=True)
    copyright_text_label = models.CharField(verbose_name=_('(c) Label'), max_length=25, blank=True, null=True)
    youtube_video_id = models.CharField(verbose_name=_('Youtube Video ID'), max_length=25,
                                        blank=True, help_text=_('Only if exists.'))
    youtube_at_asset_id = models.CharField(verbose_name=_('Youtube Art Track Asset ID'), max_length=251,
                                           blank=True, help_text=_(
            'Needed only for direct deals with Youtube. If multiple, use | to separate.'))
    youtube_sr_asset_id = models.CharField(verbose_name=_('Youtube Sound Recording Asset ID'), max_length=251,
                                           blank=True, help_text=_(
            'Needed only for direct deals with Youtube. If multiple, use | to separate.'))
    youtube_mv_asset_id = models.CharField(verbose_name=_('Youtube Music Video Asset ID'), max_length=251,
                                           blank=True,
                                           help_text=_(
                                               'Needed only for direct deals with Youtube. If multiple, use | to '
                                               'separate.'), null=True)
    youtube_composition_asset_id = models.CharField(verbose_name=_('Youtube Composition Asset ID'), max_length=251,
                                                    blank=True,
                                                    help_text=_(
                                                        'Needed only for direct deals with Youtube. If multiple, '
                                                        'use | to separate.'), null=True)
    youtube_label_done = models.BooleanField(verbose_name=_('Youtube Label Done'), default=False)
    youtube_publishing_done = models.BooleanField(verbose_name=_('Youtube Publishing Done'), default=False)
    primary_artists = models.ManyToManyField(verbose_name=_('Primary Artists'), to=Artist, blank=True,
                                             related_name='asset_primary')
    featuring_artists = models.ManyToManyField(verbose_name=_('Feat.'), to=Artist, blank=True,
                                               related_name='asset_feat')
    # publishing variables all optionals
    publishing_id = models.CharField(verbose_name=_('Publishing ID'), max_length=20, blank=True, null=True)
    publishing_title = models.CharField(verbose_name=_('Original Title'), max_length=150, blank=True, null=True)
    publishing_version = models.CharField(verbose_name=_('Version Title'), max_length=150, blank=True, null=True)

    publishing_status = models.CharField(verbose_name=_('Publishing Status'), default="PEN", max_length=3,
                                         choices=ASSET_PUBLISHING_STATUS)
    publishing_comments = models.TextField(verbose_name=_('Publishing Comments'), blank=True, null=True)
    publishing_custom_code_1 = models.TextField(verbose_name=_('Publishing Custom Code 1'), blank=True,
                                                null=True)  # todo remover
    active = models.BooleanField(verbose_name=_('Is Active'), default=True)
    audio_track = models.FileField(upload_to=get_asset_audio_file_path, verbose_name=_('Audio Track'),
                                   validators=[
                                       validate_audio_format,
                                       validate_file_max_300000],
                                   blank=True,
                                   null=True,
                                   help_text=_(
                                       'Max size: 300mb. Insert video link on Links to Medias field above. You can '
                                       'insert a link to the audio file on the Media URLs field instead.'))
    video_cover = models.ImageField(upload_to=get_asset_cover_file_path, verbose_name=_('Video Cover'), blank=True,
                                    null=True,
                                    validators=[validate_file_max_15000, validate_image_format],
                                    help_text=_(
                                        'Recommended size: 1280x720. You can enter a url for the video cover on the '
                                        'Media URLs field instead.'))
    video_cover_thumbnail = models.ImageField(verbose_name=_('Cover Thumb'), upload_to=ASSET_COVER_THUMBS_PATH,
                                              blank=True,
                                              null=True, )
    main_holder = models.ForeignKey(verbose_name=_('Main Holder'), to=Holder, on_delete=models.PROTECT)
    tiktok_preview_start_time = models.TimeField(verbose_name=_('TikTok Preview Start Time'), blank=True, null=True)
    producers = models.CharField(verbose_name=_('Producers'), max_length=250, blank=True, null=True)
    last_sync_with_publishing_company = models.DateTimeField(null=True, blank=True,
                                                             verbose_name=_('Last Sync With Publishing Company App'))

    class Meta:
        """Meta options for the model"""
        verbose_name = _('Asset')
        verbose_name_plural = _('Assets')
        ordering = ['-id']
        permissions = (("audio_management_perm", _('Can manage audio files (add, alter, remove).')),
                       ("can_hear_audio_before_release", _('Can hear audio files before release.')),)

    def save(self, *args, **kwargs):
        """Sobrescrevendo método save para garantir integridade do BD com relação a youtube assets e ISRCs"""
        self.notify_changes()  # Envia notificações sobre a mudança do fonograma
        # salvando a thumb
        make_thumbnail_and_set_for_model(self, 'video_cover', 'video_cover_thumbnail')
        super().save(*args, **kwargs)
        ensure_youtube_asset_integrity_after_asset_update.apply_async((self.id,),
                                                                      eta=timezone.now() + timezone.timedelta(
                                                                          seconds=5))

    def __str__(self):
        """str method"""
        return f'{"(INA)" if not self.active else ""} {self.title} - {self.isrc}'

    def get_latest_product(self):
        product = self.productasset_set.all().order_by('-id').first()
        if not product:
            return None
        return product.product

    def notify_changes(self):
        """ Notifica sobre mudanças feitas no modelo.
        """
        has_project = True
        if product := self.get_latest_product():
            if not product.projects:
                has_project = False
        try:
            # Lista contendo campos que, se sofrerem mudanças, devem disparar notificação.
            notifiable_fields = ['isrc', 'publishing_id', 'primary_artists', 'publishing_custom_code_1',
                                 'publishing_title', 'publishing_version', 'publishing_comments', 'publishing_status']
            if any(field in self.changed_fields for field in notifiable_fields):
                green_check_emoji = bytes.decode(b'\xE2\x9C\x85', 'utf8')
                red_times_emoji = bytes.decode(b'\xE2\x9D\x8C', 'utf8')
                pointing_arrow_emoji = bytes.decode(b'\xE2\x9E\xA1', 'utf8')
                changes = ''
                for field, change in self.diff.items():
                    if field not in notifiable_fields:
                        continue
                    last_status, current_status = change
                    if change in {(None, ''), ('', None)}:
                        continue
                    if field == 'publishing_status':  # Realiza a formatação desse campo
                        for code, status in ASSET_PUBLISHING_STATUS:
                            if code == change[0]:
                                last_status = status
                            elif code == change[1]:
                                current_status = status
                    changes += f'\n{pointing_arrow_emoji} {Asset._meta.get_field(field).verbose_name}: {red_times_emoji} {last_status} {green_check_emoji} {current_status}'
                str1 = _('has been altered. These are the changes:')
                if changes:
                    if has_project:
                        chat_ids = {
                            'lider_atendimento': LIDER_ATENDIEMENTO_TELEGRAM_CHAT_ID,
                            'atendimento': ATENDIEMENTO_TELEGRAM_CHAT_ID,
                            'comunicacao': COMUNICACAO_TELEGRAM_CHAT_ID,
                            'conteudo': CONTEUDO_TELEGRAM_CHAT_ID,
                            'financeiro': FINANCEIRO_TELEGRAM_CHAT_ID,
                            'dev': DEV_TELEGRAM_CHAT_ID,
                        }
                        if not SEND_TELEGRAM_NOTIFICATIONS:
                            log_notification(
                                'nenhuma notificação foi enviada porque SEND_TELEGRAM_NOTIFICATIONS está definida como False.')
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
                    chat_ids = {
                        'lider_atendimento': LIDER_ATENDIEMENTO_TELEGRAM_CHAT_ID,
                        'atendimento': ATENDIEMENTO_TELEGRAM_CHAT_ID,
                        'comunicacao': COMUNICACAO_TELEGRAM_CHAT_ID,
                        'conteudo': CONTEUDO_TELEGRAM_CHAT_ID,
                        'financeiro': FINANCEIRO_TELEGRAM_CHAT_ID,
                        'dev': DEV_TELEGRAM_CHAT_ID,
                    }
                    if not SEND_TELEGRAM_NOTIFICATIONS:
                        log_notification(
                            'nenhuma notificação foi enviada porque SEND_TELEGRAM_NOTIFICATIONS está definida como False.')
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
        except Exception as e:
            log_error(e)

    @property
    def is_released(self):
        """Indica se este fonograma já foi lançado, verificando as datas de lançamentos de seu primeiro produto
        """
        products = self.get_associated_products()
        if len(products) == 0:
            return True  # Se for só um fonograma solto não tem como saber se já lançou, consideraremos que sim
        if products[0].get('release_date', '1900-01-01') <= timezone.now().date():
            return True  # Se a data do primeiro produto relacionado for uma data passada, o áudio já está disponível
        return False

    @property
    def release_date(self):
        """Indica se este fonograma já foi lançado, verificando as datas de lançamentos de seu primeiro produto
        """
        products = self.get_associated_products()
        if len(products) == 0:
            return None
        return products[-1].get('release_date', None)

    def audio_track_url(self):
        """Retorna a url do audio"""
        return self.audio_track.url if self.audio_track else None

    def has_transfers(self):
        return _('Yes') if self.assetholder_set.count() > 0 else _('No')

    def count_transfers(self) -> int:
        """Retorna a quantidade de repasses que o objeto tem"""
        return self.assetholder_set.count()

    def get_main_holder_name(self):
        """Returns the main holder name"""
        return self.main_holder.name if self.main_holder else 'N/A'

    get_main_holder_name.short_description = _('Holder')

    def update_related_youtube_assets(self) -> None:
        """
        Atualiza os campos de Titular, Artista (fk), Titulo e Repasses dos YoutubeAssets relacionados
        """
        # Pegando os youtube assets por fk ou por isrc
        youtube_assets = YoutubeAsset.objects.filter(Q(Q(related_asset=self) | Q(isrc=self.isrc)))
        # Atualizando os campos que podem ser atualizados diretamente
        youtube_assets.update(title=self.title, main_holder=self.main_holder, related_asset=self)
        # iterando sobre os youtube assets para atualizar artistas e repasses
        for yt_asset in youtube_assets:
            # removendo todos os artistas anteriores para garantir a integridade desses dados
            for yt_asset_artist in yt_asset.primary_artists.all():
                yt_asset.primary_artists.remove(yt_asset_artist)
            # adicionando os novos artistas
            for asset_artist in self.primary_artists.all().order_by('label_catalog_asset_primary_artists.id'):
                yt_asset.primary_artists.add(asset_artist)
            # removendo todos os repasses anteriores para garantir a integridade desses dados
            for yt_asset_holder in yt_asset.youtubeassetholder_set.all():
                yt_asset_holder.delete()
            # criando os novos repasses
            for asset_holder in self.assetholder_set.all():
                YoutubeAssetHolder.objects.create(holder=asset_holder.holder, artist=asset_holder.artist,
                                                  percentage=asset_holder.percentage, youtube_asset=yt_asset,
                                                  ignore_main_holder_share=asset_holder.ignore_main_holder_share)

    @staticmethod
    def get_column_order_choices() -> List[str]:
        """Retorna o dicionario com as colunas do datatables em que os fonogramas podem ser ordenados"""
        return ASSET_ORDER_COLUMN_CHOICES

    def get_artists_card_info(self) -> List[dict]:
        """Retorna os artistas do produto no formato para serem usados nos cards de artista do front"""
        return default_get_artists_card_info(
            self.primary_artists.all().order_by('label_catalog_asset_primary_artists.id'),
            self.featuring_artists.all().order_by('label_catalog_asset_featuring_artists.id'))

    @staticmethod
    def filter_objects_based_on_user(request_user_profile: 'Profile', queryset: QuerySet) -> QuerySet:
        """Filtra os objetos da classe para retornar somente os que pertencem ao usuario passado como parametro"""
        filters_dict = {
            'staff': Q(),
            'catalog': Q(assetholder__holder__catalog=request_user_profile.get_user_catalog()),
            'holder': Q(assetholder__holder_id__in=request_user_profile.get_user_owner_holder_ids()),
        }
        return ObjectFilterer.filter_objects_based_on_user(request_user_profile.get_user_type(), queryset, filters_dict)

    @classmethod
    def filter_by_transfers_amount(cls, queryset: QuerySet('Asset'), amount: str) -> QuerySet('Asset'):
        """Realiza o filtro de acordo com a quantidade de repasses do fonograma"""
        count_dict = cls.get_transfers_count_dict(queryset)
        queryset = count_dict[amount]
        return queryset

    @staticmethod
    def get_transfers_count_dict(queryset: QuerySet('Asset')) -> dict:
        """Apenas retorna o dict com os annotates para reuso de codigo"""
        return {
            'ZERO': queryset.annotate(assetholder_count=Count('assetholder')).filter(assetholder_count=0),
            'ONE': queryset.annotate(assetholder_count=Count('assetholder')).filter(assetholder_count=1),
            'TWO': queryset.annotate(assetholder_count=Count('assetholder')).filter(assetholder_count=2),
            'TWOM': queryset.annotate(assetholder_count=Count('assetholder')).filter(assetholder_count__gte=3),
        }

    @classmethod
    def filter_by_youtube_statuses(cls, queryset: QuerySet('Asset'), status: str):
        """Realiza o filtro de acordo com os status booleanos dos atributos youtube_label_done e youtube_publishing_done
        """
        filter_dict = {
            # nothing_done
            'NAN': queryset.filter(youtube_label_done=False, youtube_publishing_done=False),
            # recorder_done
            'RED': queryset.filter(youtube_label_done=True),
            # recorder_not_done
            'REN': queryset.filter(youtube_label_done=False),
            # publishing_status_done
            'PBD': queryset.filter(youtube_publishing_done=True),
            # publishing_status_not_done
            'PBN': queryset.filter(youtube_publishing_done=False),
            # all_done
            'ALL': queryset.filter(youtube_label_done=True, youtube_publishing_done=True),
        }
        return filter_dict[status]

    @classmethod
    def filter_objects(cls, searched_value: str, request_user: User, values_list_fields: list = None,
                       custom_query: Q = Q(), initial_queryset: QuerySet('Asset') = None) -> QuerySet:
        """Filtra os objetos da classe com base na string passada como parâmetro. Vide docstring do método chamado."""
        queryset = initial_queryset or cls.objects.all()
        search_fields = ['title', 'isrc', 'main_holder__name']
        # Caso o usuario tenha passado uma query como parametro, o filtro sera feito com base nela apenas
        if custom_query:
            # Converte o Q em dict pra ver se o usr esta tentando filtrar por qtd de repasses ou por data
            custom_query_dict = dict(custom_query.__dict__.get('children'))
            if 'transfers_amount' not in custom_query_dict and 'youtube_status' not in custom_query_dict:
                queryset = queryset.filter(custom_query)
            else:
                if 'transfers_amount' in custom_query_dict:
                    queryset = cls.filter_by_transfers_amount(queryset, custom_query_dict.get('transfers_amount'))
                if 'youtube_status' in custom_query_dict:
                    queryset = cls.filter_by_youtube_statuses(queryset, custom_query_dict.get('youtube_status'))
        return ObjectFilterer.filter_objects(cls, searched_value, request_user.user_user_profile, search_fields,
                                             queryset, values_list_fields)

    @staticmethod
    def autocomplete_search_fields():
        return 'title', 'isrc',

    @staticmethod
    def query_assets_by_args(request) -> dict:
        """
            Metodo usado pela api do DataTables para buscar dinamicamente por fonogramas com base na caixa de busca
            Args:
                request: request da api
            Returns:
                dict contendo a queryset de produtos e outras informacoes relevantes ao DataTables
        """
        return default_query_assets_by_args(request, Asset)

    @staticmethod
    def make_new_asset(asset_data: dict) -> 'Asset':
        """Creates a new asset based on the label assets
        Args:
            asset_data: dict contendo os dados para criacao do fonograma
        Returns:
            objeto Asset
        """
        try:
            asset = Asset(
                main_holder=asset_data.get('holder'),
                isrc=asset_data.get('isrc'),
                title=asset_data.get('title'),
                media=asset_data.get('media'),
                audio_language=asset_data.get('audio_language'),
                producers=asset_data.get('producers'),
                version=asset_data.get('version'),
                tiktok_preview_start_time=asset_data.get('tiktok_preview_start_time'),
                gender=asset_data.get('gender'),
                subgender=asset_data.get('subgender'),
            )
            asset.save()
            asset.primary_artists.set(asset_data.get('primary_artists'))
            asset.featuring_artists.set(asset_data.get('featuring_artists'))
            return asset
        except Exception as e:
            log_error(e)

    def get_title_and_version(self) -> str:
        """Retorna o titulo e versao do fonograma"""
        return f'{self.title} ({self.version})' if self.version else f'{self.title}'

    def get_artists_names(self) -> str:
        """Concatenates all artists and feats in a string"""
        return helper_get_artists_names(self.primary_artists.all().order_by('label_catalog_asset_primary_artists.id'),
                                        self.featuring_artists.all().order_by(
                                            'label_catalog_asset_featuring_artists.id'))[:100]

    get_artists_names.short_description = _('Artist')

    def get_holders_names(self) -> str:
        """Concatenates all transfers and its artists in a string"""
        return helper_get_holders_names(self.assetholder_set.all().order_by('id'))

    get_holders_names.short_description = _('Royalty Splits')

    def get_composers_names(self) -> str:
        """Concatenates all composers in a string"""
        return helper_get_composers_names(
            AssetComposer.objects.filter(assetcomposerlink__asset=self).order_by('assetcomposerlink__id'))

    get_composers_names.short_description = _('Composers')

    def get_legacy_isrcs(self) -> List[dict]:
        """Retorna uma lista de dicts com os Legacy ISRCs do asset"""
        legacy_isrcs = AssetLegacyISRC.objects.filter(asset=self).distinct()
        return [{'isrc': legacy_isrc.isrc,
                 'legacy_type': legacy_isrc.get_legacy_type()}
                for legacy_isrc in legacy_isrcs]

    def get_youtube_embedded(self) -> str:
        """Código de embeded do youtube. Retorna apenas se tiver video_id"""
        return default_get_youtube_embedded(self.youtube_video_id)

    get_youtube_embedded.short_description = _('Youtube Embedded')

    def get_related_youtube_assets_as_html_btns(self) -> str:
        """Retorna os objetos YoutubeAsset relacionados à este fonograma em formato de string formatada para html"""
        youtube_assets_btns = ''.join(
            f'<br><a href="{related_yout_asset.get_admin_url()}" target="_blank" class="btn btn-primary btn-sm mb-1"><i class="fas fa-external-link-alt"></i>{related_yout_asset}</a>'
            for related_yout_asset in YoutubeAsset.objects.filter(related_asset=self))

        return format_html(youtube_assets_btns)

    def get_data_for_api(self, get_extras: bool = False) -> dict:
        """Get Asset data for api responses"""
        primary_artists = "/".join([item['name'] for item in self.primary_artists.order_by('label_catalog_asset_primary_artists.id').values('name')])
        featuring_artists = "/".join([item['name'] for item in self.featuring_artists.order_by('label_catalog_asset_featuring_artists.id').values('name')])
        data = {
            'main_holder': self.main_holder.name or '-',
            'title': self.title,
            'isrc': self.isrc,
            'version': self.version,
            'media': self.get_media_display(),
            'gender': self.gender,
            'subgender': self.subgender,
            'producers': self.producers,
            'copyright_text_label': self.copyright_text_label,
            'audio_language': self.get_audio_language_display(),
            'primary_artists': primary_artists,
            'featuring_artists': featuring_artists,
            'artists': self.get_artists_names(),
            'get_video_cover_thumb_for_humans': self.get_video_cover_thumb_for_humans(),
            'related_youtube_assets': self.get_related_youtube_assets_as_html_btns(),
        }
        if get_extras:
            data['asset_id'] = self.id
            data['asset_youtube_video_id'] = self.youtube_video_id
            data['asset_youtube_at_asset_id'] = self.youtube_at_asset_id
            data['asset_youtube_sr_asset_id'] = self.youtube_sr_asset_id
            data['asset_youtube_mv_asset_id'] = self.youtube_mv_asset_id
            data['asset_youtube_composition_asset_id'] = self.youtube_composition_asset_id
            data['asset_youtube_label_done'] = self.youtube_label_done
            data['asset_youtube_publishing_done'] = self.youtube_publishing_done
            data['asset_publishing_id'] = self.publishing_id or 'N/A'
            data['asset_publishing_title'] = self.publishing_title or 'N/A'
            data['asset_publishing_version'] = self.publishing_version or 'N/A'
            data['asset_publishing_status'] = self.get_publishing_status_display()
            data['asset_publishing_comments'] = format_html(
                self.publishing_comments) if self.publishing_comments else 'N/A'
            data[
                'asset_publishing_custom_code_1'] = self.publishing_custom_code_1 or 'N/A'
            data['get_youtube_embedded'] = self.get_youtube_embedded()
            data['asset_active'] = self.active
            data['asset_audio_track'] = self.get_audio_track_for_humans()
            data['asset_video_cover'] = self.get_video_cover_for_humans()
            data['asset_composers'] = self.get_composers_names()
            data['legacy_isrcs'] = self.get_legacy_isrcs()
            data['tiktok_preview_start_time'] = self.tiktok_preview_start_time
        return data

    def get_fuga_miss_csv_data(self) -> dict:
        """ Pega os dados do fonograma para ingestão no MISS do FUGA
        Returns:
            Um dict com os detalhes do produto relevantes para ingestão no FUGA
        """
        from music_system.apps.label_catalog.settings import FUGA_CSV_DICT
        data = FUGA_CSV_DICT
        data['Track title'] = self.title
        data['Track version'] = self.version
        data['ISRC'] = self.isrc
        data['Producers'] = self.producers
        data['Track primary artists'] = "|".join([item['name'] for item in self.primary_artists.values('name')])
        data['Track featuring artists'] = "|".join([item['name'] for item in self.featuring_artists.values('name')])
        data['Volume number'] = '1'  # todo caso implementar multiplos discos mexer aqui
        # data['Label Copyright'] = self.copyright_text_label
        try:
            data['Audio language'] = self.audio_language
        except KeyError:
            data['Audio language'] = 'PT'

        if self.gender:
            data['Track main subgenre'] = self.gender
        if self.subgender:
            data['Track alternate subgenre'] = self.subgender
        if self.tiktok_preview_start_time:
            data['Preview start'] = self.get_tiktok_preview_start_time_in_seconds()
        data['Writers'] = "|".join([item.asset_composer.name for item in self.assetcomposerlink_set.all()])
        data['Composers'] = "|".join([item.asset_composer.name for item in self.assetcomposerlink_set.all()])
        # Segundo backoffice, o preenchimento do lyricists deve ser IDÊNTICO ao do composers.
        data['Lyricists'] = "|".join([item.asset_composer.name for item in self.assetcomposerlink_set.all()])
        data['Publishers'] = "|".join([item.asset_composer.publisher for item in self.assetcomposerlink_set.all()])

        return data

    def get_tiktok_preview_start_time_in_seconds(self):
        """
        Retorna o tempo, em segundos, do início da prévia do fonograma
        Returns:
            O tempo, em segundos, do início da prévia, ou 0, caso este tempo não seja definido
        """
        if not self.tiktok_preview_start_time:
            return 0
        time_as_list = self.tiktok_preview_start_time.strftime("%H:%M:%S").split(':')
        # O tempo, no BD, é guardado como horas e minutos, mas é interpretado pelos usuários como minutos e segundos.
        return int(time_as_list[0]) * 60 + int(time_as_list[1])

    @staticmethod
    def get_publishing_status_approved() -> str:
        """Retorna o status de aprovado padrao para publicacao do fonograma"""
        return 'APP'

    def get_video_cover_for_humans(self):
        return get_thumb_with_image_download_url(self.video_cover, self.video_cover_thumbnail, 50)

    def get_video_cover_thumb_for_humans(self):
        return get_thumb_with_image_download_url(self.video_cover, self.video_cover_thumbnail, 50)

    def get_video_cover_thumb_large_for_humans(self):
        return get_thumb_with_image_download_url(self.video_cover, self.video_cover_thumbnail, 200)

    def get_audio_track_for_humans(self):
        return get_audio_track_for_humans_from_filefield(self.audio_track)

    def get_associated_products(self) -> List[dict]:
        """Retorna uma lista de dicts contendo informações relevantes dos produtos relacionados a este asset
        """
        return [{'cover': product_asset.product.cover_thumb_for_humans_small, 'title': product_asset.product.title,
                 'id': product_asset.product.id, 'upc': product_asset.product.upc,
                 'release_date': product_asset.product.date_release} for product_asset in
                self.productasset_set.all().order_by('product__date_release')]


class AssetComposer(BaseModel):
    """Assets composers. For reference only."""
    name = models.CharField(verbose_name=_('Name'), max_length=150)
    publisher = models.CharField(verbose_name=_('Publisher'), max_length=60)

    class Meta:
        """Meta options for the model"""
        ordering = ['id']
        verbose_name = _('Composer')
        verbose_name_plural = _('Composers')

    def __str__(self):
        """str method"""
        return f'{self.name} - {self.publisher}'

    @staticmethod
    def autocomplete_search_fields():
        return 'name',

    @classmethod
    def filter_objects(cls, searched_value: str, request_user: User, values_list_fields: list = None,
                       custom_query: Q = Q(), initial_queryset: QuerySet('AssetComposer') = None) -> QuerySet:
        """Filtra os objetos da classe com base na string passada como parâmetro. Vide docstring do método chamado."""
        queryset = initial_queryset or cls.objects.all()
        search_fields = ['name', 'publisher']
        # Caso o usuario tenha passado uma query como parametro, o filtro sera feito com base nela apenas
        if custom_query:
            queryset = queryset.filter(custom_query)
        results = ObjectFilterer.filter_objects(cls, searched_value, request_user.user_user_profile, search_fields,
                                                queryset, values_list_fields)
        if values_list_fields:
            from django.db.models.functions import Concat
            from django.db.models import Value
            # Para o caso específico de AssetComposer, a string a ser retornada na label é: nome(editora)
            results = results.annotate(
                composer_with_publisher=Concat('name', Value('('), 'publisher', Value(')'))).values_list('id',
                                                                                                         'composer_with_publisher')
        return results


class AssetComposerLink(BaseModel):
    """Assets composers. For reference only."""
    asset = models.ForeignKey(verbose_name=_('Asset'), to=Asset, on_delete=models.CASCADE)
    asset_composer = models.ForeignKey(verbose_name=_('Asset'), to=AssetComposer, on_delete=models.CASCADE)
    percentage = models.DecimalField(verbose_name=_("Percentage"), default=100.00, decimal_places=2, max_digits=5)

    def save(self, force_insert=False, force_update=False, using=None, update_fields=None):
        """ Override pra envio de notificação.
        """
        pencil_emoji = bytes.decode(b'\xE2\x9C\x8F', 'utf8')
        str1 = _('The composers on')
        str2 = _('have been altered.')
        chat_ids = {
            'lider_atendimento': LIDER_ATENDIEMENTO_TELEGRAM_CHAT_ID,
            'atendimento': ATENDIEMENTO_TELEGRAM_CHAT_ID,
            'comunicacao': COMUNICACAO_TELEGRAM_CHAT_ID,
            'conteudo': CONTEUDO_TELEGRAM_CHAT_ID,
            'financeiro': FINANCEIRO_TELEGRAM_CHAT_ID,
            'dev': DEV_TELEGRAM_CHAT_ID,
        }
        if not SEND_TELEGRAM_NOTIFICATIONS:
            log_notification(
                'nenhuma notificação foi enviada porque SEND_TELEGRAM_NOTIFICATIONS está definida como False.')
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
            chat_ids = {
                'lider_atendimento': LIDER_ATENDIEMENTO_TELEGRAM_CHAT_ID,
                'atendimento': ATENDIEMENTO_TELEGRAM_CHAT_ID,
                'comunicacao': COMUNICACAO_TELEGRAM_CHAT_ID,
                'conteudo': CONTEUDO_TELEGRAM_CHAT_ID,
                'financeiro': FINANCEIRO_TELEGRAM_CHAT_ID,
                'dev': DEV_TELEGRAM_CHAT_ID,
            }
            if not SEND_TELEGRAM_NOTIFICATIONS:
                log_notification(
                    'nenhuma notificação foi enviada porque SEND_TELEGRAM_NOTIFICATIONS está definida como False.')
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
        super(AssetComposerLink, self).save()


class ProductAsset(BaseModel):
    """A relationship between products and assets """
    product = models.ForeignKey(verbose_name=_('Product'), to=Product, on_delete=models.CASCADE)
    asset = models.ForeignKey(verbose_name=_('Asset'), to=Asset, on_delete=models.PROTECT)
    order = models.IntegerField(verbose_name=_('Order'))
    work_song = models.BooleanField(verbose_name=_('Work Song'), default=False)
    copyright_license_file = models.FileField(upload_to='label/files', verbose_name=_('Copyright License'), blank=True,
                                              null=True, validators=[validate_file_max_10000])

    class Meta(object):
        ordering = ('order',)

    def __str__(self):
        """str method"""
        return 'Connection'

    @staticmethod
    def make_product_asset(data: dict) -> 'ProductAsset':
        """Makes a new product asset relation based on the label data
        Args:
            data
        Returns:
            objeto ProductAsset
        """
        try:
            product_asset = ProductAsset.objects.create(
                product=data['product'],
                asset=data['asset'],
                order=data['order'],
                work_song=data['work_song'],
            )
            return product_asset
        except Exception as e:
            log_error(e)

    def audio_track__filename_from_order(self):
        """Retorna um filename usando a extensão original, mas com o número da ordem."""
        from music_system.apps.contrib.file_helpers import get_extension
        return '{}_1_{}{}'.format(self.product.upc, str(self.order).zfill(2),
                                  get_extension(self.asset.audio_track.name) if get_extension(
                                      self.asset.audio_track.name) != '' else '.wav')

    def get_asset__video_cover_thumb(self) -> str:
        """Retorna a miniatura da capa do video do fonograma relacionado"""
        return self.asset.get_video_cover_thumb_for_humans()

    get_asset__video_cover_thumb.short_description = _('Video Cover')

    def get_asset__audio_track(self) -> str:
        """Retorna o html do audio do fonograma relacionado"""
        return self.asset.get_audio_track_for_humans()

    get_asset__audio_track.short_description = _('Audio Track')

    def get_asset_media(self) -> str:
        """Retorna o display do atributo media do fonograma relacionado"""
        return self.asset.get_media_display()

    get_asset_media.short_description = _('Media')

    def get_asset_publishing_status(self) -> str:
        """Retorna o status de publicacao do fonograma relacionado"""
        if self.asset.publishing_status == 'DEC':
            return format_html(
                '<button type="button" class="btn btn-default prapopovar" data-toggle="popover" data-trigger="hover" data-placement="right" data-content="' +
                self.asset.publishing_comments + '">' + self.asset.get_publishing_status_display() + '</button>')
        else:
            return self.asset.get_publishing_status_display()

    get_asset_publishing_status.short_description = _('Publishing Status')


class AssetLegacyISRC(BaseModel):
    """Legacy ISRCs. On save, check if is unique."""
    asset = models.ForeignKey(verbose_name=_('Asset'), to=Asset, on_delete=models.PROTECT)
    isrc = models.CharField(verbose_name=_('ISRC'), max_length=20, unique=True)
    legacy_type = models.CharField(verbose_name=_('Type'), blank=True, null=True, choices=LEGACY_ISRC_TYPES,
                                   max_length=3)
    legacy_type_other = models.CharField(verbose_name=_('Type (other)'), blank=True, null=True, max_length=15,
                                         help_text=_('Mutually exclusive with "Type" field.'))

    class Meta:
        verbose_name = _('Related ISRC')
        verbose_name_plural = _('Related ISRCs')

    def __str__(self):
        return f'({self.get_legacy_type()}) {self.isrc}'

    def get_legacy_type(self):
        if not any([self.legacy_type, self.legacy_type_other]):
            return 'N/A'
        else:
            return self.get_legacy_type_display() or self.legacy_type_other


class YoutubeAsset(BaseModel, GetAdminUrl):
    """Youtube assets, represents tracks/videos"""
    asset_id = models.CharField(verbose_name=_('Asset ID'), max_length=20, unique=True)
    related_asset = models.ForeignKey(to=Asset, verbose_name=_('Related Asset'), on_delete=models.PROTECT,
                                      null=True,
                                      blank=True)
    type = models.CharField(verbose_name=_('Type'), max_length=4, choices=YOUTUBE_ASSET_TYPES)
    custom_id = models.CharField(verbose_name=_('Custom ID'), max_length=20, blank=True)
    upc = models.CharField(verbose_name=_('UPC/EAN'), max_length=20, null=True, blank=True,
        #                    validators=[
        # RegexValidator(regex='^(?!0)[0-9]{13}$',
        #                message=_('UPC must have 13 digits, not start with zero (0) and is digit-only'))]
                           )
    isrc = models.CharField(verbose_name=_('ISRC'), max_length=20, blank=True,
                            validators=[RegexValidator(regex='^[A-Z]{2}-?\w{3}-?\d{2}-?\d{5}$',
                                                       message=_('This ISRC is invalid (format).'))])
    title = models.CharField(verbose_name=_('Title'), max_length=100, blank=True)
    artist = models.CharField(verbose_name=_('Artist'), max_length=100, blank=True)
    notes = models.TextField(verbose_name=_('Notes'), blank=True)
    active = models.BooleanField(verbose_name=_('Is Active'), default=True)

    policy = models.CharField(verbose_name=_('Youtube Policy'), max_length=5, default='MON', choices=(
        ('MON', _('Monetize')), ('BLO', _('Block')), ('TRA', _('Track'))
    ))

    music_cms = models.BooleanField(verbose_name=_('Music CMS'), default=True)
    main_holder = models.ForeignKey(verbose_name=_('Main Holder'), to=Holder, on_delete=models.PROTECT)

    primary_artists = models.ManyToManyField(verbose_name=_('Primary Artists'), to=Artist, blank=True,
                                             related_name='youtube_asset_primary')

    class Meta:
        """Meta options for the model"""
        verbose_name = _('Youtube Asset')
        verbose_name_plural = _('Youtube Assets')

    def __str__(self):
        """str method"""
        return "{} {} - {} ({})".format(self.get_type_display(), self.asset_id,
                                        self.title if self.title != "" else _('*NO TITLE*'),
                                        self.artist)

    def get_artists_names(self) -> str:
        return ', '.join([artist.__str__() for artist in
                          self.primary_artists.all().order_by('label_catalog_youtubeasset_primary_artists.id')])

    def count_transfers(self) -> int:
        """Retorna a quantidade de repasses que o objeto tem"""
        return self.youtubeassetholder_set.count()

    def get_main_holder_name(self):
        """Returns the main holder name"""
        return self.main_holder.name if self.main_holder else 'N/A'

    get_main_holder_name.short_description = _('Holder')

    def get_custom_ids(self) -> str:
        """Retorna os custom_ids dos holders """
        holders = self.youtubeassetholder_set.all()
        label_set = [self.main_holder.custom_id]
        for artist in self.primary_artists.all().order_by('label_catalog_youtubeasset_primary_artists.id'):
            label_set[:len(label_set) - 1] = ["{}_{}".format(self.main_holder.custom_id, artist.custom_id)]

        for holder in holders:
            label_set[:len(label_set) - 1] = [holder.holder.custom_id,
                                              "{}_{}".format(holder.holder.custom_id, holder.artist.custom_id)]

        return "|".join(label_set)

    # todo daniel testar se ta fazendo isso certo. Todo asset do youtube tem que exibir label nem que seja so do titular.

    get_custom_ids.short_description = _('Youtube Label')

    def get_holders_names(self) -> str:
        """Concatenates all transfers and its artists in a string
        """
        return helper_get_holders_names(self.youtubeassetholder_set.all().order_by('id'))

    get_holders_names.short_description = _('Royalty Splits')

    @staticmethod
    def get_youtube_asset_type_for_youtube(asset_type: str) -> str:
        """Returns a dictionary of asset types to comply with youtube csv templates.
        """
        types_dict = {
            'MV': 'music_video',
            'SR': 'sound_recording',
            'WEB': 'web',
            'AT': 'art_track',
            'MO': 'movie',
            'TV': 'episode',
        }
        return types_dict.get(asset_type, None)

    @staticmethod
    def get_youtube_policy_for_youtube(asset_type: str) -> str:
        """Returns a dictionary of asset types to comply with youtube csv templates.
        """
        types_dict = {
            'MON': 'Monetize in all countries',
            'BLO': 'Block in all countries',
            'TRA': 'Track in all countries'
        }
        return types_dict.get(asset_type, None)

    @classmethod
    def filter_objects(cls, searched_value: str, request_user: User, values_list_fields: list = None,
                       custom_query: Q = Q(), initial_queryset: QuerySet('YoutubeAsset') = None) -> QuerySet:
        """Filtra os objetos da classe com base na string passada como parâmetro. Vide docstring do método chamado."""
        queryset = initial_queryset or cls.objects.all()
        search_fields = ['asset_id', 'title', 'isrc', 'main_holder__name', 'artist', 'custom_id',
                         'main_holder__custom_id', 'youtubeassetholder__holder__custom_id',
                         'primary_artists__custom_id']
        # Caso o usuario tenha passado uma query como parametro, o filtro sera feito com base nela apenas
        if custom_query:
            # Converte o Q em dict pra ver se o usr esta tentando filtrar por qtd de repasses ou por data
            custom_query_dict = dict(custom_query.__dict__.get('children'))
            if 'holders_count' in custom_query_dict:
                queryset = cls.filter_by_holders_amount(queryset, custom_query_dict.get('holders_count'))
            elif 'created_at' in custom_query_dict:
                date = custom_query_dict.get('created_at')
                queryset = cls.get_number_of_days_based_on_string_timestamp(queryset).get(date)
            else:
                queryset = queryset.filter(custom_query)
        return ObjectFilterer.filter_objects(cls, searched_value, request_user.user_user_profile, search_fields,
                                             queryset, values_list_fields)

    @classmethod
    def filter_by_holders_amount(cls, queryset: QuerySet('YoutubeAsset'), amount: str) -> QuerySet('YoutubeAsset'):
        """Realiza o filtro de acordo com a quantidade de repasses do asset"""
        count_dict = cls.get_holders_count_dict(queryset)
        queryset = count_dict[amount]
        return queryset

    @staticmethod
    def get_number_of_days_based_on_string_timestamp(queryset: QuerySet('YoutubeAsset')) -> dict:
        today = timezone.now()
        return {
            'today': queryset.filter(created_at__gte=today.date()),
            'week': queryset.filter(created_at__gte=today.date() - timezone.timedelta(days=7)),
            'month': queryset.filter(created_at__gte=today.date() - timezone.timedelta(days=30)),
            'year': queryset.filter(created_at__gte=today.date() - timezone.timedelta(days=365)),
        }

    @staticmethod
    def get_holders_count_dict(queryset: QuerySet('YoutubeAsset')) -> dict:
        """Apenas retorna o dict com os annotates para reuso de codigo"""
        return {
            'ZERO': queryset.annotate(youtubeassetholder_count=Count('youtubeassetholder')).filter(
                youtubeassetholder_count=0),
            'ONE': queryset.annotate(youtubeassetholder_count=Count('youtubeassetholder')).filter(
                youtubeassetholder_count=1),
            'TWO': queryset.annotate(youtubeassetholder_count=Count('youtubeassetholder')).filter(
                youtubeassetholder_count=2),
            'TWOM': queryset.annotate(youtubeassetholder_count=Count('youtubeassetholder')).filter(
                youtubeassetholder_count__gte=3),
        }

    @staticmethod
    def download_asset_update_csv(queryset):
        import csv
        from io import StringIO as IO
        from django.http import HttpResponse
        excel_file = IO()
        writer = csv.writer(excel_file, dialect='excel', delimiter=',')
        writer.writerow(
            ['asset_id', 'custom_id', 'asset_type', 'title', 'add_asset_labels', 'ownership', 'enable_content_id',
             'match_policy', 'update_all_claims'])
        for item in queryset:
            writer.writerow(
                [item.asset_id, item.custom_id, YoutubeAsset.get_youtube_asset_type_for_youtube(item.type), item.title,
                 item.get_custom_ids(), 'WW', 'Yes',
                 item.get_youtube_policy_for_youtube(item.policy), 'No'])
        response = HttpResponse(excel_file.getvalue().encode('utf-8'), content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename=asset_update.csv'
        return response


# copyright classes

class BasePercentageHolder(BaseModel):
    """Base class for Percentages Holder. Repasses"""
    holder = models.ForeignKey(verbose_name=_('Holder'), to=Holder, on_delete=models.PROTECT)
    artist = models.ForeignKey(verbose_name=_('Artist'), to=Artist, on_delete=models.PROTECT)
    percentage = models.DecimalField(verbose_name=_("Percentage"), default=100.00, decimal_places=2, max_digits=5)
    ignore_main_holder_share = models.BooleanField(verbose_name=_('Ignore Holder Share'), default=False)

    class Meta:
        """Meta options for the model"""
        abstract = True

    def get_share(self, main_holder_share):
        """Retorna o share do repasse com base na flag de ignorar o share do titular
        """
        # Retorna 100 caso o repasse deva ser feito na cabeça. Ou seja, sobre 100% do Recebimento
        return 100 if self.ignore_main_holder_share else main_holder_share


class ProductHolder(BasePercentageHolder):
    """Products Holder. """
    product = models.ForeignKey(verbose_name=_('Product'), to=Product, on_delete=models.CASCADE)

    def __str__(self):
        """str method"""
        return f'{self.holder.name} - {self.product.title}'


class AssetHolder(BasePercentageHolder):
    """Assets Holder. """
    asset = models.ForeignKey(verbose_name=_('Asset'), to=Asset, on_delete=models.CASCADE)

    def __str__(self):
        """str method"""
        return f'{self.holder.name} - {self.asset.title}'


class YoutubeAssetHolder(BasePercentageHolder):
    """Products Holder. """
    youtube_asset = models.ForeignKey(verbose_name=_('Youtube Asset'), to=YoutubeAsset, on_delete=models.CASCADE)

    def __str__(self):
        """str method"""
        return f'{self.holder.name} - {self.youtube_asset.title}'


# noinspection PyUnusedLocal
@receiver(post_save, sender=Product)
def product_post_save(sender, instance: Product, created, *args, **kwargs):
    """Product post save signal handler.

    Dispatches a task to identify financial reports items. Ajusta ordem dos Fonogramas relacionados.
    """
    if not instance.custom_id:
        instance.custom_id = instance.upc
        instance.save()
    # recheck_product_and_assets_match_dispatch(product_ids=[instance.id])

    # Confere se a lista de Assets esta com a ordem correta
    product_assets = ProductAsset.objects.filter(product=instance).order_by('order')
    if not product_assets:
        return
    correct_order = [i for i in range(1, product_assets.count() + 1)]
    for asset, order in zip(product_assets, correct_order):
        if asset.order != order:
            asset.order = order
            asset.save()


# noinspection PyUnusedLocal
@receiver(post_save, sender=ProductLegacyUPC)
def product_legacy_upc_post_save(sender, instance: ProductLegacyUPC, *args, **kwargs):
    """Product presave signal handler.

    Dispatches a task to identifiy financial reports items
    """
    recheck_product_and_assets_match_dispatch(product_ids=[instance.product_id])


# noinspection PyUnusedLocal
@receiver(post_save, sender=Asset)
def asset_post_save(sender, instance: Asset, *args, **kwargs):
    """Asset presave signal handler.

    Checks if the ISRC is used on any AssetLegacyISRC. and defaults custom_id to ISRC.
    """
    recheck_product_and_assets_match_dispatch(asset_ids=[instance.id])
    # checking and creating youtube assets
    asset_holders = instance.assetholder_set.all()
    asset_primary_artists = instance.primary_artists.all().order_by('label_catalog_asset_primary_artists.id')
    asset_types = ['sr', 'at', 'mv']
    for asset_type in asset_types:
        for youtube_asset in str(getattr(instance, "youtube_{}_asset_id".format(asset_type))).replace(",", "|").split(
                "|"):
            if youtube_asset != "":
                try:
                    YoutubeAsset.objects.get(asset_id=youtube_asset)
                except ObjectDoesNotExist:
                    new_youtube_asset = YoutubeAsset(asset_id=youtube_asset, type=asset_type.upper(),
                                                     artist=instance.get_artists_names(),
                                                     title=instance.title, main_holder_id=instance.main_holder_id,
                                                     isrc=instance.isrc,
                                                     related_asset=instance)
                    new_youtube_asset.save()
                    new_youtube_asset.primary_artists.set(asset_primary_artists)  # todo mudar a logica disso
                    new_youtube_asset.save()
                    for asset_holder in asset_holders:
                        new_youtube_asset_holder = YoutubeAssetHolder(holder=asset_holder.holder,
                                                                      artist=asset_holder.artist,
                                                                      percentage=asset_holder.percentage,
                                                                      youtube_asset=new_youtube_asset,
                                                                      ignore_main_holder_share=asset_holder.ignore_main_holder_share)
                        new_youtube_asset_holder.save()


# noinspection PyUnusedLocal
@receiver(post_save, sender=AssetLegacyISRC)
def asset_legacy_isrc_post_save(sender, instance: AssetLegacyISRC, *args, **kwargs):
    """AssetLegacyUPC postsave signal handler.

    Dispatches a task to identifiy financial reports items
    """
    recheck_product_and_assets_match_dispatch(asset_ids=[instance.asset_id])


# noinspection PyUnusedLocal
@receiver(post_save, sender=YoutubeAsset)
def youtube_asset_post_save(sender, instance: YoutubeAsset, *args, **kwargs):
    """YoutubeAsset postsave signal handler.

    Dispatches a task to identifiy financial reports items
    """
    recheck_product_and_assets_match_dispatch(youtube_asset_ids=[instance.id])


# noinspection PyUnusedLocal
@receiver(post_delete, sender=ProductProject)
def product_project_post_delete(sender, instance: ProductProject, *args, **kwargs):
    """ProductProject post_delete handler.

    Deletes the project attached
    """

    instance.project.delete()


# noinspection PyUnusedLocal
@receiver(post_save, sender=ProductProject)
def product_project_post_save(sender, instance: ProductProject, created, *args, **kwargs):
    """Envia as notificações de chegada de novo produto
    """
    if not created:
        return
    project = instance.project
    project_model = instance.project_model
    product = instance.product
    str1 = _('The material for the release of the')
    str3 = _('is available on the private drive')
    str4 = _('Release Date')
    str5 = _('All the tasks related to this project have been released.')
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
    # Não existe uma lógica bem definida para classificar um produto como grande. A regra de negócio é: se aparecer a
    #  palavra 'grande' em algum lugar do projeto ou do projeto modelo, o produto é grande.
    product_is_big = 'grande' in project.title.lower() or 'grande' in project.description.lower() or 'grande' in project_model.title.lower() or 'grande' in project_model.description.lower()
    if product_is_big:
        notification_code = SystemNotification.get_new_big_product_entry_code()
    else:
        notification_code = SystemNotification.get_new_common_product_entry_code()
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


def recheck_product_and_assets_match_dispatch(product_ids: list = "", asset_ids: list = "",
                                              youtube_asset_ids: list = "") -> None:
    """Dispara rechecagem para relatórios sempre que algum asset for salvo. """
    from music_system.apps.label_reports.tasks import recheck_items_id_reports_task
    recheck_items_id_reports_task.apply_async((product_ids, asset_ids, youtube_asset_ids),
                                              eta=timezone.now() + timezone.timedelta(seconds=15))


@shared_task
def ensure_youtube_asset_integrity_after_asset_update(asset_id):
    """Atualiza os YoutubeAssets quando o Asset for alterado"""
    asset = Asset.objects.get(id=asset_id)
    asset.update_related_youtube_assets()


auditlog.register(Product)
auditlog.register(ProductLegacyUPC)
auditlog.register(ProductProject)
auditlog.register(Asset)
auditlog.register(AssetComposer)
auditlog.register(AssetComposerLink)
auditlog.register(ProductAsset)
auditlog.register(AssetLegacyISRC)
auditlog.register(YoutubeAsset)
auditlog.register(ProductHolder)
auditlog.register(AssetHolder)
auditlog.register(YoutubeAssetHolder)

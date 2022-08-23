from typing import Tuple

from auditlog.registry import auditlog
from django.contrib import messages
from django.contrib.auth.models import User
from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.db import models
from django.urls import reverse

from django.db.models import Q, QuerySet
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils.html import format_html
from notifications.signals import notify
from post_office import mail
from rest_framework.permissions import BasePermission

from music_system.apps.contrib.log_helper import log_error, log_tests
from music_system.apps.contrib.models.admin_helpers import GetAdminUrl
from music_system.apps.contrib.models.base_model import BaseModel, get_file_path
from django.core.validators import RegexValidator, MinValueValidator
from django.utils.translation import gettext_lazy as _

from music_system.apps.notifications_helper.notification_helpers import notify_on_telegram
from music_system.apps.contrib.models.object_filterer import ObjectFilterer
from music_system.apps.contrib.validators import validate_file_max_10000, validate_file_max_15000, \
    validate_document_format, validate_image_format, validate_audio_format, validate_file_max_300000, \
    validate_file_max_2000
from music_system.apps.contrib.views.base import get_user_profile_from_request
from music_system.apps.label_catalog.forms import clean_isrc_and_upc
from music_system.apps.label_catalog.helpers import get_audio_track_for_humans_from_filefield, \
    get_thumb_with_image_download_url, default_docs_for_humans, return_validation_error_message, \
    put_string_between_li_tags, helper_get_artists_names
from music_system.apps.label_catalog.models import Holder, Artist
from music_system.apps.label_catalog.models.products import BasePercentageHolder, PRODUCT_FORMATS, PRODUCT_MEDIAS, \
    Product, AssetComposer, Asset, ProductAsset, ProductProject, ProductHolder, AssetHolder, AssetComposerLink, \
    get_audio_only_product_media_code, get_audio_and_video_product_media_code, get_video_only_product_media_code, \
    AUDIO_LANGUAGES

from music_system.apps.label_catalog.settings import VALIDATED_MESSAGE
from music_system.apps.tasks.models import ProjectModel

from django.core.files.base import ContentFile

COMMENT_TYPES = [
    ('CO', _('Comment')),
    ('PE', _('Pending')),
    ('RE', _('Disapproval')),
]

PRODUCT_GENERATION_STATUSES = (
    ('suc', _('Success')),
    ('fai', _('Failure')),
    ('non', '-')
)


def get_label_asset_audio_file_path(instance, filename):
    """Define o file_path do arquivo usando um nome aleatorio para o filename, impedindo conflitos de nome igual"""
    return get_file_path(instance, filename, 'label/tracks')


def get_cover_file_path(instance, filename):
    """Define o file_path do arquivo usando um nome aleatorio para o filename, impedindo conflitos de nome igual"""
    return get_file_path(instance, filename, 'label/covers')


def get_sticker_teaser_cover_file_path(instance, filename):
    """Define o file_path do arquivo usando um nome aleatorio para o filename, impedindo conflitos de nome igual"""
    return get_file_path(instance, filename, 'label/covers/stickers')


def get_sticker_teaser_audio_file_path(instance, filename):
    """Define o file_path do arquivo usando um nome aleatorio para o filename, impedindo conflitos de nome igual"""
    return get_file_path(instance, filename, 'label/tracks/stickers')


class LabelApprovedException(Exception):
    """Exceção usada para evitar que labels aprovadas sejam editadas"""

    def __init__(self, *args, **kwargs):
        """Init """
        Exception.__init__(self, _('An attempt to edit an approved label has been blocked'))


def get_bio_default() -> str:
    """
    Retorna o texto default da bio da label
    Returns:

    """
    return """
        <p class="font-weight-bold">Nos fale um pouco sobre a canção. Ela é autoral? Se sim, fale sobre o processo da composição. (OBS: descrever com detalhes)</p>
        <p class="font-weight-bold">Onde e quando foi realizada a gravação?</p>
        <p class="font-weight-bold">Qual o nome da diretora ou diretor de vídeo?</p>
        <p class="font-weight-bold">Qual o nome da fotografa ou fotografo que cobriu a gravação?</p>
        <p class="font-weight-bold">Como foi no dia da gravação? Tinha muitos amigos e familiares presentes?</p>
        <p class="font-weight-bold">Nos conte uma curiosidade sobre a canção.</p>
        <p class="font-weight-bold">Nos conte uma curiosidade sobre o dia da gravação.</p>
        <p class="font-weight-bold">O que você espera que essa canção cause nas pessoas?</p>
        <p class="font-weight-bold">Qual é o seu plano e objetivo com esse lançamento?</p>
        <p class="font-weight-bold">Qual versículo resume essa canção para você?</p>
    """


class IsLabelProductOrAssetOwner(BasePermission):
    def has_object_permission(self, request, view, obj):
        try:
            user_profile = get_user_profile_from_request(request)
            # Se for admin, pode ver se tiver permissão pra ver produtos
            if user_profile.user_is_staff():
                return user_profile.has_permission('label_catalog.view_labelproduct')
            # Se não for admin, só pode ver se o objeto pertencer a ele
            else:
                return obj.holder.id in user_profile.get_user_owner_holder_ids() or obj.label_creator.id == request.user.id
        except Exception:
            return False


class LabelProduct(BaseModel, GetAdminUrl):
    """LabelProduct class"""
    product = models.ForeignKey(verbose_name=_('Product'), to=Product, on_delete=models.CASCADE, null=True,
                                blank=True)
    holder = models.ForeignKey(verbose_name=_('Holder'), to=Holder, on_delete=models.CASCADE)
    upc = models.CharField(verbose_name=_('UPC/EAN'), max_length=20, null=True, blank=True,
                           # validators=[RegexValidator(regex='^(?!0)[0-9]{13}$',
                           #                            message=_(
                           #                                'UPC must have 13 digits, not start with zero (0) and is digit-only'))]
                           )
    title = models.CharField(verbose_name=_('Title'), max_length=100)
    main_interpreter = models.CharField(verbose_name=_('Main Interpreter(s)'), max_length=250)
    release_date = models.DateField(verbose_name=_('Release Date'))
    video_release_time = models.TimeField(verbose_name=_('Video Release Time'), blank=True, null=True)
    audio_release_time = models.TimeField(verbose_name=_('Audio Release Time'), blank=True, null=True,
                                          help_text=_('Works only at Google Music, Amazon Music, Deezer and Spotify'))
    audio_language = models.CharField(verbose_name=_('Audio Language'), max_length=4, choices=AUDIO_LANGUAGES,
                                      default='PT')
    type = models.CharField(max_length=3, choices=PRODUCT_FORMATS, verbose_name=_('Format'), default='ALB')
    product_media = models.CharField(verbose_name=_('Product Media'), max_length=4, choices=PRODUCT_MEDIAS,
                                     default='AUVD')
    version = models.CharField(verbose_name=_('Version'), max_length=255, blank=True, null=True,
                               help_text=_('e.g., Live, Acoustic, Playback'))
    language = models.CharField(verbose_name=_('Language'), max_length=15, blank=True, null=True,
                                default=_('Portuguese'))
    primary_artists = models.ManyToManyField(verbose_name=_('Primary Artists'), to=Artist, blank=True,
                                             related_name='label_product_primary')
    featuring_artists = models.ManyToManyField(verbose_name=_('Feat.'), to=Artist, blank=True,
                                               related_name='label_product_feat')

    holder_text = models.TextField(verbose_name=_('Copyright Notes'), blank=True, null=True,
                                   help_text=_('If the copyrights are shared in any form, specify here.'))
    extras_datasheet = models.TextField(verbose_name=_('Datasheet'), blank=True, null=True)
    extras_bio = models.TextField(verbose_name=_('Bio'), blank=True, null=True, default=get_bio_default())
    extras_notes = models.TextField(verbose_name=_('General notes'), blank=True, null=True,
                                    help_text=_('Inform here who is the asset producer, etc'))
    project_model = models.ForeignKey(verbose_name=_('Project Model'), help_text=_(
        'If you want this label to generate a project, specify the desired model here.'),
                                      blank=True, null=True, to=ProjectModel, on_delete=models.DO_NOTHING)

    approved = models.BooleanField(verbose_name=_('Approved'), default=False)
    documents = models.FileField(upload_to='label/files', verbose_name=_('Documents'), blank=True, null=True,
                                 validators=[validate_file_max_10000]
                                 , help_text=_('You can upload any documents you wish here'))
    label_creator = models.ForeignKey(to=User, verbose_name=_('Label Creator'), on_delete=models.PROTECT,
                                      blank=True, null=True)
    media_links = models.TextField(verbose_name=_('Media Urls'), blank=True, null=True,
                                   help_text=_('Insert video and other medias (if any) urls here'))
    content_delivery_dates = models.TextField(verbose_name=_('Product Receival Date'), blank=True)
    # todo thumbnail automatico
    cover = models.ImageField(verbose_name=_('Cover'), upload_to=get_cover_file_path,
                              validators=[validate_file_max_15000, validate_image_format],
                              null=True,
                              help_text=_(
                                  'Recommended size: 1500x1500 or 3000x3000. You can enter a url for the cover on the Medis URls field instead.'))

    # preview_start_time = models.TimeField(verbose_name=_('Preview Start Time'), blank=True, null=True)
    # tiktok_preview_start_time = models.TimeField(verbose_name=_('Preview Start Time'), blank=True, null=True)
    onimusic_network_comm_date = models.DateField(verbose_name=_('Onimusic Network Communication Date'), null=True,
                                                  blank=True)
    product_generation_status = models.CharField(verbose_name=_('Product Generation Status'), max_length=3,
                                                 choices=PRODUCT_GENERATION_STATUSES, default='non')
    gender = models.CharField(verbose_name=_('Gender'), max_length=25, blank=True, null=True)
    subgender = models.CharField(verbose_name=_('Subgender'), max_length=25, blank=True, null=True)
    sticker_teaser_cover = models.ImageField(verbose_name=_('Sticker Teaser Cover'),
                                             upload_to=get_sticker_teaser_cover_file_path,
                                             validators=[validate_file_max_15000, validate_image_format],
                                             blank=True, null=True,
                                             help_text=_('Recommended size: 1500x1500 or 3000x3000.'))
    sticker_teaser_audio_track = models.FileField(upload_to=get_sticker_teaser_audio_file_path,
                                                  verbose_name=_('Sticker Teaser Audio Track'),
                                                  validators=[validate_audio_format, validate_file_max_300000],
                                                  blank=True, null=True)
    copyright_text_label = models.CharField(verbose_name=_('(c) Label'), max_length=25, blank=True, null=True)

    class Meta:
        """Meta options for the model"""
        verbose_name = _('Label Product')
        verbose_name_plural = _('Label Products')

    def __str__(self):
        """str method"""
        return f'{self.title} - {self.main_interpreter} - {self.release_date.strftime("%d/%m/%Y")}'

    @property
    def get_product_generation_status(self):
        return self.get_product_generation_status_display()

    get_product_generation_status.fget.short_description = _('Product Generation Status')

    def status_for_humans(self) -> str:
        """Retorna o status da label"""
        if self.approved:
            return _('Approved')
        else:
            if self.labelcomment_set.count() > 0:
                return "{}. {}.".format(_('Pending'), _('Please check comments'))
            else:
                return _('Pending')

    status_for_humans.short_description = _('Label Status')

    # def get_staff_details_url(self):
    #     return reverse('label_catalog:label.view', kwargs={'label_id': self.id})
    #
    # def get_artist_details_url(self):
    #     return reverse('artists:artists.label.view', kwargs={'label_id': self.id})

    def cover_for_humans(self) -> str:
        """Retorna o html da miniatura da capa"""
        return get_thumb_with_image_download_url(self.cover, self.cover, 100)

    cover_for_humans.short_description = _('Cover Image')

    def docs_for_humans(self) -> str:
        """Retorna o link para os documentos da label"""
        return default_docs_for_humans(self.documents)

    docs_for_humans.short_description = _('Label Docs')

    def get_artists_names(self) -> str:
        """Concatenates all artists and feats in a string"""
        return helper_get_artists_names(
            self.primary_artists.all().order_by('label_catalog_labelproduct_primary_artists.id'),
            self.featuring_artists.all().order_by('label_catalog_labelproduct_featuring_artists.id'))[:100]

    def get_product_admin_link(self) -> str:
        """Retorna o link admin para o produto da label"""
        product = self.product
        if product is not None:
            return format_html(
                '<a href=\"{}\" target=\"_blank\">{}</a>'.format(product.get_admin_url(), _('Check Product')))

    get_product_admin_link.short_description = _('Product Admin Link')

    # def create_temp_holders_on_creation(self) -> None:
    #     """Metodo para ser chamado após criação da label, checando se ela tem repasses
    #     """
    #     pass
    # try:
    #     artist = Artist.objects.filter(name=self.holder.name).first()
    #     if not artist:
    #         raise ObjectDoesNotExist
    # except ObjectDoesNotExist:
    #     artist = Artist.objects.create(name=self.holder.name, type='IND', catalog=self.holder.catalog,
    #                                    custom_id="{}{}".format(self.holder.name[:5],
    #                                                            str(self.holder.id).zfill(5)))
    # LabelProductHolder.objects.create(
    #     holder=self.holder,
    #     artist=artist,
    #     percentage=100.00,
    #     product_id=self.id
    # )
    # for asset in self.labelasset_set.all():
    #     asset.holder = self.holder
    #     asset.save()
    #     if asset.labelassetholder_set.count() == 0:
    #         LabelAssetHolder.objects.create(
    #             holder=self.holder,
    #             artist=artist,
    #             percentage=100.00,
    #             asset_id=asset.id
    #         )

    @staticmethod
    def can_be_approved(label: 'LabelProduct') -> Tuple[str, bool]:
        """Returns true if the label can be approved, and false otherwise
        Args:
            label
        """
        validation_message: str = '<br><ol>'
        # thumbs_missing indica se ha thumbnails faltando em assets que deveriam tê-lo. usada para informar ao usuário
        #  dessa ausência sem bloquear a label
        thumbs_missing = False
        # label holders shares must be 100%
        holders = LabelProductHolder.objects.filter(product=label)
        if not label.upc:
            validation_message += return_validation_error_message(_('UPC'))
        if not label.validate_holders_list(holders):
            validation_message += return_validation_error_message(_('Royalty Splits'))
        # R.N.: Se a midia da label tiver audio, eh obrigatorio a capa
        if label.product_media in [get_audio_only_product_media_code(), get_audio_and_video_product_media_code()]:
            if not label.cover:
                validation_message += return_validation_error_message(_('Cover'), ': ' + _(
                    'label media contains audio'))
        # validating the label assets
        assets = LabelAsset.objects.filter(product=label)
        # validate each asset individually
        for asset in assets:
            # R.N.: Se o fonograma já existir no BD, é pq a label já foi aprovada anteriormente, então tá tudo certo
            if asset.asset:
                continue
            # R.N.: Se a midia do asset for apenas audio, tem que ter audio_track e nao pode ter video_cover
            if asset.media == get_audio_only_product_media_code():
                if not asset.audio_track:
                    validation_message += return_validation_error_message(_('Audio Track'), _(' of asset: {}').format(
                        asset.title) + ': ' + _('asset media contains audio'))
                if asset.video_cover:
                    validation_message += return_validation_error_message(_('Video Cover'), _(' of asset: {}').format(
                        asset.title) + ': ' + _('asset media is audio only'))
            # R.N.: Se a midia do asset for apenas video, tem que ter video_cover e nao pode ter audio_track
            if asset.media == get_video_only_product_media_code():
                if asset.audio_track:
                    validation_message += return_validation_error_message(_('Audio Track'), _(' of asset: {}').format(
                        asset.title) + ': ' + _('asset media is video only'))
                if not asset.video_cover:
                    # Não devemos invalidar a label neste caso, mas sim, apenas informar ao usuário disto
                    thumbs_missing = True
            # R.N.: Se a midia do asset for audio e video, tem que ter video_cover e audio_track
            if asset.media == get_audio_and_video_product_media_code():
                if not asset.video_cover:
                    # Não devemos invalidar a label neste caso, mas sim, apenas informar ao usuário disto
                    thumbs_missing = True
                if not asset.audio_track:
                    validation_message += return_validation_error_message(_('Audio Track'), _(' of asset: {}').format(
                        asset.title) + ': ' + _('asset media contains audio'))
        validation_message += '</ol>'
        # Se nao houver tag <li> na mensagem de validacao, significa que nenhum erro foi encontrado, entao retornamos
        #  apenas a mensagem de sucesso, indicando que a label esta valida
        if '<li>' not in validation_message:
            validation_message = VALIDATED_MESSAGE
        else:
            validation_message = format_html(validation_message)
        return validation_message, thumbs_missing

    @staticmethod
    def approve_label(request, label_id: int, author: User) -> 'LabelProduct':
        """Set the label approved status to True
        Args:
            request: HttpRequest, usado apenas para envio da mensagem de erro, caso haja
            label_id: id da label em questao
            author: quem aprovou a label
        """
        label = LabelProduct.objects.get(id=label_id)
        label.approved = True
        label.save()
        try:
            author = author
            recipients = User.objects.filter(holderuser__holder_id=label.holder.id)
            verb = _('approved label')
            action_object = label
            url = f"{reverse('artists:artists.labels')}{label.id}"
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
                        log_error('Não há um master client no sistema. Favor corrigir.')
                        author = recipients[0]
                email_description = f'{author} - {verb}: {action_object}' if action_object else f'{author} - {verb}'
                # bell notification
                notify.send(sender=author, recipient=recipients, verb=verb, action_object=action_object, url=url,
                            emailed=True, level='info')
                # todo quando o ator da notificação for um usuário, colocar o nome dele como ator pra melhorar a legibilidade

                # email notification management
                email_support = _('Any questions? Email us!')
                email_support_mail = 'SUPPORT_MAIL'
                email_site_name = 'FRONT_END__SITE_NAME'
                context = {
                    'url': email_url,
                    'email_title': email_site_name,
                    'email_subject': f'{email_site_name}',
                    'email_description': email_description,
                    'email_button_text': _('Go'),
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
                        template='level',
                        context=context,
                    )
                except ValidationError as e:
                    log_error(f'Erro ao enviar email de notificação: {e}\n')
            else:
                log_error(f'A notificação: "{verb}" não possui recipientes, e por isso não foi enviada.')
        except Exception as e:
            log_error(e)
            log_tests(e)
            messages.error(request, _('The email notification could not be sent. Contact support for info.'))

        return label

    @staticmethod
    def validate_holders_list(holders: QuerySet) -> bool:
        """Checks if there are transfers and if their percentage share is between 0 and 100%
        Args:
            holders: queryset de repasses (holders)
        """
        from _decimal import Decimal
        percentage = Decimal(0)
        if holders is not None:
            for holder in holders:
                percentage += holder.percentage
            return True if 0 <= percentage <= 100.00 else False
        return False

    @staticmethod
    def validate_asset_composers(composers: QuerySet) -> bool:
        """Checks if there are composers and if their percentage share equals 100%
        Args:
            composers: queryset de compositores
        """
        from _decimal import Decimal
        percentage = Decimal(0)
        if composers is not None:
            for composer in composers:
                percentage += composer.percentage
            return True if percentage == 100.00 else False
        return False

    @staticmethod
    def can_make_product(label: 'LabelProduct') -> Tuple[str, bool]:
        """Retorna uma mensagem de validacao informando se a label pode virar produto ou quais campos estao invalidos
        para tal.
        Args:
            label: LabelProduct
        """
        validation_message = '<br><ol>'
        # thumbs_missing indica se ha thumbnails faltando em assets que deveriam tê-lo. usada para informar ao usuário
        #  dessa ausência sem bloquear a label
        thumbs_missing = False
        # these fields are required on the Product model
        if not label.upc:
            validation_message += return_validation_error_message(_('UPC'))
        if Product.objects.filter(upc=label.upc).count() > 0:
            validation_message += put_string_between_li_tags(_('UPC already exists'))
        if not label.title:
            validation_message += return_validation_error_message(_('Title'))
        if not label.product_media:
            validation_message += return_validation_error_message(_('Media'))
        if not label.type:
            validation_message += return_validation_error_message(_('Type'))
        # label holders shares must be 100%
        holders = LabelProductHolder.objects.filter(product=label)
        if not label.validate_holders_list(holders):
            validation_message += return_validation_error_message(_('Royalty Splits'))

        # validating the label assets
        assets = LabelAsset.objects.filter(product=label)
        # validate each asset individually
        for asset in assets:
            if not asset.order:
                validation_message += return_validation_error_message(_('Asset Order'), _(' of asset: {}').format(
                    asset.title))
            # R.N.: Se a midia do asset for contiver vídeo, ele deverá ter capa
            if asset.media in [get_video_only_product_media_code(), get_audio_and_video_product_media_code()]:
                if not asset.video_cover:
                    # Não devemos invalidar a label neste caso, mas sim, apenas informar ao usuário disto
                    thumbs_missing = True
            # R.N.: Fonogramas devem, obrigatoriamente, ter isrc
            if not asset.isrc:
                validation_message += return_validation_error_message('ISRC', _(' of asset: {}').format(asset.title))
            if not asset.title:
                validation_message += return_validation_error_message(_('Title'), _(' of asset: {}').format(
                    asset.title))
            # R.N.: Se o fonograma já existir no BD, é pq a label já foi aprovada anteriormente, então tá tudo certo
            if asset.asset:
                continue
            # validate asset composers
            composers = LabelAssetComposerLink.objects.filter(asset=asset)
            if not label.validate_asset_composers(composers):
                validation_message += return_validation_error_message(_('Asset Composers'), _(' of asset: {}').format(
                    asset.title))
            # validate asset holders
            asset_holders = LabelAssetHolder.objects.filter(asset=asset)
            if not label.validate_holders_list(asset_holders):
                validation_message += return_validation_error_message(_('Asset Royalty Splits'),
                                                                      _(' of asset: {}').format(
                                                                          asset.title))

        validation_message += '</ol>'
        # Se nao houver tag <li> na mensagem de validacao, significa que nenhum erro foi encontrado, entao retornamos
        #  apenas a mensagem de sucesso, indicando que a label esta valida
        if '<li>' not in validation_message:
            validation_message = VALIDATED_MESSAGE
        else:
            validation_message = format_html(validation_message)
        return validation_message, thumbs_missing

    @staticmethod
    def make_product(label: 'LabelProduct') -> 'Product':
        """Makes a product based on a label
        Args:
            label
        """
        # definindo dict com dados da label pra criar o produto
        product_data = {
            'holder': label.holder,
            'upc': clean_isrc_and_upc(label.upc),
            'title': label.title,
            'release_date': label.release_date,
            'video_release_time': label.video_release_time,
            'audio_release_time': label.audio_release_time,
            'audio_language': label.audio_language,
            'type': label.type,
            'product_media': label.product_media,
            'version': label.version,
            'primary_artists': label.primary_artists.all().order_by('label_catalog_labelproduct_primary_artists.id'),
            'featuring_artists': label.featuring_artists.all().order_by('label_catalog_labelproduct_featuring_artists.id'),
            'holder_text': label.holder_text,
            'extras_datasheet': label.extras_datasheet,
            'extras_bio': label.extras_bio,
            'extras_notes': label.extras_notes,
            'project_model': label.project_model,
            'cover': label.cover,
            'copyright_text_label': label.copyright_text_label,
            # 'preview_start_time': label.preview_start_time,
            'onimusic_network_comm_date': label.onimusic_network_comm_date,
            'assets_link': label.media_links,
            'language': label.language,
            'approved': label.approved,
            'main_interpreter': label.main_interpreter,
            'gender': label.gender,
            'subgender': label.subgender,
        }
        # cria o produto e pega os assets da label pra criar assets do produto
        product = Product.make_new_product(product_data)
        label.product = product
        # copia capa da label para o produto
        if label.cover:
            new_product__cover = ContentFile(label.cover.read())
            new_product__cover.name = label.cover.name.split('/')[-1]
            product.cover = new_product__cover
            product.save()
            label.cover = None
            label.save()

        # copia a capa do sticker teaser
        if label.sticker_teaser_cover:
            new_product__sticker_teaser_cover = ContentFile(label.sticker_teaser_cover.read())
            new_product__sticker_teaser_cover.name = label.sticker_teaser_cover.name.split('/')[-1]
            product.sticker_teaser_cover = new_product__sticker_teaser_cover
            product.save()
            label.sticker_teaser_cover = None
            label.save()

        # copia o audio do sticker teaser
        if label.sticker_teaser_audio_track:
            new_product__sticker_teaser_audio_track = ContentFile(label.sticker_teaser_audio_track.read())
            new_product__sticker_teaser_audio_track.name = label.sticker_teaser_audio_track.name.split('/')[-1]
            product.sticker_teaser_audio_track = new_product__sticker_teaser_audio_track
            product.save()
            label.sticker_teaser_audio_track = None
            label.save()

        # cria repasses, caso hajam
        transfers = LabelProductHolder.objects.filter(product_id=label.id)
        if transfers.count() > 1:
            for product_holder in transfers:
                ProductHolder.objects.create(
                    product_id=product.id,
                    holder_id=product_holder.holder_id,
                    artist_id=product_holder.artist_id,
                    percentage=product_holder.percentage,
                    ignore_main_holder_share=product_holder.ignore_main_holder_share,
                )
        # Caso haja apenas um objeto LabelProductHolder, ele é um titular, e ja foi posto como titular. portanto este
        #  objeto se torna irrelevante por nao ser um repasse. podemos deletá-lo. Caso não haja nenhum objeto, ignore.
        elif transfers:
            transfers.delete()

        label_assets = LabelAsset.objects.filter(product=label)
        for label_asset in label_assets:
            # ve se ja existe asset com o ISRC do asset da label em questao
            try:
                # caso ja exista asset com o ISRC do label_asset, um tá ligado no outro.
                asset = label_asset.asset or Asset.objects.get(isrc=label_asset.isrc)
                asset.media = label_asset.media  # tem que alterar a mídia do fonograma pra corresponder à atualizada
                asset.save()
                label_asset.asset = asset  # liga os dois objetos pra garantir a consistência no resto do algoritmo.
                label_asset.save()
                # atualizando o campo notes do produto, informando que o Asset ja existia
                product.notes += _('Asset with ISRC: ') + asset.isrc + _(
                    ' already exists. No new Asset object created.')
                product.save()
                # define dict com dados pra criacao do novo ProductAsset (Asset ja existente com Product recem criado)
            except ObjectDoesNotExist:
                # se nao existir nenhum Asset com aquele ISRC, tem que criar um novo.
                # aqui define-se o dict com os dados pra cria-lo
                asset_data = {
                    'holder': label.holder,
                    'product': label_asset.product,
                    'title': label_asset.title,
                    'main_interpreter': label_asset.main_interpreter,
                    'asset_holder_text': label_asset.asset_holder_text,
                    'asset_composers_text': label_asset.asset_composers_text,
                    'featuring': label_asset.featuring,
                    'lyrics': label_asset.lyrics,
                    'isrc': clean_isrc_and_upc(label_asset.isrc),
                    'extras_notes': label_asset.extras_notes,
                    'valid': label_asset.valid,
                    'order': label_asset.order,
                    'primary_artists': label_asset.primary_artists.all().order_by('label_catalog_labelasset_primary_artists.id'),
                    'featuring_artists': label_asset.featuring_artists.all().order_by('label_catalog_labelasset_featuring_artists.id'),
                    'media': label_asset.media,
                    'audio_language': label_asset.audio_language,
                    'producers': label_asset.producers,
                    'version': label_asset.version,
                    'tiktok_preview_start_time': label_asset.tiktok_preview_start_time,
                    'gender': label_asset.gender,
                    'subgender': label_asset.subgender,
                }
                # cria o asset. agora tem que ligar ele no produto atraves da classe ProductAsset
                asset = Asset.make_new_asset(asset_data)
            # define o dict com os dados pra criar a relacao entre asset e produto
            product_asset_data = {
                'product': product,
                'asset': asset,
                'order': label_asset.order,
                'work_song': label_asset.work_song,
            }
            # relacionando asset com produto
            pa = ProductAsset.make_product_asset(product_asset_data)
            # copia arquivo de licencas autorais pro product asset
            if label_asset.documents:
                new_product_asset__copyright_license_file = ContentFile(label_asset.documents.read())
                new_product_asset__copyright_license_file.name = label_asset.documents.name.split('/')[-1]
                log_tests(new_product_asset__copyright_license_file.name)
                log_tests(new_product_asset__copyright_license_file)
                pa.copyright_license_file = new_product_asset__copyright_license_file
                pa.save()
                label_asset.documents = None
                label_asset.save()
            # copia capa do video para o asset e audios
            if label_asset.video_cover:
                new_asset__cover = ContentFile(label_asset.video_cover.read())
                new_asset__cover.name = label_asset.video_cover.name.split('/')[-1]
                asset.video_cover = new_asset__cover
                asset.save()
                label_asset.video_cover = None
            if label_asset.audio_track:
                if not label_asset.asset:  # Só altera o arquivo de áudio se for fonograma novo
                    new_asset__audio = ContentFile(label_asset.audio_track.read())
                    new_asset__audio.name = label_asset.audio_track.name.split('/')[-1]
                    asset.audio_track = new_asset__audio
                    asset.save()
                label_asset.audio_track = None
            if not label_asset.asset:  # Se o label_asset não tiver sido gerado a partir de um asset pré-existente
                # cria repasses dos assets, caso hajam
                asset_transfers = LabelAssetHolder.objects.filter(asset_id=label_asset.id)
                if asset_transfers.count() > 1:
                    for asset_holder in asset_transfers:
                        AssetHolder.objects.create(
                            asset_id=asset.id,
                            holder_id=asset_holder.holder_id,
                            artist_id=asset_holder.artist_id,
                            percentage=asset_holder.percentage,
                            ignore_main_holder_share=asset_holder.ignore_main_holder_share,
                        )
                # Caso haja apenas um objeto LabelAssetHolder, ele é um titular, e ja foi posto como titular. portanto
                # este objeto se torna irrelevante por nao ser um repasse. podemos deletá-lo. Caso não haja nenhum obj,
                # ignore.
                elif asset_transfers:
                    asset_transfers.delete()

                # cria compositores dos assets
                for asset_composer in LabelAssetComposerLink.objects.filter(asset_id=label_asset.id):
                    AssetComposerLink.objects.create(
                        asset_id=asset.id,
                        asset_composer_id=asset_composer.asset_composer_id,
                        percentage=asset_composer.percentage
                    )
            else:  # Se o asset tiver sido criado a partir de um asset existente, não cria mais repases nem compositores
                # ligando label_asset no asset
                label_asset.asset_id = asset.id
                label_asset.save()
        label.save()
        # se a label tem um project model especificado, gerar o projeto atraves da classe ProductProject
        if label.project_model:
            project = ProductProject(
                product=product,
                project_model=label.project_model
            )
            project.save()

        return product

    def label_actions(self, caller: str = 'front'):
        none_text = '-'
        can_approve, thumbs_missing_at_approval = self.can_be_approved(self)
        can_make_product, thumbs_missing_at_making_product = self.can_make_product(self)
        if not self.approved:
            if VALIDATED_MESSAGE in can_approve:
                if thumbs_missing_at_approval:
                    return format_html(
                        '<small style="color: red; font-size: medium;"><strong>{}: </strong>{}</small><br><a class="button default btn btn-info" href="{}">{}</a></br><small>{}</small>',
                        _('WARNING'), _('This label is missing a video cover in at least one of its assets.'),
                        reverse('label_catalog:label.approve', kwargs={'label_id': self.id, 'caller': caller}),
                        _('Approve'), _('Save any changes made before clicking the button'))
                else:
                    return format_html(
                        '<a class="button default btn btn-info" href="{}">{}</a></br><small>{}</small>',
                        reverse('label_catalog:label.approve', kwargs={'label_id': self.id, 'caller': caller}),
                        _('Approve'), _('Save any changes made before clicking the button'))
            else:
                return format_html('<span>{}</span>', can_approve)
        elif not self.product:
            if VALIDATED_MESSAGE in can_make_product:
                if thumbs_missing_at_making_product:
                    return format_html(
                        '<small style="color: red; font-size: medium;"><strong>{}: </strong>{}</small><br><a class="button default btn btn-info" href="{}">{}</a></br><small>{}</small>',
                        _('WARNING'), _('This label is missing a video cover in at least one of its assets.'),
                        reverse('label_catalog:label.into_product', kwargs={'label_id': self.id, 'caller': caller}),
                        _('Make Product'), _('Save any changes made before clicking this button'))
                else:
                    return format_html('<a class="button default btn btn-info" href="{}">{}</a></br><small>{}</small>',
                                       reverse('label_catalog:label.into_product',
                                               kwargs={'label_id': self.id, 'caller': caller}),
                                       _('Make Product'), _('Save any changes made before clicking this button'))
            else:
                return format_html('<span>{}</span>', can_make_product)
        else:
            return format_html('<span>{}</span>', none_text)

    @staticmethod
    def filter_objects_based_on_user(request_user_profile: 'Profile', queryset: QuerySet) -> QuerySet:
        """Filtra os objetos da classe para retornar somente os que pertencem ao usuario passado como parametro"""
        filters_dict = {
            'staff': Q(),
            'catalog': Q(Q(label_creator_id=request_user_profile.user.id) | Q(
                holder_id__in=request_user_profile.get_user_owner_holder_ids())),
            'holder': Q(Q(label_creator_id=request_user_profile.user.id) | Q(
                holder_id__in=request_user_profile.get_user_owner_holder_ids())),
        }
        return ObjectFilterer.filter_objects_based_on_user(request_user_profile.get_user_type(), queryset, filters_dict)

    @classmethod
    def filter_objects(cls, searched_value: str, request_user: User, values_list_fields: list = None,
                       custom_query: Q = Q(), initial_queryset: QuerySet('LabelProduct') = None) -> QuerySet:
        """Filtra os objetos da classe com base na string passada como parâmetro. Vide docstring do método chamado."""
        queryset = initial_queryset or cls.objects.all()
        search_fields = ['upc', 'title', 'version', 'type']
        # Caso o usuario tenha passado uma query como parametro, o filtro sera feito com base nela apenas
        if custom_query:
            queryset = queryset.filter(custom_query)
        return ObjectFilterer.filter_objects(cls, searched_value, request_user.user_user_profile, search_fields,
                                             queryset, values_list_fields)


class LabelComment(BaseModel):
    """Label Comment class"""
    label = models.ForeignKey(verbose_name=_('Label'), to='LabelProduct', on_delete=models.CASCADE)
    comment = models.TextField(verbose_name=_('Comment'))
    user = models.ForeignKey(verbose_name=_('User'), to=User, on_delete=models.CASCADE,
                             limit_choices_to={
                                 'is_staff': True
                             }, related_name="label_comment_user")
    type = models.CharField(verbose_name=_('Comment type'), max_length=2, choices=COMMENT_TYPES, default='CO')

    class Meta:
        """Meta options for the model"""
        verbose_name = _('Label Comment')
        verbose_name_plural = _('Label Comments')

    def __str__(self):
        """str method"""
        return "Comment"

    @property
    def user_avatar(self):
        from music_system.apps.clients_and_profiles.models import Profile
        return Profile.get_gravatar(self.user)


class LabelAsset(BaseModel):
    """Asset class"""

    product = models.ForeignKey(verbose_name=_('Product'), to=LabelProduct, on_delete=models.CASCADE)
    asset = models.ForeignKey(verbose_name=_('Asset'), to=Asset, on_delete=models.PROTECT, null=True, blank=True)
    title = models.CharField(max_length=100, verbose_name=_('Title'))
    main_interpreter = models.CharField(max_length=250, verbose_name=_('Main Interpreter(s)'), blank=True,
                                        default='')
    # todo pergutnar para nelson pai
    asset_holder_text = models.TextField(verbose_name=_('Copyright Notes'), blank=True, null=True,
                                         help_text=_(
                                             'Master shares. Only fill this field in case the master rights are shared somehow.'))
    asset_composers_text = models.TextField(verbose_name=_('Composers'),
                                            help_text=_(
                                                'State who are the composers of this asset and their percentages.'))
    featuring = models.CharField(max_length=250, blank=True, default='', verbose_name=_('Feat.'))
    lyrics = models.TextField(verbose_name=_('Lyrics'), blank=True, null=True)
    isrc = models.CharField(verbose_name=_('ISRC'), max_length=20,
                            validators=[RegexValidator(regex='^[A-Z]{2}-?\w{3}-?\d{2}-?\d{5}$',
                                                       message=_('This ISRC is invalid (format).'))])
    extras_notes = models.TextField(verbose_name=_('General notes'), blank=True, null=True,
                                    help_text=_('Inform here who is the asset producer, etc'))
    valid = models.BooleanField(verbose_name=_('Valid'), default=False)
    order = models.IntegerField(verbose_name=_('Order'), default=1, validators=[MinValueValidator(1)])
    primary_artists = models.ManyToManyField(verbose_name=_('Primary Artists'), to=Artist, blank=True,
                                             related_name='label_asset_primary')
    featuring_artists = models.ManyToManyField(verbose_name=_('Feat.'), to=Artist, blank=True,
                                               related_name='label_asset_feat')
    documents = models.FileField(upload_to='label/files', verbose_name=_('Documents'), blank=True, null=True,
                                 validators=[validate_file_max_10000]
                                 , help_text=_('You can upload any documents you wish here'))
    media = models.CharField(verbose_name=_('Media'), max_length=4, choices=PRODUCT_MEDIAS)
    audio_track = models.FileField(upload_to=get_label_asset_audio_file_path, verbose_name=_('Audio Track'),
                                   validators=[
                                       validate_audio_format,
                                       validate_file_max_300000],
                                   blank=True,
                                   null=True,
                                   help_text=_(
                                       'Max size: 300mb. Insert video link on Links to Medias field above. You can '
                                       'insert a link to the audio file on the Media URLs field instead.'))
    audio_language = models.CharField(verbose_name=_('Audio Language'), max_length=4, choices=AUDIO_LANGUAGES,
                                      default='PT')
    video_cover = models.FileField(upload_to='label/tracks_covers', verbose_name=_('Video Cover'), blank=True,
                                   null=True,
                                   validators=[validate_file_max_2000, validate_image_format],
                                   help_text=_(
                                       'Recommended size: 1280x720. You can enter a url for the video cover on the '
                                       'Media URLs field instead.'))

    media_links = models.TextField(verbose_name=_('Media Urls'), blank=True, null=True,
                                   help_text=_('Insert video and other medias (if any) urls here'))
    holder = models.ForeignKey(verbose_name=_('Holder'), to=Holder, on_delete=models.PROTECT, null=True, blank=True)
    tiktok_preview_start_time = models.TimeField(verbose_name=_('Preview Start Time (TIKTOK too)'), blank=True,
                                                 null=True)
    version = models.CharField(verbose_name=_('Version'), max_length=40, blank=True)
    producers = models.CharField(_('Producers'), max_length=250, blank=True, null=True)
    gender = models.CharField(verbose_name=_('Gender'), max_length=25, blank=True, null=True)
    subgender = models.CharField(verbose_name=_('Subgender'), max_length=25, blank=True, null=True)
    work_song = models.BooleanField(verbose_name=_('Work Song'), default=False)

    class Meta:
        """Meta options for the model"""
        verbose_name = _('Label Asset')
        verbose_name_plural = _('Label Assets')

    def __str__(self):
        """str method"""
        return '{} - {}({}) - {}'.format(self.order, self.title, self.isrc, self.main_interpreter)

    def video_cover_for_humans(self):
        """Retorna o html da capa do video"""
        if self.asset:
            return get_thumb_with_image_download_url(self.asset.video_cover, self.asset.video_cover_thumbnail, 100)
        else:
            return get_thumb_with_image_download_url(self.video_cover, self.video_cover, 100)

    def audio_track_url(self):
        """Retorna a url do audio"""
        return self.audio_track.url if self.audio_track else None

    def audio_track_for_humans(self):
        """Retorna o html do audio"""
        return get_audio_track_for_humans_from_filefield(self.audio_track)

    def docs_for_humans(self):
        """Retorna o html dos docs do fonograma"""
        return default_docs_for_humans(self.documents)

    def get_artists_names(self) -> str:
        """Concatenates all artists and feats in a string"""
        return helper_get_artists_names(
            self.primary_artists.all().order_by('label_catalog_labelasset_primary_artists.id'),
            self.featuring_artists.all().order_by('label_catalog_labelasset_featuring_artists.id'))[:100]


class LabelAssetComposerLink(BaseModel):
    """Assets composers. For reference only."""
    asset = models.ForeignKey(verbose_name=_('Asset'), to=LabelAsset, on_delete=models.CASCADE)
    asset_composer = models.ForeignKey(verbose_name=_('Composer'), to=AssetComposer, on_delete=models.CASCADE)
    percentage = models.DecimalField(verbose_name=_("Percentage"), default=100.00, decimal_places=2, max_digits=5)


class LabelProductHolder(BasePercentageHolder):
    """Products Holder."""
    product = models.ForeignKey(verbose_name=_('Product'), to=LabelProduct, on_delete=models.CASCADE)
    ignore_main_holder_share = models.BooleanField(verbose_name=_('Ignore Holder Share'), default=False)

    def __str__(self):
        """str method"""
        return self.holder.name + " - " + self.product.title


class LabelAssetHolder(BasePercentageHolder):
    """Assets Holder."""
    asset = models.ForeignKey(verbose_name=_('Asset'), to=LabelAsset, on_delete=models.CASCADE)
    ignore_main_holder_share = models.BooleanField(verbose_name=_('Ignore Holder Share'), default=False)

    def __str__(self):
        """str method"""
        return self.holder.name + " - " + self.asset.title


@receiver(post_save, sender=LabelProduct)
def notify_label_filling_by_artist(sender, instance: LabelProduct, created, *args, **kwargs):
    """ Notifica pelo Telegram quando um associado preencher uma nova label.
    """
    if not created:
        return
    # Se entrar no if abaixo, eu sei que é a criação e que foi preenchido por um artista
    if instance.label_creator.user_user_profile.user_is_holder():
        str1 = _('New label filled by')
        str2 = _('with release date set to')
        notify_on_telegram('atendimento',
                           f"{str1} {instance.label_creator.user_user_profile.get_user_owner()}: \"{instance.title}\", {str2} {instance.release_date.strftime('%d/%m/%Y')}")


# @receiver(post_save, sender=LabelComment)
# def label_comment_post_save(sender, instance: LabelComment, created, *args, **kwargs):
#     """Label Comment post save
#
#     Sends notifications to users related to the label
#     """
#     if created:
#
#         # create holders with temp artists if needed
#         queryparams = Q()
#         if instance.label.labelcomment_set.count() > 1:
#             queryparams = queryparams | Q(label_comment_user__label_id=instance.label_id)
#         if instance.label.label_creator:
#             queryparams = queryparams | Q(id=instance.label.label_creator_id)
#
#         recipients = User.objects.filter(queryparams).distinct().exclude(id=instance.user_id)
#         url = reverse('artists:artists.label.view', kwargs=dict(label_id=instance.label.id))
#
#         send_notification(author=instance.user, recipients=recipients, verb=_('commented on'), target=instance.label,
#                           url=url, email_title=_('New Comment on Label'), email_subject=_('New Comment on Label'),
#                           email_description="{}: {}.".format(_('New Comment on Label'), instance.label.title),
#                           email_template='info',
#                           send_email=True)

auditlog.register(LabelProduct)
auditlog.register(LabelComment)
auditlog.register(LabelAsset)
auditlog.register(LabelAssetComposerLink)
auditlog.register(LabelProductHolder)
auditlog.register(LabelAssetHolder)

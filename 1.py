from typing import List, Optional

from auditlog.registry import auditlog
from django.contrib.auth.models import User, Permission
from django.core.exceptions import ObjectDoesNotExist
from django.db import models, transaction
from django.db.models import Q, QuerySet
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.http import HttpRequest
from django.urls import reverse
from django.utils.html import format_html
from django.utils.translation import gettext_lazy as _
from notifications.signals import notify

from music_system.apps.clients_and_profiles.models import MasterClient
from music_system.apps.clients_and_profiles.models.notifications import SystemNotification, notify_users
from music_system.apps.contrib.log_helper import log_error, log_tests
from music_system.apps.contrib.validators import validate_image_format, validate_image_max_300
from music_system.apps.label_catalog.helpers import get_thumb_with_image_download_url, default_get_youtube_embedded
from music_system.apps.label_catalog.models import Holder
from music_system.apps.contrib.models.base_model import BaseModel
from music_system.apps.label_catalog.models import BaseCatalog
from music_system.settings.local import SITE_URL

from django.templatetags.static import static

DEFAULT_POST_FEATURED_IMAGE_URL = '{}{}'.format(SITE_URL, static('img/Logocolorida_Onimusic_960x540.png'))


class HolderUser(BaseModel):
    """HolderUser is a class to add a User to a Holder"""
    user = models.OneToOneField(verbose_name=_('User'), to=User, on_delete=models.CASCADE,
                                limit_choices_to={
                                    'is_staff': False,
                                    'cataloguser': None
                                }, )
    holder = models.ForeignKey(verbose_name=_('Holder'), to=Holder, on_delete=models.CASCADE)
    parent_holder_user = models.ForeignKey(verbose_name=_('"Parent" Holder'), to='HolderUser',
                                           on_delete=models.CASCADE, null=True, blank=True)

    class Meta:
        """Meta options for the model"""
        verbose_name = _('Holder User')
        verbose_name_plural = _('Holder User')

    def __str__(self):
        """Classe str"""
        return self.user.username

    def save(self, force_insert=False, force_update=False, using=None, update_fields=None):
        """Sobrescrita do método save para concecer permissões padrão a Titulares "pai" """
        if not self.parent_holder_user:
            permission_codenames = ['add_labelproduct', 'view_labelproduct', 'change_labelproduct',
                                    'delete_labelproduct', 'view_product', 'view_report']
            permissions_to_be_given = Permission.objects.filter(codename__in=permission_codenames).values_list('id',
                                                                                                               flat=True)
            self.user.user_permissions.set(permissions_to_be_given)
        super(HolderUser, self).save()

    @staticmethod
    def create_or_update_object(holderuser: 'HolderUser', data: dict) -> bool:
        try:
            holderuser.user_id = data.get('user_id')
            holderuser.holder_id = data.get('holder_id')
            holderuser.parent_holder_user_id = data.get('parent_holder_user_id')
            permission_codes = [
                *data.get('product_perms').split(','),
                *data.get('report_perms').split(','),
                *data.get('labelproduct_perms').split(',')
            ]
            # Na linha abaixo, precisamos do "if code" no final pro caso de virem campos de permissão vazios. Nestes
            # casos, estávamos buscando por codename vazio: ''.
            permission_ids = [Permission.objects.get(codename=code).id for code in permission_codes if code]
            # Limpa todas as permissões antes de definir as novas, para garantir consistência
            holderuser.user.user_permissions.clear()
            holderuser.user.user_permissions.set(permission_ids)
            holderuser.save()
            # Inativando/ativando o usuário do HolderUser
            holderuser.user.is_active = eval(data.get('ACTIVE'))
            holderuser.user.save()
            return True
        except Exception as e:
            log_error(e)
            return False

    @classmethod
    def atomically_delete_holderuser(cls, holderuser) -> bool:
        """
        Tenta apagar o titular usando uma transação atomica, para garantir que ou tudo ou nada seja feito. Ao tentar
        apagar um HolderUser, temos que apagar o perfil to usuario, limpar suas permissoes (para apagar os registros de
        auth_user_permissions do bd), em seguida apagar o usuario e so entao apagar o holderuser.
        Args:
            holderuser: objeto a ser apagado

        Returns: True se der tudo certo ou False caso contrário
        """
        with transaction.atomic():
            try:
                holderuser.user.user_user_profile.delete()
                holderuser.user.user_permissions.clear()
                holderuser.user.delete()
                holderuser.delete()
                return True
            except Exception as e:
                log_error(e)
                return False

    def is_parent_holder(self) -> bool:
        """
        Retorna True se o titular em questão não tiver nenhum parent (ou seja, eh o titular do artista)
        """
        return self.parent_holder_user is None

    @property
    def get_catalog_perms_as_list(self):
        return list(self.user.user_permissions.filter(codename__contains='_product').values_list('codename', flat=True))

    @property
    def get_financial_perms_as_list(self):
        return list(self.user.user_permissions.filter(codename__contains='_report').values_list('codename', flat=True))

    @property
    def get_label_perms_as_list(self):
        return list(
            self.user.user_permissions.filter(codename__contains='_labelproduct').values_list('codename', flat=True))

    get_catalog_perms_as_list.fget.short_description = _('Catalog Permissions')

    get_financial_perms_as_list.fget.short_description = _('Financial Permissions')

    get_label_perms_as_list.fget.short_description = _('Label Permissions')

    @staticmethod
    def get_available_scopes() -> List[str]:
        """
        Retorna uma lista de todos os escopos de permissão disponíveis
        """
        return ['labelproduct', 'report', 'product']

    @staticmethod
    def get_available_permissions() -> List[str]:
        """
        Retorna uma lista de todos os tipos de permissão disponíveis
        """
        return ['view', 'add', 'change', 'delete']


class CatalogUser(BaseModel):
    """CatalogUser é uma classe para ligar um usuário à um catálogo"""
    user = models.OneToOneField(verbose_name=_('User'), to=User, on_delete=models.CASCADE,
                                limit_choices_to={
                                    'is_staff': False,
                                    'holderuser': None
                                }, )
    catalog = models.ForeignKey(verbose_name=_('Catalog'), to=BaseCatalog, on_delete=models.CASCADE)

    # todo add settings and other fields
    # role
    # active
    # oauth

    class Meta:
        """Meta options for the model"""
        verbose_name = _('Record Label User')
        verbose_name_plural = _('Record Label User')

    def __str__(self):
        """str method"""
        return f'{self.user.first_name} - {self.catalog}' if self.user.first_name else f'{self.user.username} - {self.catalog}'


POST_SHOW_TO_CHOICES = (
    ('ALL', _('All')),
    ('CAT', _('Catalog Owners Only')),
    ('HOL', _('Holders Only')),
)


class BasePostModel(BaseModel):
    """Base model com campos em comum de FAQ e Post"""
    title = models.CharField(verbose_name=_('Title'), max_length=250)
    description = models.TextField(verbose_name=_('Description'))
    youtube_embedded = models.CharField(verbose_name=_('Youtube Video ID'), null=True, blank=True,
                                        help_text=_('Video ID to embbed a youtube video'), max_length=100)
    featured = models.BooleanField(verbose_name=_('Featured'), default=False)
    order = models.IntegerField(verbose_name=_('Order'), default=0)

    show_to = models.CharField(verbose_name=_('Show To'), help_text=_('Chose what type of user should see this.'),
                               max_length=3, choices=POST_SHOW_TO_CHOICES, default='ALL')

    client = models.ForeignKey(verbose_name=_('Master Client'), on_delete=models.PROTECT, to=MasterClient,
                               help_text=_('Leave blank to show to all Master Clients.'), null=True, blank=True)

    class Meta:
        """Meta options for the model"""
        abstract = True

    def get_youtube_embedded(self):
        """Retorna o html formatado do youtube embedado para o front"""
        return default_get_youtube_embedded(self.youtube_embedded)

    @staticmethod
    def get_show_to_all_code():
        return 'ALL'

    @staticmethod
    def get_show_to_holder_code():
        return 'HOL'

    @staticmethod
    def get_show_to_catalog_code():
        return 'CAT'

    @staticmethod
    def get_show_to_codes_list(user_is_catalog: bool = False) -> List[str]:
        """
        Retorna uma lista contendo os códigos indicando quem pode ver o post/faq
        Args:
            user_is_catalog: bool indicando se o usuario eh cataloguser

        Returns:
            Lista de códigos indicando quem pode ver o post/faq
        """
        return [BasePostModel.get_show_to_all_code(),
                BasePostModel.get_show_to_catalog_code() if user_is_catalog else BasePostModel.get_show_to_holder_code()]

    def get_self_query(self, user_profile: 'Profile', item_id: bool = None) -> Optional[QuerySet]:
        if item_id:
            query = self.complete_self_query(self.objects.all(), user_profile)
            try:
                return query.get(id=item_id)
            except ObjectDoesNotExist:
                return None
        else:
            return self.complete_self_query(self.objects.all(), user_profile)

    @staticmethod
    def complete_self_query(query: QuerySet, user_profile: 'Profile') -> QuerySet:
        """
        Retorna apenas os posts/faqs que o usuario do perfil passado como parametro pode ver
        Args:
            query: queryset de posts a serem filtrados
            user_profile: perfil do usuario em questao

        Returns:
            Queryset de posts/faqs filtrados por permissão de visualização
        """
        if user_profile.user_is_staff():
            return query
        return query.filter(
            show_to__in=FAQ.get_show_to_codes_list(user_profile.user_is_catalog())
        ).exclude(
            Q(client__isnull=False) & ~Q(
                client=user_profile.get_user_catalog().master_client))


class FAQCategory(BaseModel):
    """FAQ Category"""
    title = models.CharField(verbose_name=_('Title'), max_length=250)
    order = models.IntegerField(verbose_name=_('Order'), default=0)

    class Meta:
        """Meta options for the model"""
        verbose_name = _('FAQ Category')
        verbose_name_plural = _('FAQ Categories')

    def __str__(self):
        """str method"""
        return self.title


class FAQ(BasePostModel):
    """FAQ"""
    category = models.ForeignKey(verbose_name=_('Category'), to=FAQCategory, on_delete=models.PROTECT)

    class Meta:
        """Meta options for the model"""
        verbose_name = _('FAQ')
        verbose_name_plural = _('FAQs')

    def __str__(self):
        """str method"""
        return self.title


@receiver(post_save, sender=FAQ)
def faq_post_save(sender, instance, created, *args, **kwargs):
    """FAQ postsave signal handler.

    Sends a notification to all artists when a new faq is posted
    """
    if created:
        recipients = User.objects.filter(user_user_profile__is_artist=True)
        author = instance
        verb = _('FAQ was posted')
        url = reverse('artists:artists.faqs')
        action_object = instance
        if len(recipients) > 0:
            if author is None:
                author = recipients[0].user_user_profile.get_default_system_master_client()
                # No caso extremo de não haver um master client no sistema, colocamos um autor qualquer
                if not author:
                    log_error('Não há um master client no sistema. Favor corrigir.')
                    author = recipients[0]
                notify.send(sender=author, recipient=recipients, verb=verb, action_object=action_object, url=url,
                            emailed=False, level='info')


class PostCategory(BaseModel):
    """Post Category"""
    title = models.CharField(verbose_name=_('Title'), max_length=250)

    class Meta:
        """Meta options for the model"""
        verbose_name = _('Post Category')
        verbose_name_plural = _('Post Categories')

    def __str__(self):
        """str method"""
        return self.title


class Post(BasePostModel):
    """Post for news"""
    # todo implementar created_by
    slug = models.CharField(verbose_name=_('Slug'), help_text=_('Slug field is used to create the post`s url.'),
                            max_length=250, unique=True, blank=True, null=True)
    category = models.ForeignKey(verbose_name=_('Category'), to=PostCategory, on_delete=models.PROTECT)
    featured_image = models.ImageField(verbose_name=_('Featured Image'), upload_to='posts/covers/', blank=True,
                                       null=True, validators=[validate_image_format, validate_image_max_300],
                                       help_text=str(_('Scale')) + ': 960x540; ' + str(_('Max size')) + ': 300kb; ' + str(_(
                                           'Only .jpeg, .jpg and .png formats are allowed.')))
    sub_categories = models.ManyToManyField(verbose_name=_('Subcategories'), to=PostCategory,
                                            related_name='post_subcategories', blank=True)

    class Meta:
        """Meta options for the model"""
        ordering = ['-created_at']

    def __str__(self):
        """str method"""
        return self.title

    def get_featured_image(self):
        """Retorna a imagem destaque do post"""
        return get_thumb_with_image_download_url(self.featured_image, self.featured_image, 540)

    def get_featured_image_url(self):
        """Retorna a url da imagem destaque caso haja, se nao houver retorna a url da imagem padrao"""
        if self.featured_image:
            return self.featured_image.url
        else:
            return DEFAULT_POST_FEATURED_IMAGE_URL


@receiver(post_save, sender=Post)
def post_post_save(sender, instance, created, *args, **kwargs):
    """Post post save signal handler.

    Set a random slug if none is provided. Sends notification to artists
    """
    if created:
        recipients = User.objects.filter(user_user_profile__is_artist=True)
        author = instance
        verb = _('FAQ was posted')
        url = reverse('artists:artists.faqs')
        action_object = instance
        if len(recipients) > 0:
            if author is None:
                author = recipients[0].user_user_profile.get_default_system_master_client()
                # No caso extremo de não haver um master client no sistema, colocamos um autor qualquer
                if not author:
                    log_error('Não há um master client no sistema. Favor corrigir.')
                    author = recipients[0]
                notify.send(sender=author, recipient=recipients, verb=verb, action_object=action_object, url=url,
                            emailed=False, level='info')
    if instance.slug is None or instance.slug == '':
        import uuid
        instance.slug = uuid.uuid4()
        instance.save()


@receiver(post_save, sender=HolderUser)
def holder_user_post_save(sender, instance: HolderUser, created, *args, **kwargs):
    """Post post save signal handler.

    Sets the profile created to the artist one. Sends notification to staff users
    """
    if created:
        from music_system.settings.base import FRONT_END__SITE_NAME
        from music_system.settings.base import SUPPORT_MAIL
        from post_office import mail
        holder_user = User.objects.get(id=instance.user.id)
        holder_user.user_user_profile.is_artist = True
        holder_user.user_user_profile.save()
        try:
            context = dict()
            context['url'] = '{}{}'.format(SITE_URL, reverse('dashboard:index'))
            context['email_title'] = _('Welcome mail')
            context['email_subject'] = _('Welcome to the family!')
            context['email_description'] = _('Your profile has been created! Click the button below to access it')
            context['email_button_text'] = _('Go')
            context['email_support'] = _('Any questions? Email us!')
            context['email_support_mail'] = SUPPORT_MAIL
            context['email_site_name'] = FRONT_END__SITE_NAME
            context['email_logo'] = instance.user.user_user_profile.get_master_client_email_logo_url()
            context['email_master_client_name'] = instance.user.user_user_profile.get_master_client().name
            mail.send(
                holder_user.email,
                template='info',
                context=context,
            )
        except Exception as e:
            log_error(e)
        # Pegando o código e recipientes da notificação de entrada de novo associado para dispará-la
        notification_code = SystemNotification.get_new_associated_entry_code()
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


auditlog.register(HolderUser)
auditlog.register(CatalogUser)
auditlog.register(FAQCategory)
auditlog.register(FAQ)
auditlog.register(Post)
auditlog.register(PostCategory)

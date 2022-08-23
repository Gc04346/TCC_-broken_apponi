from django import forms
from django.contrib.auth.models import User
from django.core.exceptions import ObjectDoesNotExist, NON_FIELD_ERRORS
from django.forms import inlineformset_factory
from django.urls import reverse
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

from music_system.apps.clients_and_profiles.models.notifications import SystemNotification, notify_users
from music_system.apps.contrib.log_helper import log_tests, log_error
from music_system.apps.notifications_helper.notification_helpers import notify_on_telegram
from music_system.apps.label_catalog.models import Asset, AssetLegacyISRC, ProductLegacyUPC, Product, ProductHolder, \
    ProductAsset, AssetHolder, Holder, YoutubeAssetBulk
from music_system.apps.label_catalog.models.products import AssetComposerLink, YoutubeAsset, YoutubeAssetHolder, \
    AssetComposer, ProductProject
from music_system.apps.label_catalog.widgets import DynamicSelect2CustomWidget


def notify_product_changes(form):
    if any(field in form.changed_data for field in ['primary_artists', 'featuring_artists']):
        microphone_emoji = bytes.decode(b'\xF0\x9F\x8E\xA4', 'utf-8')
        str1 = _('Interpreters have changed on')
        try:
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
            product = form.instance
            urgency = 'warning' if product.date_release - timezone.now().date() < timezone.timedelta(days=8) else 'info'
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
        except Exception as e:
            log_error(e)


def notify_asset_changes(form):
    if any(field in form.changed_data for field in ['primary_artists', 'featuring_artists']):
        microphone_emoji = bytes.decode(b'\xF0\x9F\x8E\xA4', 'utf-8')
        str1 = _('Interpreters have changed on')
        try:
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


class CopyrightInlineFormset(forms.models.BaseInlineFormSet):
    """Validacao de porcentagens nos repasses"""

    @staticmethod
    def get_owner_text():
        return _('holder')

    def clean(self):
        # get forms that actually have valid data and is not delete
        super(CopyrightInlineFormset, self).clean()
        # count = 0
        percentage_total = 0
        for form in self.forms:
            try:
                is_delete = False
                if len(form.changed_data):
                    is_delete = form.changed_data[0] == 'DELETE'
                if form.cleaned_data and not is_delete:
                    # verifica se o valor de uma porcentagem esta entre 0 e 100
                    percentage = form.cleaned_data.get('percentage', 0)
                    if not (0 <= percentage <= 100):
                        form.add_error(field='percentage',
                                       error=forms.ValidationError(
                                           _('Each percentage value must be between 0 and 100.')))
                    percentage_total += percentage
                    # count += 1
            except AttributeError:
                pass
        for form in self.forms:
            # if count < 1:
            #     # verifica se ha ao menos um titular
            #     form.add_error(field=NON_FIELD_ERRORS,
            #                    error=forms.ValidationError(
            #                        _('You must have at least one %(owner)s.') % {'owner': self.get_owner_text()}))
            if not 0 <= percentage_total <= 100.00:
                # verifica se a soma das porcentagens esta entre 0 e 100
                form.add_error(field=NON_FIELD_ERRORS,
                               error=forms.ValidationError(_('The percentages sum must be between 0 and 100.')))


ProductHolderInline = inlineformset_factory(Product, ProductHolder, formset=CopyrightInlineFormset,
                                            fields=(
                                                'holder', 'artist', 'percentage', 'ignore_main_holder_share',
                                            ), extra=1, can_delete=True,
                                            widgets={
                                                'holder': DynamicSelect2CustomWidget(),
                                                'artist': DynamicSelect2CustomWidget(),
                                            }
                                            )


class BaseYoutubeAssetHolderFormset(CopyrightInlineFormset):
    pass


YoutubeAssetHolderInline = inlineformset_factory(YoutubeAsset, YoutubeAssetHolder,
                                                 formset=BaseYoutubeAssetHolderFormset,
                                                 exclude=(), extra=0)


class YoutubeAssetBulkForm(forms.ModelForm):
    class Meta:
        model = YoutubeAssetBulk
        fields = ['file', 'mode']


class ComposerInlineFormset(CopyrightInlineFormset):
    @staticmethod
    def get_owner_text():
        return _('composer')


class ProductFrontForm(forms.models.ModelForm):
    class Meta:
        """Meta options for the form"""
        model = Product
        fields = [
            'main_holder',
            'custom_id',
            'upc',
            'date_release',
            'date_divulgation',
            'time_release',
            'audio_release_time',
            'audio_language',
            'date_recording',
            'date_video_received',
            'date_audio_received',
            'title',
            'release_type',
            'assets_link',
            'version',
            'media',
            'format',
            'gender',
            'subgender',
            'copyright_text_label',
            'primary_artists',
            'featuring_artists',
            'ready_for_delivery',
            'delivery_started',
            'delivery_finished',
            'delivery_notes',
            'active',
            'notes',
            'notes_ads',
            'cover',
            'cover_thumbnail',
            'dsp_itunes_id',
            'dsp_spotify_id',
            'dsp_youtube_id',
            'dsp_chartmetric_id',
            # 'preview_start_time',
            'onimusic_network_comm_date',
            'sticker_teaser_cover',
            'sticker_teaser_audio_track',
        ]
        widgets = {
            'main_holder': forms.Select(attrs={'class': 'select2_dynamic'}),
            'assets_link': forms.Textarea(attrs={'class': 'summernote'}),
            'delivery_notes': forms.Textarea(attrs={'class': 'summernote'}),
            'notes': forms.Textarea(attrs={'class': 'summernote'}),
            'notes_ads': forms.Textarea(attrs={'class': 'summernote'}),
            'date_release': forms.TextInput(attrs={'type': 'date'}),
            'date_video_received': forms.TextInput(attrs={'type': 'date'}),
            'date_audio_received': forms.TextInput(attrs={'type': 'date'}),
            'time_release': forms.TextInput(attrs={'type': 'time'}),
            'audio_release_time': forms.TextInput(attrs={'type': 'time'}),
            'date_recording': forms.TextInput(attrs={'type': 'date'}),
            'onimusic_network_comm_date': forms.TextInput(attrs={'type': 'date'}),
            'media': forms.Select(attrs={'class': 'select2'}),
            'format': forms.Select(attrs={'class': 'select2'}),
            'primary_artists': forms.SelectMultiple(attrs={'class': 'select2'}),
            'featuring_artists': forms.SelectMultiple(attrs={'class': 'select2'}),
            'release_type': forms.Select(attrs={'class': 'select2'}),
            'projects': forms.SelectMultiple(),  # attrs={'class': 'select2'} # todo remover
            # 'preview_start_time': forms.TimeInput(attrs={'type': 'time'}),
        }

    def __init__(self, *args, **kwargs):
        super(ProductFrontForm, self).__init__(*args, **kwargs)
        holders = Holder.objects.all().prefetch_related('catalog')
        self.fields['main_holder'].queryset = holders

    def clean(self):
        super(ProductFrontForm, self).clean()
        notify_product_changes(self)
        self.cleaned_data['upc'] = clean_isrc_and_upc(self.cleaned_data['upc'])
        try:
            ProductLegacyUPC.objects.get(upc=self.cleaned_data['upc'])
            #     if no error was raised, a legacy upc was found
            raise forms.ValidationError(_("UPC is already used for legacy reference."))
        except ObjectDoesNotExist:
            pass


class ApiProductForm(forms.ModelForm):
    class Meta:
        model = Product
        exclude = ('projects', 'custom_id')

    def clean(self):
        super(ApiProductForm, self).clean()
        upc = self.cleaned_data.get('upc', None)
        notify_product_changes(self)
        if not upc:
            self.add_error('upc', forms.ValidationError(_("UPC is mandatory.")))
        self.cleaned_data['upc'] = clean_isrc_and_upc(upc)
        try:
            ProductLegacyUPC.objects.get(upc=self.cleaned_data.get('upc'))
            #     if no error was raised, a legacy upc was found
            self.add_error('upc', forms.ValidationError(_("UPC is already used for legacy reference.")))
        except ObjectDoesNotExist:
            pass


class ProductForm(forms.models.ModelForm):
    def clean(self):
        super(ProductForm, self).clean()
        notify_product_changes(self)
        self.cleaned_data['upc'] = clean_isrc_and_upc(self.cleaned_data['upc'])
        try:
            ProductLegacyUPC.objects.get(upc=self.cleaned_data['upc'])
            #     if no error was raised, a legacy upc was found
            raise forms.ValidationError(_("UPC is already used for legacy reference."))
        except ObjectDoesNotExist:
            pass


class LegacyProductForm(forms.models.BaseInlineFormSet):
    def clean(self):
        super(LegacyProductForm, self).clean()
        for form in self.forms:
            try:
                if form.cleaned_data['legacy_type'] and form.cleaned_data['legacy_type_other']:
                    form.add_error('legacy_type_other',
                                   forms.ValidationError(_('Fields "Type" and "Type (other)" are mutually exclusive')))
            except KeyError:
                pass
            try:
                Product.objects.get(upc=form.cleaned_data['upc'])
                #     if no error was raised, upc was found
                form.add_error('upc', forms.ValidationError(_("UPC is already used on a Product.")))
            except (ObjectDoesNotExist, KeyError):
                pass


ProductLegacyUPCFrontInline = inlineformset_factory(Product, ProductLegacyUPC, formset=LegacyProductForm,
                                                    fields=(
                                                        'upc', 'legacy_type', 'legacy_type_other'
                                                    ), extra=1, can_delete=True
                                                    # widgets={
                                                    #     'legacy_type': forms.Select(attrs={'class': 'select2'}),
                                                    # }
                                                    )


class AssetForm(forms.models.ModelForm):
    def clean(self):
        notify_asset_changes(self)
        isrc = self.cleaned_data.get('isrc', None)
        if not isrc:
            self.add_error(field='isrc', error=forms.ValidationError(_("ISRC field is mandatory.")))
        # Se existir isrc, podemos acessá-lo com os brackets pra colocá-lo "limpo" no cleaned data pra ser salvo assim
        isrc = clean_isrc_and_upc(isrc)
        self.cleaned_data['isrc'] = isrc
        if Asset.objects.filter(isrc=isrc).exclude(id=self.instance.id).exists():
            self.add_error(field='isrc', error=forms.ValidationError(_("ISRC already exists.")))
        try:
            AssetLegacyISRC.objects.get(isrc=isrc)
            #     if no error was raised, a legacy isrc was found
            self.add_error(field='isrc', error=forms.ValidationError(_("ISRC is already used for legacy reference.")))
        except ObjectDoesNotExist:
            pass


class AssetFrontForm(AssetForm):
    class Meta:
        """Meta options for the form"""
        model = Asset
        fields = [
            'main_holder',
            'isrc',
            'title',
            'version',
            'media',
            'gender',
            'subgender',
            'producers',
            'copyright_text_label',
            'youtube_video_id',
            'youtube_at_asset_id',
            'youtube_sr_asset_id',
            'youtube_mv_asset_id',
            'youtube_composition_asset_id',
            'youtube_label_done',
            'youtube_publishing_done',
            'primary_artists',
            'featuring_artists',
            'publishing_id',
            'publishing_title',
            'publishing_version',
            'publishing_status',
            'publishing_comments',
            'publishing_custom_code_1',
            'active',
            'audio_language',
            'video_cover',
            'video_cover_thumbnail',
            'tiktok_preview_start_time',
        ]
        widgets = {
            'main_holder': forms.Select(attrs={'class': 'select2_dynamic'}),
            'media': forms.Select(attrs={'class': 'select2', 'name': 'asset_media_type_choices'}),
            'primary_artists': forms.SelectMultiple(attrs={'class': 'select2'}),
            'featuring_artists': forms.SelectMultiple(attrs={'class': 'select2'}),
            'publishing_status': forms.Select(attrs={'class': 'select2'}),
            'publishing_comments': forms.Textarea(attrs={'class': 'summernote'}),
            'publishing_custom_code_1': forms.Textarea(attrs={'class': 'summernote'}),
            'tiktok_preview_start_time': forms.TextInput(attrs={'type': 'time'}),
        }

    def clean(self):
        super(AssetFrontForm, self).clean()
        notify_asset_changes(self)


class AssetModalForm(forms.models.ModelForm):
    class Meta:
        """Meta options for the form"""
        model = Asset
        fields = [
            'youtube_video_id',
            'youtube_at_asset_id',
            'youtube_sr_asset_id',
            'youtube_mv_asset_id',
            'youtube_composition_asset_id',
            'youtube_label_done',
            'youtube_publishing_done',
            'publishing_id',
            'publishing_title',
            'publishing_version',
            'publishing_status',
            'publishing_comments',
            'publishing_custom_code_1',
            'tiktok_preview_start_time',
        ]
        widgets = {
            'publishing_status': forms.Select(attrs={'class': 'select2'}),
            'publishing_comments': forms.Textarea(attrs={'class': 'summernote'}),
            'publishing_custom_code_1': forms.Textarea(attrs={'class': 'summernote'}),
            'tiktok_preview_start_time': forms.TextInput(attrs={'type': 'time'}),
        }

    def clean(self):
        super(AssetModalForm, self).clean()
        notify_asset_changes(self)


def single_true(iterable):
    i = iter(iterable)
    return any(i) and not any(i)


class ProductAssetFrontFormset(forms.models.BaseInlineFormSet):

    def clean(self):
        """
        Este clean irá garantir a regra de negócio que dita que todos os produtos no formato Álbum ou EP devem ter uma
        (e apenas uma) música de trabalho.
        """
        super(ProductAssetFrontFormset, self).clean()
        if len(self.forms) > 1:  # Se o produto tiver mais de uma música, deve-se informar qual é a música de trabalho.
            songs_marked_as_work_song = [form.cleaned_data.get('work_song') for form in self.forms]
            if not any(songs_marked_as_work_song):  # Caso onde nenhuma música foi marcada como a de trabalho
                self.forms[0].add_error('work_song',
                                        str(_('This product is an Album or EP. You must indicate the work song.')))
            if not single_true(songs_marked_as_work_song):  # Caso onde mais de uma música foi marcada como de trabalho.
                for form in self.forms:
                    if form.cleaned_data.get('work_song'):
                        form.add_error('work_song', str(_('There can be only one work song.')))


ProductAssetFrontInline = inlineformset_factory(Product, ProductAsset, formset=ProductAssetFrontFormset,
                                                fields=(
                                                    'asset', 'order', 'work_song',
                                                ), extra=1, can_delete=True,
                                                widgets={
                                                    'asset': DynamicSelect2CustomWidget(),
                                                }
                                                )


class ProductProjectFrontForm(forms.ModelForm):
    class Meta:
        """Meta options for the form"""
        model = ProductProject
        verbose_name = _('Project')
        verbose_name_plural = _('Projects')
        exclude = ('project',)


ProductProjectInline = inlineformset_factory(Product, ProductProject, form=ProductProjectFrontForm,
                                             fields=('project_model',), extra=1, can_delete=True)


class ProductHolderFrontForm(CopyrightInlineFormset):
    class Meta:
        """Meta options for the form"""
        model = ProductHolder
        exclude = ()


ProductHolderFrontInline = inlineformset_factory(Product, ProductHolder, formset=ProductHolderFrontForm,
                                                 fields=(
                                                     'holder', 'artist', 'percentage', 'ignore_main_holder_share'
                                                 ), extra=0, can_delete=True)

AssetHolderInline = inlineformset_factory(Asset, AssetHolder, formset=CopyrightInlineFormset,
                                          fields=(
                                              'holder', 'artist', 'percentage', 'ignore_main_holder_share'
                                          ), extra=1, can_delete=True,
                                          )


class AddComposerForm(forms.ModelForm):
    class Meta:
        """Meta options for the form"""
        model = AssetComposer
        exclude = ()


AssetComposerInline = inlineformset_factory(Asset, AssetComposerLink, formset=CopyrightInlineFormset,
                                            fields=(
                                                'asset_composer', 'percentage'
                                            ), extra=1, can_delete=True
                                            )


class LegacyAssetFormset(forms.models.BaseInlineFormSet):
    def clean(self):
        for form in self.forms:
            if form.cleaned_data.get('legacy_type', False) and form.cleaned_data.get('legacy_type_other', False):
                form.add_error('legacy_type_other',
                               forms.ValidationError(_('Fields "Type" and "Type (other)" are mutually exclusive')))
            try:
                Asset.objects.get(isrc=form.cleaned_data['isrc'])
                #     if no error was raised, upc was found
                form.add_error('isrc', forms.ValidationError(_("ISRC is already used on an Asset.")))
            except ObjectDoesNotExist:
                pass
            except KeyError:
                pass


LegacyISRCFrontInline = inlineformset_factory(Asset, AssetLegacyISRC, formset=LegacyAssetFormset,
                                              fields=(
                                                  'isrc', 'legacy_type', 'legacy_type_other',
                                              ), extra=1, can_delete=True
                                              )


def clean_isrc_and_upc(isrc):
    return str(isrc).replace(" ", "").replace("-", "").replace(".", "")


class HolderFrontForm(forms.ModelForm):
    email = forms.EmailField(required=False)

    class Meta:
        """Meta options for the form"""
        model = Holder
        fields = [
            'name',
            'representative',
            'name_short',
            'catalog',
            'type',
            'link',
            'legal_name',
            'legal_document',
            'legal_document_type',
            'bank_data',
            'tax_retention',
            'share',
            'share_youtube',
            'active',
            'contract_start',
            'contract_end',
            'notes',
            'remarketing_pixel',
            'profile_spotify_url',
            'profile_deezer_url',
            'profile_google_url',
            'profile_apple_url',
            'profile_youtube_url',
            'profile_facebook_url',
            'profile_twitter_url',
            'profile_instagram_url',
            'profile_extra_1_url',
            'profile_extra_2_url',
            'profile_notes',
            'tax_retention',
            'dsp_itunes_id',
            'dsp_spotify_id',
            'dsp_youtube_id',
            'dsp_chartmetric_id',
            'impact',
            'tooltor_id',
        ]
        widgets = {
            'active': forms.HiddenInput(),
            'catalog': forms.HiddenInput(),
        }


class YoutubeAssetForm(forms.ModelForm):
    class Meta:
        model = YoutubeAsset
        exclude = ()

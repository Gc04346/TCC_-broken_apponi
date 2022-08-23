from typing import List

from auditlog.registry import auditlog
from django.contrib.auth.models import User
from django.db import models
from django.db.models import Q, Avg, F, QuerySet
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.urls import reverse
from django.utils import timezone
from django.utils.html import format_html

from music_system.apps.clients_and_profiles.models.notifications import SystemNotification, notify_users
from music_system.apps.contrib.log_helper import log_tests
from music_system.apps.contrib.models.admin_helpers import GetAdminUrl
from music_system.apps.contrib.models.base_model import BaseModel, BaseApiDataClass
from django.utils.translation import gettext_lazy as _
from datetime import timedelta

from music_system.apps.clients_and_profiles.models.base import Profile
from music_system.apps.contrib.models.object_filterer import ObjectFilterer
from music_system.apps.contrib.string_helpers import return_mark_safe
from music_system.apps.label_catalog.helpers import default_query_assets_by_args
from music_system.apps.label_catalog.models import BaseCatalog

PROJECT_ORDER_COLUMN_CHOICES = ['title', 'num_tasks',
                                'start_date', 'updated_at']  # lista que corresponde a ordem das colunas no datatables

PROJECT_MODEL_ORDER_COLUMN_CHOICES = ['title', 'num_tasks']  # lista que corresponde a ordem das colunas no datatables

TASK_MODEL_ORDER_COLUMN_CHOICES = ['title','days_after_start','group','assigned_to',]  # lista que corresponde a ordem das colunas no datatables


def default_task_objects_filter(filtered_class, searched_value: str, request_user: User,
                                values_list_fields: list = None, custom_query: Q = Q()) -> QuerySet:
    """Centraliza os métodos de filtro das classes de tarefa, já que são similares. Vide docstring do método chamado."""
    queryset = filtered_class.objects.all()

    search_fields = ['title']
    # Caso o usuario tenha passado uma query como parametro, o filtro sera feito com base nela apenas
    if custom_query:
        queryset = queryset.objects.filter(custom_query)
    return ObjectFilterer.filter_objects(filtered_class, searched_value, request_user.user_user_profile, search_fields,
                                         queryset, values_list_fields)


class Project(BaseModel):
    """Project is the mother class for this taks app."""
    catalog = models.ForeignKey(verbose_name=_('Catalog'), to=BaseCatalog, on_delete=models.SET_NULL, null=True,
                                blank=True)
    title = models.CharField(verbose_name=_('Title'), max_length=200)
    description = models.TextField(verbose_name=_('Description'), blank=True, null=True, default=" ")
    start_date = models.DateField(verbose_name=_('Start Date'))
    model = models.ForeignKey(verbose_name=_('Model'), to='ProjectModel', on_delete=models.SET_NULL, blank=True,
                              null=True)

    class Meta:
        verbose_name = _('Project')
        verbose_name_plural = _('Projects')
        permissions = [('can_postpone_project', _('Can Postpone Project'))]

    def __str__(self):
        """str method"""
        return self.title

    def get_data_for_api(self):
        """Get project data for api responses"""
        data = dict(
            catalog=self.catalog_id,
            title=self.title,
            description=self.description,
            start_date=self.start_date.strftime('%d/%m/%Y'),
            updated_at=self.updated_at.strftime('%d/%m/%Y'),
            num_tasks=self.count_tasks(),
        )
        return data

    def postpone_for_n_days(self, postpone_days: int):
        """Adia ou adiante o projeto em [postpone_days]] dias, adia todas tarefas tambem.

        Retorna None em caso de sucesso. Em todos outros casos retorna erro.
        """
        # noinspection PyBroadException
        try:
            self.start_date = self.start_date + timedelta(days=postpone_days)
            for task in self.task_project.all():
                task.due_date = task.due_date + timedelta(days=postpone_days)
                task.save()
            self.save()
            return None
        except Exception as e:
            return str(e)

    @staticmethod
    def get_column_order_choices() -> List[str]:
        """Retorna o dicionario com as colunas do datatables em que os produtos podem ser ordenados"""
        return PROJECT_ORDER_COLUMN_CHOICES

    @staticmethod
    def query_products_by_args(request) -> dict:
        """
            Metodo usado pela api do DataTables para buscar dinamicamente por produtos com base na caixa de busca
            Args:
                request: request da api
            Returns:
                dict contendo a queryset de produtos e outras informacoes relevantes ao DataTables
        """
        return default_query_assets_by_args(request, Project)

    def get_project_valid_tasks(self):
        """retorna um queryset com todas tarefas nao canceladas do projeto
        """
        return self.task_project.exclude(status=Task.get_canceled_status_code())

    def count_tasks(self):
        return self.get_project_valid_tasks().count()

    count_tasks.short_description = _("# of Tasks")

    def get_tasks_past_due_date(self):
        return self.get_project_valid_tasks().filter(Q(
            Q(status=Task.get_done_status_code(), due_date__lt=F('done_date')) |
            Q(~Q(status=Task.get_done_status_code()) & Q(due_date__lt=timezone.now()))
        ))

    def count_past_due_tasks(self):
        return self.get_tasks_past_due_date().count()

    count_past_due_tasks.short_description = _("# of Past Due Tasks")

    def count_past_due_not_on_hold_tasks(self):
        return self.get_tasks_past_due_date().filter(~Q(status=Task.get_onhold_status_code())).count()

    def get_done_tasks(self):
        return self.get_project_valid_tasks().filter(status=Task.get_done_status_code(), due_date__isnull=False,
                                                     done_date__isnull=False)

    def count_done_tasks(self):
        return self.get_done_tasks().count()

    count_done_tasks.short_description = _("# of Done Tasks")

    def get_done_tasks_count_for_humans(self):

        return "%s/%s" % (self.count_done_tasks(), self.count_tasks())

    get_done_tasks_count_for_humans.short_description = _('Tasks')

    def tasks_past_due_percentage(self):
        """Calcula o percentual de tarefas atrasadas
        """
        if self.count_tasks() > 0:
            return round(100 * (self.count_past_due_tasks() / self.count_tasks()), None)
        else:
            return 0

    def tasks_past_due_not_on_hold_percentage(self):
        """Calcula o percentual de tarefas atrasadas QUE NÃO ESTÃO em estado de "em espera"
        """
        if self.count_tasks() > 0:
            return round(100 * (self.count_past_due_not_on_hold_tasks() / self.count_tasks()), None)
        else:
            return 0

    tasks_past_due_percentage.short_description = _("% of Tasks Past Due")

    def tasks_past_due_date_average(self) -> int:
        """Calcula a média de dias de atraso do projeto.
        """
        total_days: int = 0
        total_tasks: int = 0
        for task in self.get_done_tasks():
            total_days += (task.done_date - task.due_date).days
            total_tasks += 1
        # str(self.get_tasks_past_due_date().filter(Q(due_date__isnull=False)).aggregate(
        #     average_difference=Avg(F('due_date') - F('done_date')))['average_difference'].days) + " " + _('day(s)')
        try:
            return round(total_days / total_tasks)
        except ZeroDivisionError:
            return 0

    tasks_past_due_date_average.short_description = _("Avg Days Past Due")

    def tasks_past_due_date_not_on_hold_average(self) -> int:
        """Calcula a média de dias de atraso do projeto sem levar em conta as tarefas em espera.
        """
        total_days: int = 0
        total_tasks: int = 0
        for task in self.get_done_tasks().filter(~Q(status=Task.get_onhold_status_code())):
            total_days += (task.done_date - task.due_date).days
            total_tasks += 1
        # return str(self.get_tasks_past_due_date().filter(
        #     Q(due_date__isnull=False) & ~Q(status=Task.get_onhold_status_code())).aggregate(
        #     average_difference=Avg(F('due_date') - F('done_date')))['average_difference'].days) + " " + _('day(s)')
        try:
            return round(total_days / total_tasks)
        except ZeroDivisionError:
            return 0

    def tasks_progress(self):
        if self.count_tasks() > 0:
            return round(100 * (self.count_done_tasks() / self.count_tasks()), None)
        else:
            return None

    tasks_progress.short_description = '% ' + str(_("of Completion"))

    def tasks_progress_for_humans_admin(self):
        return return_mark_safe(
            """{prog}% - <progress max="100" value="{prog}"></progress>""".format(prog=self.tasks_progress()))

    tasks_progress_for_humans_admin.short_description = '% ' + str(_("of Completion"))

    @classmethod
    def filter_objects(cls, searched_value: str, request_user: User, values_list_fields: list = None,
                       custom_query: Q = Q(), initial_queryset: QuerySet('Project') = None) -> QuerySet:
        """Filtra os objetos da classe com base na string passada como parâmetro. Vide docstring do método chamado."""
        return default_task_objects_filter(filtered_class=cls, searched_value=searched_value, request_user=request_user,
                                           values_list_fields=values_list_fields, custom_query=custom_query)

    @staticmethod
    def filter_objects_based_on_user(request_user_profile: 'Profile', queryset: QuerySet) -> QuerySet:
        """Filtra os objetos da classe para retornar somente os que pertencem ao usuario passado como parametro"""
        filters_dict = {
            'staff': Q(),
            'catalog': Q(catalog=request_user_profile.get_user_catalog()),
        }
        return ObjectFilterer.filter_objects_based_on_user(request_user_profile.get_user_type(), queryset, filters_dict)


class ProjectModel(BaseModel):
    """Project model is simply a model for deploying full projects with its tasks"""
    catalog = models.ForeignKey(verbose_name=_('Catalog'), to=BaseCatalog, on_delete=models.SET_NULL, null=True,
                                blank=True)
    title = models.CharField(verbose_name=_('Title'), max_length=100)
    description = models.TextField(verbose_name=_('Description'), blank=True, null=True, default=" ")
    tasks = models.ManyToManyField(verbose_name=_('Tasks Models'), to='TaskModel', blank=True)

    class Meta:
        verbose_name = _('Project Model')
        verbose_name_plural = _('Project Models')

    def __str__(self):
        """str method"""
        return self.title

    @staticmethod
    def get_column_order_choices() -> List[str]:
        """Retorna o dicionario com as colunas do datatables em que os produtos podem ser ordenados"""
        return PROJECT_MODEL_ORDER_COLUMN_CHOICES

    def get_project_valid_tasks(self):
        """retorna um queryset com todas tarefas nao canceladas do projeto
        """
        return self.tasks.all()

    def count_tasks(self):
        return self.tasks.count()

    count_tasks.short_description = _("# of Tasks")

    def deploy_project(self, start_date: timezone, title: str, prefix_description: str = ""):
        """Deploy project and return a Project id

        """
        description = str(prefix_description) + "<br>" + str(self.description)
        project_title = title[:50] if len(title) < 48 else "".join([title[:47], '...'])
        project_subtitle = self.title[:50] if len(self.title) < 43 else "".join([self.title[:42], '...'])
        project = Project(title="{} ({})".format(project_title, project_subtitle), description=description,
                          start_date=start_date)
        project.save()
        self.deploy_tasks(project)
        return project.id

    def deploy_tasks(self, project):
        """ Deploy tasks for a project
        """
        for task in self.tasks.all():
            due_date = project.start_date + timedelta(days=task.days_after_start)
            Task.objects.create(project=project, due_date=due_date, task_model_id=task.id)

            # O código abaixo é executado no save.
            # new_task = Task.objects.create(project=project, title=task.title, description=task.description,
            #                                due_date=due_date, priority=task.priority,
            #                                group=task.group, assigned_to=task.assigned_to)
            # for item in task.taskitemmodel_taskmodel.order_by('id').all():
            #     TaskItem.objects.create(title=item.title, task_id=new_task.id)
        #     todo criar as relações de parent
        for task in project.task_project.all():
            task.attach_parent_tasks()
        return project.id

    @staticmethod
    def filter_objects_based_on_user(request_user_profile: 'Profile', queryset: QuerySet) -> QuerySet:
        """Filtra os objetos da classe para retornar somente os que pertencem ao usuario passado como parametro"""
        filters_dict = {
            'staff': Q(),
            'catalog': Q(catalog=request_user_profile.get_user_catalog()),
        }
        return ObjectFilterer.filter_objects_based_on_user(request_user_profile.get_user_type(), queryset, filters_dict)

    @classmethod
    def filter_objects(cls, searched_value: str, request_user: User, values_list_fields: list = None,
                       custom_query: Q = Q(), initial_queryset: QuerySet('ProjectModel') = None) -> QuerySet:
        """
        Filtra os objetos da classe com base na string passada como parâmetro
        Args:
            searched_value: valor a ser buscado
            request_user: usuário da request
            values_list_fields: valores a se colocar no values_list, caso seja necessário
            custom_query: caso o usuário deseje fazer alguma query diferente da padrão, deverá preencher este parametro
        Returns:
            QuerySet de objetos da classe que correspondem ao filtro aplicado
        """
        return default_task_objects_filter(filtered_class=cls, searched_value=searched_value, request_user=request_user,
                                           values_list_fields=values_list_fields, custom_query=custom_query)

    @staticmethod
    def query_products_by_args(request) -> dict:
        """
            Metodo usado pela api do DataTables para buscar dinamicamente por produtos com base na caixa de busca
            Args:
                request: request da api
            Returns:
                dict contendo a queryset de produtos e outras informacoes relevantes ao DataTables
        """
        return default_query_assets_by_args(request, ProjectModel)

    def get_data_for_api(self):
        return {
            'catalog': self.catalog,
            'title': self.title,
            'description': self.description,
            'num_tasks': self.count_tasks(),
        }


TASK_STATUSES = (
    ('NEW', _('New')),
    ('HOL', _('On Hold')),
    # ('WOR', _('Working')),
    ('DON', _('Done')),
    ('CAN', _('Canceled')),
)

TASK_PRIORITY = (
    ('9LOW', _('Low')),
    ('5MED', _('Medium')),
    ('2HIG', _('High')),
    ('0FAT', _('Fatal')),
)

TASK_PRIORITY_FRONT_CLASS = {
    '9LOW': 'secondary',
    '5MED': 'info',
    '2HIG': 'warning',
    '0FAT': 'danger'
}


class TaskItemBase(BaseModel):
    """Task Item Base model. A kind of checklist"""
    title = models.CharField(verbose_name=_('Item'), max_length=250)
    order = models.PositiveIntegerField(verbose_name=_('Order'), default=0)

    class Meta(object):
        abstract = True
        verbose_name = _('Task Item')
        verbose_name_plural = _('Tasks Item')
        ordering = ['order']

    def __str__(self):
        """str method"""
        return self.title


class TaskModel(BaseModel):
    """Task model class"""
    catalog = models.ForeignKey(verbose_name=_('Catalog'), to=BaseCatalog, on_delete=models.SET_NULL, null=True,
                                blank=True)
    title = models.CharField(verbose_name=_('Title'), max_length=150)
    description = models.TextField(verbose_name=_('Description'), blank=True, null=True, default=" ")
    days_after_start = models.IntegerField(verbose_name=_('Days After Project Start'), help_text=_('Can be negative.'),
                                           default=0)
    priority = models.CharField(verbose_name=_('Priority'), max_length=5, choices=TASK_PRIORITY, default="9LOW")
    group = models.ForeignKey(verbose_name=_('Group'), to='TaskGroup', on_delete=models.PROTECT, null=True, blank=True)
    assigned_to = models.ForeignKey(verbose_name=_('Assigned To'), null=True, blank=True, to=User,
                                    on_delete=models.PROTECT,
                                    limit_choices_to=Q(user_user_profile__is_artist=False, is_active=True),
                                    related_name="task_model_user")

    parent_task_model = models.ManyToManyField("TaskModel", verbose_name=_('Parent Task'), blank=True,
                                               symmetrical=False)

    is_private = models.BooleanField(verbose_name=_('Private Task'),
                                     help_text=_('Only the assigned user or group participants can see this task'),
                                     default=False)

    class Meta:
        verbose_name = _('Task Model')
        verbose_name_plural = _('Task Models')

    def __str__(self):
        """str method"""
        return self.title

    @staticmethod
    def autocomplete_search_fields():
        return 'title',

    @staticmethod
    def filter_objects_based_on_user(request_user_profile: 'Profile', queryset: QuerySet) -> QuerySet:
        """Filtra os objetos da classe para retornar somente os que pertencem ao usuario passado como parametro"""
        filters_dict = {
            'staff': Q(),
            'catalog': Q(catalog=request_user_profile.get_user_catalog()),
        }
        return ObjectFilterer.filter_objects_based_on_user(request_user_profile.get_user_type(), queryset, filters_dict)

    @classmethod
    def filter_objects(cls, searched_value: str, request_user: User, values_list_fields: list = None,
                       custom_query: Q = Q(), initial_queryset: QuerySet('TaskModel') = None) -> QuerySet:
        """
        Filtra os objetos da classe com base na string passada como parâmetro
        Args:
            searched_value: valor a ser buscado
            request_user: usuário da request
            values_list_fields: valores a se colocar no values_list, caso seja necessário
            custom_query: caso o usuário deseje fazer alguma query diferente da padrão, deverá preencher este parametro
        Returns:
            QuerySet de objetos da classe que correspondem ao filtro aplicado
        """
        return default_task_objects_filter(filtered_class=cls, searched_value=searched_value, request_user=request_user,
                                           values_list_fields=values_list_fields, custom_query=custom_query)

    def get_data_for_api(self):
        return {
            'catalog': self.catalog_id,
            'title': self.title,
            'description': self.description,
            'days_after_start': self.days_after_start,
            'priority': self.priority,
            'group': self.group_id,
            'assigned_to': self.assigned_to_id,
            # 'parent_task_model': self.parent_task_model,
            'is_private': self.is_private,
            'catalog_name': self.catalog.__str__() if self.catalog else 'N/A',
            'group_name': self.group.__str__() if self.group else 'N/A',
            'assigned_to_name': self.assigned_to.__str__() if self.assigned_to else 'N/A',
            'get_priority_display': self.get_priority_display(),
        }

    @staticmethod
    def query_products_by_args(request) -> dict:
        """
            Metodo usado pela api do DataTables para buscar dinamicamente por tarefas com base na caixa de busca
            Args:
                request: request da api
            Returns:
                dict contendo a queryset de produtos e outras informacoes relevantes ao DataTables
        """
        return default_query_assets_by_args(request, TaskModel)

    @staticmethod
    def get_column_order_choices() -> List[str]:
        """Retorna o dicionario com as colunas do datatables em que os produtos podem ser ordenados"""
        return TASK_MODEL_ORDER_COLUMN_CHOICES


class Task(BaseModel, GetAdminUrl, BaseApiDataClass):
    """Task class"""
    catalog = models.ForeignKey(verbose_name=_('Catalog'), to=BaseCatalog, on_delete=models.SET_NULL, null=True,
                                blank=True)
    project = models.ForeignKey(verbose_name=_('Project'), to=Project, on_delete=models.CASCADE,
                                related_name='task_project', blank=True, null=True)
    title = models.CharField(verbose_name=_('Title'), max_length=150)
    description = models.TextField(verbose_name=_('Description'), blank=True, null=True, default=" ")
    due_date = models.DateField(verbose_name=_('Due Date'), blank=True, null=True)
    done_date = models.DateField(verbose_name=_('Done Date'), blank=True, null=True)
    status = models.CharField(verbose_name=_('Status'), max_length=3, choices=TASK_STATUSES, default="NEW")
    priority = models.CharField(verbose_name=_('Priority'), max_length=5, choices=TASK_PRIORITY, default="9LOW")
    group = models.ForeignKey(verbose_name=_('Group'), to='TaskGroup', on_delete=models.CASCADE,
                              related_name="task_group", blank=True, null=True)
    archived = models.BooleanField(verbose_name=_('Archived'), default=False)
    assigned_to = models.ForeignKey(verbose_name=_('Assigned To'), to=User, on_delete=models.SET_NULL,
                                    limit_choices_to=Q(user_user_profile__is_artist=False, is_active=True),
                                    # limit_choices_to={'is_staff': True},
                                    related_name="task_user", blank=True, null=True)
    task_model = models.ForeignKey(verbose_name=_('Task Model (opt)'), to=TaskModel, on_delete=models.SET_NULL,
                                   null=True, blank=True, help_text=_(
            'If you select a Task Model, "group", "assigned to" and "priority" fields become irrelevant.'
            ' Selecting a model AFTER a task is created will have no effect.'))
    is_private = models.BooleanField(verbose_name=_('Private Task'),
                                     help_text=_('Only the assigned user or group participants can see this task'),
                                     default=False)

    parent_task = models.ManyToManyField("self", verbose_name=_('Parent Task'), blank=True, symmetrical=False)

    def __init__(self, *args, **kwargs):
        """Init """
        super(Task, self).__init__(*args, **kwargs)
        self.prev_status = self.status

    class Meta:
        verbose_name = _('Task')
        verbose_name_plural = _('Tasks')

    def __str__(self):
        """str method"""
        try:
            return f'{self.get_tilte_with_id()} - ' + self.due_date.strftime('%d/%m/%Y')
        except AttributeError:
            return f'{self.get_tilte_with_id()} - ' + _('No due date')

    def get_relevant_task_notification_recipients(self, initial_queryparams: Q) -> QuerySet(User):
        """
        Retorna um queryset de usuários para os quais é relevante receber notificações sobre alteração/criação
            de tarefa
        Args:
            initial_queryparams: Q inicial que contém o filtro de quais usuários desejam receber a notificação

        Returns: Queryset de User
        """
        queryparams = Q()
        if self.group:
            queryparams = queryparams | Q(id__in=[user.id for user in self.group.users.all()])
        if self.assigned_to:
            queryparams = queryparams | Q(id=self.assigned_to_id)

        queryparams &= initial_queryparams
        return User.objects.filter(queryparams).distinct()

    def get_tilte_with_id(self):
        return f"[{str(self.id)}] - {self.title}"

    @staticmethod
    def autocomplete_search_fields():
        return ('id', 'title', 'date')

    def save(self, **kwargs):
        """
        O metodo save eh sobrescrito para que, na criacao da tarefa, se ela estiver sendo criada a partir de uma Tarefa
        Modelo, os ajustes sejam feitos para tal. Estes ajustes sao:
            O titulo final será a união do título da tarefa com o título do modelo (respeitando o limite de caracteres)
            A descrição também será a união da descrição tarefa com a do modelo
            A prioridade da tarefa será igual à do modelo
            O grupo da tarefa será o mesmo do modelo
            O usuario atribuido da tarefa será o mesmo do modelo
            Os checklists do modelo são reproduzidos na tarefa
        Caso algum dos campos citados acima tenham sido preenchidos na tarefa, eles serão sobrescritos. Os checklists
        criados na tarefa não serão ignorados. A tarefa final terá os checklists do modelo E os definidos manualmente
        na sua criação.

        Testa inicialmente o status para determinar o done date
        """
        if not self.id and self.task_model:
            model = self.task_model
            task_title = self.title
            task_subtitle = model.title
            if len(task_title) + len(task_subtitle) >= 150:
                task_title = self.title[:70] if len(self.title) <= 70 else ''.join([self.title[:67], '...'])
                task_subtitle = model.title[:75] if len(model.title) < 74 else ''.join([model.title[:72], '...'])

            if task_title != '':
                # pode ser que seja gerado via projeto sem título
                final_title = f'{task_title} ({task_subtitle})'
            else:
                final_title = task_subtitle
            self.title = final_title
            if model.description:
                final_description = '{}: {} <br><br><br> ----------- <br>{}: {}'.format(_('Model description'),
                                                                                        model.description,
                                                                                        _('Individual description'),
                                                                                        self.description)
            else:
                final_description = self.description

            self.description = final_description
            self.priority = model.priority
            self.group = model.group
            self.assigned_to = model.assigned_to
            self.is_private = model.is_private
            super().save()
            for item in model.taskitemmodel_taskmodel.all():
                TaskItem.objects.create(title=item.title, task=self, assigned_to=item.assigned_to, group=item.group,
                                        order=item.order)
        else:
            super().save()

    def attach_parent_tasks(self):
        """Anexa os pre-requisitos de acordo com os modelos."""
        if self.task_model:
            if model_parent_tasks_id := [*self.task_model.parent_task_model.values_list('id', flat=True)]:
                if parent_tasks := Task.objects.filter(task_model_id__in=model_parent_tasks_id, project=self.project):
                    # * para expandir porque add() não aceita lista, apenas args
                    self.parent_task.add(*parent_tasks)

    def check_parent_is_done(self):
        """Checa se existe algum parent em aberto"""
        for parent in self.parent_task.all():
            if parent.status not in [self.get_done_status_code(), self.get_canceled_status_code()]:
                # caso tenha tarefa nova ou em espera
                return False
        return True

    def mark_status_as_done(self):
        """Marca a tarefa como feita. Se retornar None é porque saiu certo, qualquer string representa erro."""
        if self.get_items_done_count() != self.taskitem_task.all().count():
            return _('Task has open items. Please close them before marking the task as done.')
        elif not self.check_parent_is_done():
            return _('Task has open parent tasks. Please close them before marking the task as done.')
        else:
            self.status = self.get_done_status_code()
            self.done_date = timezone.now()
            self.save()
            notification_code = SystemNotification.get_task_status_updated_code()
            recipients = self.get_relevant_task_notification_recipients(
                Q(user_user_profile__profilesystemnotification__notification__code=notification_code))
            notify_users(notification_code, recipients, action_object=self,
                         url=reverse('dashboard:tasks.schedule', args=[self.id]),
                         extra_info=self.get_status_display())
            return None

    def mark_status_as_new(self):
        """Marca a tarefa como nova. Se retornar None é porque saiu certo, qualquer string representa erro."""
        self.status = self.get_new_status_code()
        if self.get_items_done_count() > 0:
            return _('Task has done items.')
        return None

    def mark_status_as_canceled_or_onhold(self, is_canceled=False):
        """Marca a tarefa como cancelada ou em espera."""
        if is_canceled:
            self.status = self.get_canceled_status_code()
        else:
            self.status = self.get_onhold_status_code()
        self.done_date = None
        self.save()
        notification_code = SystemNotification.get_task_status_updated_code()
        recipients = self.get_relevant_task_notification_recipients(
            Q(user_user_profile__profilesystemnotification__notification__code=notification_code))
        notify_users(notification_code, recipients, action_object=self,
                     url=reverse('dashboard:tasks.schedule', args=[self.id]),
                     extra_info=self.get_status_display())
        return None

    @classmethod
    def edit_self(cls, form, task_id) -> object:
        """Made to edit the task based on the data from a form
        Arguments:
            form(form)
            task_id(int)
        """
        from django.core.exceptions import ObjectDoesNotExist
        try:
            task = cls.objects.get(id=task_id)
            if form['project']:
                task.project_id = form['project']
            else:
                task.project_id = None
            task.title = form['title']
            task.description = form['description']
            task.due_date = form['due_date']
            task.status = form['status']
            task.priority = form['priority']
            task.group_id = form['group']
            # task.archived=form['archived']
            task.assigned_to_id = form['assigned_to']
            task.save()
            return task
        except ObjectDoesNotExist:
            return None

    @staticmethod
    def get_new_status_code() -> str:
        """Return the new status code. Built for reuse."""
        return "NEW"

    @staticmethod
    def get_done_status_code() -> str:
        """Return the done status code. Built for reuse."""
        return "DON"

    @staticmethod
    def get_onhold_status_code() -> str:
        """Return the on hold status code. Built for reuse."""
        return "HOL"

    @staticmethod
    def get_canceled_status_code() -> str:
        """Return the CANCELED status code. Built for reuse."""
        return "CAN"

    @staticmethod
    def get_all_statuses_code_except_finished() -> list:
        """Return all statuses codes except get_done_status_code(). Built for reuse"""
        status_list = []
        for status in TASK_STATUSES:
            if status[0] != Task.get_done_status_code() and status[0] != Task.get_canceled_status_code():
                status_list.append(status[0])
        return status_list

    @staticmethod
    def get_count_tasks_by_user(user):
        """Counts user's undone tasks

        :param user: User
        :return: Integer
        """
        return Task.objects.filter(
            ~Q(status=Task.get_done_status_code()),
            Q(group__users__id=user.id, assigned_to_id__isnull=True) | Q(
                assigned_to__id=user.id)).distinct().count()

    def project_title(self):
        # test = self.objects.filter(assigned_to_id__isnull=)
        if self.project is not None:
            return self.project.title
        else:
            return 'N/A'

    project_title.short_description = _("Project")

    def project_description(self):
        if self.project is not None:
            return format_html(self.project.description)
        else:
            return 'N/A'

    project_description.short_description = _("Notes")

    def get_items_done_count(self):
        return self.taskitem_task.filter(done=True).count()
        # done_items = 0
        # for item in items:
        #     done_items += 1 if item.done else 0
        # return done_items

    def get_items_done_count_for_humans(self):
        num_items = self.taskitem_task.count()
        return "%s/%s" % (self.get_items_done_count(), num_items)

    get_items_done_count_for_humans.short_description = _('Task Items')

    def get_data_for_api(self, include_sub_item, include_id=False, user_to_check=None,
                         include_project_data: bool = False, include_comments: bool = False,
                         include_parent_tasks: bool = False):
        """Get product data for api responses"""
        sub_items_dict = []
        user_to_check_id = user_to_check.id if user_to_check is not None else 0
        user_to_check_groups = [item.id for item in user_to_check.task_group_users.only('id')]
        data = {
            'sub_items': None,
            'sub_items__total': 0,
            'parent_tasks': [],
            'parent_tasks__total': 0,
            'sub_items__assignee_total': 0,
            'sub_items__todo': 0,
            'assigned_to__name': _('N/A'),
            'group__name': _('N/A'),
            'assignment_type': "",
            'project': None,
            'is_past_due': None,
            'comments': [],
            'is_private': False,
        }

        sub_items = self.taskitem_task.all()

        for sub_item in sub_items:
            sub_item_dict = sub_item.get_data_for_api(user_to_check_id, True)
            sub_items_dict.append(sub_item_dict)
            data['sub_items__total'] += 1
            data['sub_items__todo'] += 1 if (
                    (sub_item_dict['assigned_to__id'] == user_to_check_id or sub_item_dict[
                        'assigned_to__group__id'] in user_to_check_groups) and not sub_item_dict['done']) else 0
            data['sub_items__assignee_total'] += 1 if (
                    sub_item_dict['assigned_to__id'] == user_to_check_id or sub_item_dict[
                'assigned_to__group__id'] in user_to_check_groups) else 0
        if include_sub_item:
            data['sub_items'] = sub_items_dict

        if include_project_data:
            data['project__description'] = self.project.description if self.project is not None else 'N/A'
            data['project__title'] = self.project.title if self.project is not None else 'N/A'
            data['project__start_date'] = self.project.start_date if self.project is not None else 'N/A'

        if include_parent_tasks:
            for parent in self.parent_task.all():
                data['parent_tasks__total'] += 1
                data['parent_tasks'].append({
                    'title': parent.get_tilte_with_id(),
                    'status': parent.get_status_display()
                })

        group = self.group
        group_name = _('N/A')
        if group:
            group_name = group.name
        assignee = self.assigned_to
        assignee_name = _('N/A')

        if assignee:
            assignee_name = assignee.get_username()
        if user_to_check_id == self.assigned_to_id:
            data['assignment_type'] = 'O'
        else:
            if data['sub_items__assignee_total']:
                data['assignment_type'] += 'C'
            elif group is not None:
                if group.id in user_to_check_groups:
                    data['assignment_type'] += 'G'

        for comment in self.taskcomment_set.order_by('-created_at').prefetch_related('user').all():
            comment_user = comment.user
            data['comments'].append({
                'comment': comment.comment,
                'created_at': comment.created_at,
                'user__name': comment_user.get_username(),
                'user__avatar': Profile.get_gravatar(comment_user),
            })
        data['admin_url'] = self.get_admin_url()
        data['title'] = self.get_tilte_with_id()
        data['description'] = self.description
        data['due_date'] = self.due_date if self.due_date else _('No Date')
        data['status'] = self.status
        data['status_display'] = self.get_status_display()
        data['priority'] = self.priority
        data['priority_display'] = self.get_priority_display()
        data['group__name'] = group_name
        data['archived'] = self.archived
        data['assigned_to__name'] = assignee_name
        data['is_private'] = self.is_private
        if self.due_date:
            data['is_past_due'] = self.due_date <= timezone.now().date()
        if include_id:
            data['id'] = self.id

        return data

    @staticmethod
    def get_priority_front_color(priority):
        try:
            return TASK_PRIORITY_FRONT_CLASS[priority]
        except KeyError:
            return 'secondary'

    @staticmethod
    def filter_objects_based_on_user(request_user_profile: 'Profile', queryset: QuerySet) -> QuerySet:
        """Filtra os objetos da classe para retornar somente os que pertencem ao usuario passado como parametro"""
        filters_dict = {
            'staff': Q(),
            'catalog': Q(catalog=request_user_profile.get_user_catalog()),
        }
        return ObjectFilterer.filter_objects_based_on_user(request_user_profile.get_user_type(), queryset, filters_dict)

    @classmethod
    def filter_objects(cls, searched_value: str, request_user: User, values_list_fields: list = None,
                       custom_query: Q = Q(), initial_queryset: QuerySet('Task') = None) -> QuerySet:
        """
        Filtra os objetos da classe com base na string passada como parâmetro
        Args:
            searched_value: valor a ser buscado
            request_user: usuário da request
            values_list_fields: valores a se colocar no values_list, caso seja necessário
            custom_query: caso o usuário deseje fazer alguma query diferente da padrão, deverá preencher este parametro
        Returns:
            QuerySet de objetos da classe que correspondem ao filtro aplicado
        """
        return default_task_objects_filter(filtered_class=cls, searched_value=searched_value, request_user=request_user,
                                           values_list_fields=values_list_fields, custom_query=custom_query)


# noinspection PyUnusedLocal
@receiver(post_save, sender=Task)
def task_post_save(sender, instance: Task, created, *args, **kwargs):
    """Task post save signal handler

    Sends notifications to those related to this task being created
    """
    if created:
        notification_code = SystemNotification.get_new_task_code()
        queryparams: Q = Q(user_user_profile__profilesystemnotification__notification__code=notification_code)
        recipients: QuerySet(User) = instance.get_relevant_task_notification_recipients(queryparams)
        author = instance.project if instance.project is not None else None
        notify_users(notification_code, recipients, author=author, action_object=instance,
                     url=reverse('dashboard:tasks.schedule', args=[instance.id]))


class TaskItem(TaskItemBase, BaseApiDataClass):
    """Override to add a relation to task"""
    task = models.ForeignKey(verbose_name=_('Task'), to=Task, on_delete=models.CASCADE,
                             related_name="taskitem_task")
    done = models.BooleanField(verbose_name=_('Done'), default=False)
    assigned_to = models.ForeignKey(verbose_name=_('Assigned To'), null=True, blank=True, to=User,
                                    on_delete=models.SET_NULL,
                                    limit_choices_to=Q(user_user_profile__is_artist=False, is_active=True)
                                    # limit_choices_to={'is_staff': True}
                                    , related_name="task_item_user")

    group = models.ForeignKey(verbose_name=_('Group'), to='TaskGroup', on_delete=models.SET_NULL, null=True, blank=True)

    def get_data_for_api(self, user_to_check_id: int = 0, include_id: bool = False):
        """Get Task Items"""
        data = {
            'done': self.done,
            'title': self.title,
            'assigned_to__name': None,
            'assigned_to__id': None,
            'assigned_to__group__id': None,
            'assigned_to__group__name': None,
            'assigned_to__requester': False,
        }
        if self.assigned_to:
            assigned_to = self.assigned_to
            data['assigned_to__name'] = assigned_to.get_username()
            data['assigned_to__id'] = assigned_to.id
            data['assigned_to__requester'] = assigned_to.id == user_to_check_id

        if self.group:
            data['assigned_to__group__id'] = self.group_id
            data['assigned_to__group__name'] = self.group.name

        if include_id:
            data['id'] = self.id

        return data


class TaskItemModel(TaskItemBase):
    """Override to add a relation to task model
    """
    task = models.ForeignKey(verbose_name=_('TaskModel'), to=TaskModel, on_delete=models.CASCADE,
                             related_name="taskitemmodel_taskmodel")

    assigned_to = models.ForeignKey(verbose_name=_('Assigned To'), null=True, blank=True, to=User,
                                    on_delete=models.PROTECT,
                                    limit_choices_to={
                                        'is_staff': True
                                    }, related_name="task_item_model_user")

    group = models.ForeignKey(verbose_name=_('Group'), to='TaskGroup', on_delete=models.PROTECT, null=True, blank=True)


class TaskGroup(BaseModel):
    """Task Group class
    Organize tasks into groups to improve manageability.
    """
    name = models.CharField(verbose_name=_('Name'), max_length=100)
    users = models.ManyToManyField(verbose_name=_('Users'), to=User, limit_choices_to={
        'is_staff': True
    }, related_name="task_group_users")
    owner = models.ForeignKey(verbose_name=_('Owner'), to=User, on_delete=models.CASCADE,
                              limit_choices_to={
                                  'is_staff': True
                              }, related_name="task_group_owner")

    class Meta:
        verbose_name = _('Task Group')
        verbose_name_plural = _('Task Groups')

    def __str__(self):
        """str method"""
        return self.name

    @classmethod
    def filter_objects(cls, searched_value: str, request_user: User, values_list_fields: list = None,
                       custom_query: Q = Q(), initial_queryset: QuerySet('TaskGroup') = None) -> QuerySet:
        """Filtra os objetos da classe com base na string passada como parâmetro. Vide docstring do método chamado."""
        queryset = initial_queryset or cls.objects.all()
        search_fields = ['name']
        # Caso o usuario tenha passado uma query como parametro, o filtro sera feito com base nela apenas
        if custom_query:
            queryset = queryset.filter(custom_query)
        return ObjectFilterer.filter_objects(cls, searched_value, request_user.user_user_profile, search_fields,
                                             queryset, values_list_fields)


class TaskComment(BaseModel):
    """Task Comment class"""
    task = models.ForeignKey(verbose_name=_('Task'), to='Task', on_delete=models.CASCADE)
    comment = models.TextField(verbose_name=_('Comment'))
    user = models.ForeignKey(verbose_name=_('User'), to=User, on_delete=models.CASCADE,
                             limit_choices_to={
                                 'is_staff': True
                             }, related_name="task_comment_user")

    class Meta:
        verbose_name = _('Task Comment')
        verbose_name_plural = _('Task Comments')

    def __str__(self):
        """str method"""
        return "Comment"


# noinspection PyUnusedLocal
@receiver(post_save, sender=TaskComment)
def task_comment_post_save(sender, instance: TaskComment, created, *args, **kwargs):
    """Task Comment post save
    Sends notifications to users related to the task
    """
    if not created:
        return
    notification_code = SystemNotification.get_task_commented_code()
    # O último Q antes do filtro garante que só os usuários que desejam este tipo de notificação o recebam.
    #  Os anteriores filtram os usuários relevantes para receber a notificação. No fim das contas, quem a recebe são
    #  os usuários relevantes *que marcaram* que desejam ser notificados disto.
    queryparams = Q()
    if instance.task.group:
        queryparams = queryparams | Q(id__in=[user.id for user in instance.task.group.users.all()])
    if instance.task.taskcomment_set.count() > 1:
        queryparams = queryparams | Q(task_comment_user__task_id=instance.task_id)
    if instance.task.assigned_to:
        queryparams = queryparams | Q(id=instance.task.assigned_to_id)
    if instance.task.taskitem_task.count() > 0:
        queryparams = queryparams | Q(task_item_user__task_id=instance.task_id)
    queryparams = queryparams & Q(
        user_user_profile__profilesystemnotification__notification__code=notification_code)
    recipients = User.objects.filter(queryparams).distinct().exclude(id=instance.user_id)
    notify_users(notification_code, recipients, author=instance.user, action_object=instance.task,
                 url=reverse('dashboard:tasks.schedule', args=[instance.task.id]),
                 extra_info=f'"{instance.user}": {instance.comment[:25]}...')


# noinspection PyUnusedLocal
@receiver(post_save, sender=Project)
def project_post_save(sender, instance, created, *args, **kwargs):
    """Project post_save.

    Deploys model tasks if needed
    """
    if created and instance.model:
        instance.model.deploy_tasks(instance)


auditlog.register(Project)
auditlog.register(ProjectModel)
auditlog.register(TaskModel)
auditlog.register(Task)
auditlog.register(TaskItem)
auditlog.register(TaskItemModel)
auditlog.register(TaskGroup)
auditlog.register(TaskComment)

from typing import Tuple, List, Type, Union

from crispy_forms.utils import render_crispy_form
from django.contrib import messages
from django.contrib.auth.decorators import login_required, user_passes_test, permission_required
from django.contrib.auth.models import User
from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.db.models import Q
from django.http import HttpResponseNotFound, JsonResponse, HttpResponseBadRequest, HttpRequest
from django.urls import reverse
from django.utils.translation import gettext_lazy as _
from django.shortcuts import render, redirect

from django.utils import timezone
from django.views.decorators.http import require_POST, require_GET

from django import forms

from music_system.apps.clients_and_profiles.models.notifications import SystemNotification, notify_users
from music_system.apps.contrib.decorators import has_perm_custom
from music_system.apps.contrib.form_widgets import render_dynamic_crispy_formset
from music_system.apps.contrib.log_helper import log_tests, log_error
from music_system.apps.contrib.views.base import get_user_profile_from_request
from music_system.apps.label_catalog.helpers import default_api_get_queryset_for_select2
from music_system.apps.label_catalog.models import Holder, Product
from music_system.apps.tasks.forms import TaskFrontForm, TaskItemFrontInline, ProjectForm, TaskFrontNoGroupForm, \
    TaskItemNoGroupFrontInline, ProjectModelForm, TaskModelFrontForm, TaskModelItemFrontInline, \
    TaskModelItemNoGroupFrontInline, TaskModelFrontNoGroupForm
from music_system.apps.tasks.models import Task
from music_system.apps.tasks.models.base import TaskItem, TaskComment, Project, TaskModel, ProjectModel

from music_system.apps.contrib.api_helpers import get_default_response_dict, get_generic_error_status, \
    get_success_status, get_api_response_dict

from notifications.signals import notify


def get_query_params_for_tasks_lists(request, request_has_dates=False) -> Q:
    if request_has_dates:
        start = request.GET.get('start', timezone.now() + timezone.timedelta(-30))
        end = request.GET.get('end', timezone.now() + timezone.timedelta(+30))
        query_params = Q(due_date__gte=start, due_date__lte=end)
    else:
        query_params = Q()

    # filtra por catalogo caso o usuario seja cataloguser
    request_user_profile = get_user_profile_from_request(request)
    if request_user_profile.user_is_catalog():
        query_params &= Q(
            catalog=request_user_profile.get_user_catalog())  # todo consistencia de catalogo. eh pra colocar se a tarefa modelo ou se o projeto eh do msm catalogo tbm?

    multiple_select_fields = [
        'project',
        'status',
        'priority',
    ]
    # se nao for cataloguser, group tem que ir no multiple_select_fields.
    if not request.user.user_user_profile.user_is_catalog():
        multiple_select_fields.append('group')
    filter_title = request.GET.get('filter_title', None)
    if filter_title:
        filter_title_agg = Q(title__icontains=filter_title)
        try:
            filter_title_agg |= Q(id=int(filter_title))
            query_params &= Q(filter_title_agg)
        except ValueError:
            query_params &= filter_title_agg

    include_archived = request.GET.get('filter_archived')
    if not include_archived or include_archived != "on":
        query_params &= Q(archived=False)

    filter_assigned_to = request.GET.getlist('filter_assigned_to', [])
    if len(filter_assigned_to) > 0:
        filter_assigned_to_query = Q(assigned_to__in=filter_assigned_to)
        filter_assigned_to_query |= Q(taskitem_task__assigned_to__in=filter_assigned_to)
        if request.GET.get('filter_assigned_to_include_groups', 'off') == 'on':
            filter_assigned_to_query |= Q(group__users__in=filter_assigned_to)
            filter_assigned_to_query |= Q(taskitem_task__group__users__in=filter_assigned_to)
        query_params &= Q(filter_assigned_to_query)

    for multiple_select_field in multiple_select_fields:
        form_field = request.GET.getlist("filter_{}".format(multiple_select_field), []) if request.GET.getlist(
            "filter_{}".format(multiple_select_field), None) else request.GET.getlist(
            "filter_{}[]".format(multiple_select_field), [])
        if len(form_field) > 0:
            query_params &= Q(**{"{}__in".format(multiple_select_field): form_field})

    return query_params


def get_private_tasks_query(user_id: int, is_superuser: bool = False):
    """
    Método usado para filtrar as tarefas privadas do sistema. Caso o usuário seja superusuário, a queryset volta vazia,
    pois o superuser deve poder ver todas as tarefas do sistema. Caso contrário, a queryset volta um filtro das tarefas
    privadas cujo usuário em questão não é o atribuído e nem pertence ao grupo, para que estas tarefas não aparecam para
    este usuário.
    Args:
        user_id: Usuário logado atualmente
        is_superuser: boolean que indica se o usuário é superuser (usado para evitar uma busca no bd só para fazer esta
            verificação)

    Returns: queryset de filtragem
    """
    if is_superuser:
        query = Q()
    else:
        query = Q(is_private=True) & Q(~Q(assigned_to__id=user_id) & ~Q(group__users__id__in=[user_id]))
    return query


@require_GET
@login_required
@user_passes_test(lambda user: user.is_staff or user.user_user_profile.user_is_catalog())
def api_get_tasks_calendar(request):
    """Get Tasks list for calendars.
    """
    response = []

    try:
        query_params = get_query_params_for_tasks_lists(request, True)
        tasks = Task.objects.prefetch_related(
            'assigned_to', 'group', 'assigned_to__groups', 'taskcomment_set', 'taskcomment_set__user',
            'project').filter(query_params).distinct().exclude(get_private_tasks_query(request.user.id))
        for task in tasks:
            task_dict_response = {}
            task_dict = task.get_data_for_api(True, True, request.user, True, False, False)
            task_dict_response['start'] = task_dict['due_date']
            task_dict_response['end'] = task_dict['due_date']
            task_dict_response['id'] = task_dict['id']
            task_dict_response['allDay'] = ""
            if task.project is not None:
                task_dict_response['project__title'] = task_dict['project__title']
            else:
                task_dict_response['project__title'] = 'N/A'
            task_dict_response['title'] = task_dict['title']
            task_dict_response['description'] = task_dict['description']
            task_dict_response['checklist_counter'] = "{}: {}/{}".format(_('Checklists'), task_dict['sub_items__todo'],
                                                                         task_dict['sub_items__total'])
            task_dict_response['priority_display'] = "{}: {}".format(_('Priority'), task_dict['priority_display'])
            task_dict_response['is_done'] = task_dict['status'] == Task.get_done_status_code()
            task_dict_response['status_display'] = "{}: {}".format(_('Status'), task_dict['status_display'])
            task_dict_response['assignment_type'] = task_dict['assignment_type']
            task_dict_response['assigned_to__name'] = "{}: {}".format(_('Assigned To'), task_dict['assigned_to__name'])
            # somente manda o group name na response se o usuario nao for catalog
            if not request.user.user_user_profile.user_is_catalog():
                task_dict_response['group__name'] = "{} {}".format(_('Group'), task_dict['group__name'])
            task_dict_response['front_class'] = Task.get_priority_front_color(task_dict['priority'])
            response.append(task_dict_response)

            # response['status'] = get_success_status()

    except ObjectDoesNotExist:
        return HttpResponseNotFound()

    return JsonResponse(response, safe=False)


@require_GET
@login_required
@user_passes_test(lambda user: user.is_staff or user.user_user_profile.user_is_catalog())
def api_get_tasks_list(request):
    """Get Tasks list for generic view.
    """
    response = get_default_response_dict()
    response['data']['items'] = []
    http_status = 200
    query_params = get_query_params_for_tasks_lists(request, False)
    try:
        today = timezone.datetime.now().date()

        tasks_late = get_tasks_for_list(
            Task.objects.filter(Q(query_params, due_date__lt=today) & Q(
                Q(status__in=Task.get_all_statuses_code_except_finished()) | Q(done_date__isnull=True))).order_by(
                'due_date').distinct().exclude(
                get_private_tasks_query(request.user.id)), request)
        response['data']['items'].append({'name': 'late', 'items': tasks_late})

        tasks_today = get_tasks_for_list(
            Task.objects.filter(query_params, due_date=today).order_by('due_date').distinct().exclude(
                get_private_tasks_query(request.user.id)), request)
        response['data']['items'].append({'name': 'today', 'items': tasks_today})

        tasks_week = get_tasks_for_list(Task.objects.filter(query_params, due_date__range=(
            today + timezone.timedelta(days=1), today + timezone.timedelta(days=7))).order_by(
            'due_date').distinct().exclude(get_private_tasks_query(request.user.id)), request)
        response['data']['items'].append({'name': 'week', 'items': tasks_week})

        tasks_upcoming = get_tasks_for_list(
            Task.objects.filter(query_params, due_date__gt=today + timezone.timedelta(days=7)).order_by(
                'due_date').distinct().exclude(get_private_tasks_query(request.user.id)), request)
        response['data']['items'].append({'name': 'upcoming', 'items': tasks_upcoming})

        tasks_dateless = get_tasks_for_list(
            Task.objects.filter(query_params, due_date__isnull=True).order_by('due_date').distinct().exclude(
                get_private_tasks_query(request.user.id)), request)
        response['data']['items'].append({'name': 'dateless', 'items': tasks_dateless})

        tasks_done = []
        if request.GET.get('filter_status') == 'DON':
            tasks_done = get_tasks_for_list(
                Task.objects.filter(query_params, due_date__lt=today,
                                    due_date__gte=today - timezone.timedelta(days=60)).order_by(
                    '-due_date').distinct().exclude(get_private_tasks_query(request.user.id)), request)
        response['data']['items'].append({'name': 'done', 'items': tasks_done})

    except ObjectDoesNotExist as e:
        log_error(e)
        http_status = 404
        response['status'] = get_generic_error_status()

    return JsonResponse(response, safe=False, status=http_status)


def get_tasks_for_list(tasks, request) -> list:
    task_list = []
    for task in tasks:
        task_dict_response = {}
        task_dict = task.get_data_for_api(True, True, request.user, True, False)
        task_dict_response['id'] = task_dict['id']
        if task.project is not None:
            task_dict_response['project__title'] = task_dict['project__title']
        else:
            task_dict_response['project__title'] = 'N/A'
        task_dict_response['title'] = task_dict['title'] if task_dict['assignment_type'] == '' else "{} - {}".format(
            task_dict['assignment_type'], task_dict['title'])
        task_dict_response['due_date'] = task_dict['due_date'] if task_dict['due_date'] is not None else _('N/A')
        task_dict_response['checklist_counter'] = "{}: {}/{}".format(_('Checklists'), task_dict['sub_items__todo'],
                                                                     task_dict['sub_items__total'])
        task_dict_response['is_done'] = task_dict['status'] == Task.get_done_status_code()
        task_dict_response['status_display'] = task_dict['status_display']
        task_dict_response['front_class'] = Task.get_priority_front_color(task.priority)
        task_list.append(task_dict_response)
    return task_list


@require_GET
@login_required
@user_passes_test(lambda user: user.is_staff or user.user_user_profile.user_is_catalog())
def api_get_task_details(request, task_id: int):
    """Get Product details for api.
    """
    # define a opcao default pra possibilitar escolha nula
    default_option = ('', '------------')
    response = get_default_response_dict()
    request_user_profile = get_user_profile_from_request(request)
    try:
        task = Task.objects.get(id=task_id)
        if not request_user_profile.user_is_catalog():
            form = TaskFrontForm(instance=task)
            formset = TaskItemFrontInline(instance=task)
        else:
            form = TaskFrontNoGroupForm(instance=task)
            formset = TaskItemNoGroupFrontInline(instance=task)
            # define a lista de usuarios que o cataloguser pode ver
            assigned_to_choices = list(User.objects.filter(
                cataloguser__catalog=request_user_profile.get_user_catalog()).values_list('id', 'username'))
            form.fields['assigned_to'].choices = assigned_to_choices
            # como os catalogusers nao definem grupo, assigned_to se torna obrigatorio. por isso so adicionamos a opcao
            #  default depois que colocamos as opções de usuario no assigned to, de forma a obrigar o cataloguser a defi
            #  nir um assigned to na tarefa, mas possibilitando que ele nao crie taskitems
            assigned_to_choices.insert(0, default_option)
            for formset_form in formset.forms:
                formset_form.fields['assigned_to'].choices = assigned_to_choices
        response['data']['items'] = [task.get_data_for_api(True, True, request.user, True, True, True)]
        response['data']['items'][0]['front_class'] = Task.get_priority_front_color(task.priority)
        response['data']['items'][0]['front_form'] = render_crispy_form(form)
        response['data']['items'][0]['front_form_item'] = render_dynamic_crispy_formset(formset, _('Task Items'))
        response['data']['items'][0]['front_form_item_prefix'] = formset.prefix

        response['data']['message'] = ''
    except ObjectDoesNotExist:
        return HttpResponseNotFound()

    return JsonResponse(response)


@require_POST
@login_required
@user_passes_test(lambda user: user.is_staff or user.user_user_profile.user_is_catalog())
def api_task_toggle_status(request, task_id: int):
    """Alterna status. Se nenhum status é passado, então marca como 'done'
    """

    mark_as_done = request.POST.get('mark_as_done', False)
    mark_as_canceled = request.POST.get('mark_as_canceled', False)
    mark_as_new = request.POST.get('mark_as_new', False)
    return helper_api_task_status(request, task_id, False, mark_as_done, mark_as_canceled, mark_as_new)


@require_POST
@login_required
@user_passes_test(lambda user: user.is_staff or user.user_user_profile.user_is_catalog())
def api_task_archive(request, task_id: int):
    """Arquiva tarefa
    """
    return helper_api_task_status(request, task_id, True, False, False)


@login_required
@has_perm_custom('task.delete_task')
def delete_task(request, task_id):
    """Deleta tarefa
    """
    response = get_default_response_dict()
    try:
        task = Task.objects.get(id=task_id)
        task.delete()
        response['data']['message'] = _('Task deleted successfully')
    except Task.DoesNotExist:
        response['status'] = get_generic_error_status()
        response['data']['message'] = _('Task not found')
    return JsonResponse(response)


def helper_api_task_status(request, task_id, archive: bool = False, mark_as_done: bool = False,
                           mark_as_canceled: bool = False, mark_as_new: bool = False):
    """Helper para views de pequenas atualizações de tarefas (arquivar/mudar status).
    """
    response = get_default_response_dict()
    try:
        if request.method != "POST":
            return HttpResponseBadRequest()
        task = Task.objects.get(id=task_id)
        if archive:
            task.archived = True
            task.status = Task.get_done_status_code()
        else:
            if mark_as_new:
                new_check = task.mark_status_as_new()
                if new_check is not None:
                    response['status'] = get_generic_error_status()
                    response['data']['message'] = new_check
                    return JsonResponse(response)
            elif mark_as_done:
                done_check = task.mark_status_as_done()
                if done_check is not None:
                    response['status'] = get_generic_error_status()
                    response['data']['message'] = done_check
                    return JsonResponse(response)
            else:
                task.mark_status_as_canceled_or_onhold(mark_as_canceled)
        task.save()
    except ObjectDoesNotExist:
        return HttpResponseNotFound()
    except KeyError:
        return HttpResponseBadRequest()
    return JsonResponse(response)


@require_POST
@login_required
@user_passes_test(lambda user: user.is_staff or user.user_user_profile.user_is_catalog())
def api_task_comment(request, task_id: int):
    """Add a comment to a task
    """
    response = get_default_response_dict()
    try:
        if request.method != "POST":
            return HttpResponseBadRequest()
        comment_text = request.POST.get('text', None)
        if comment_text is None:
            raise KeyError
        new_task_comment = TaskComment(task_id=task_id, comment=comment_text, user_id=request.user.id)
        new_task_comment.save()
    except ObjectDoesNotExist:
        return HttpResponseNotFound()
    except KeyError:
        return HttpResponseBadRequest()
    return JsonResponse(response)


@require_POST
@login_required
@user_passes_test(lambda user: user.is_staff or user.user_user_profile.user_is_catalog())
def api_task_item_toggle_status(request, task_id: int):
    """Toggle checklist status
    """

    # todo if not is staff filter task
    response = get_default_response_dict()
    try:
        if request.method != "POST":
            return HttpResponseBadRequest()
        task_item = TaskItem.objects.get(id=task_id)
        is_done = request.POST.get('is_done', None)
        if is_done is None:
            raise KeyError
        task_item.done = (is_done and is_done != "false")
        task_item.save()
    except ObjectDoesNotExist:
        return HttpResponseNotFound()
    except KeyError:
        return HttpResponseBadRequest()
    return JsonResponse(response)


# @require_POST
@login_required
@user_passes_test(lambda user: user.is_staff or user.user_user_profile.user_is_catalog)
def api_new_task(request):
    """View to create the new task and its checklists (through formsets)
    """
    # define a opcao default pra possibilitar escolha nula
    default_option = ('', '------------')
    request_user_profile = get_user_profile_from_request(request)
    if request.method != "POST":
        if not request_user_profile.user_is_catalog():
            form = TaskFrontForm()
            formset = TaskItemFrontInline()
        else:
            form = TaskFrontNoGroupForm(initial={'catalog': request_user_profile.get_user_catalog()})
            # define a lista de usuarios que o cataloguser pode ver
            assigned_to_choices = list(User.objects.filter(
                cataloguser__catalog=request_user_profile.get_user_catalog()).values_list('id', 'username'))
            form.fields['assigned_to'].choices = assigned_to_choices
            # como os catalogusers nao definem grupo, assigned_to se torna obrigatorio. por isso so adicionamos a opcao
            #  default depois que colocamos as opções de usuario no assigned to, de forma a obrigar o cataloguser a defi
            #  nir um assigned to na tarefa, mas possibilitando que ele nao crie taskitems
            assigned_to_choices.insert(0, default_option)
            formset = TaskItemNoGroupFrontInline()
            for formset_form in formset.forms:
                formset_form.fields['assigned_to'].choices = assigned_to_choices
        response = helper_populate_task_response_with_empty_front_form(form, formset)

    else:
        task = Task()
        response = process_task_form(task, request)

    return JsonResponse(response)


@login_required
@user_passes_test(lambda user: user.is_staff or user.user_user_profile.user_is_catalog)
def task_model_list(request):
    """Renderiza a pagina de listar as tarefas modelo existentes no sistema"""
    return render(request, 'tasks/task_model_list.html')


@login_required
@user_passes_test(lambda user: user.is_staff or user.user_user_profile.user_is_catalog)
def new_or_edit_task_model(request, task_id=None):
    """View to create the new task and its checklists (through formsets)
    """
    context = {}
    if request.method == 'POST':
        if task_id:
            try:
                taskmodel = TaskModel.objects.get(id=task_id)
                form = TaskModelFrontForm(instance=taskmodel, data=request.POST)
                formset = TaskModelItemFrontInline(data=request.POST, instance=taskmodel)
            except TaskModel.DoesNotExist:
                return HttpResponseNotFound()
            context['taskmodel'] = taskmodel
        else:
            form = TaskModelFrontForm(data=request.POST)
            formset = TaskModelItemFrontInline(data=request.POST)
        if form.is_valid():
            taskmodel = form.save(commit=False)
            formset = TaskModelItemFrontInline(data=request.POST, instance=taskmodel)
            if formset.is_valid():
                form.save()
                formset.save()
                messages.success(request, _('Model Task was created successfully.'))
                return redirect('tasks:taskmodels.index')
    else:
        if task_id:
            try:
                taskmodel = TaskModel.objects.get(id=task_id)
            except TaskModel.DoesNotExist:
                return HttpResponseNotFound()
            form = TaskModelFrontForm(instance=taskmodel)
            formset = TaskModelItemFrontInline(instance=taskmodel)
        else:
            form = TaskModelFrontForm()
            formset = TaskModelItemFrontInline()
    context['form'] = form
    context['formset'] = formset
    return render(request, 'tasks/task_model_new_or_edit.html', context)


@require_POST
@login_required
@user_passes_test(lambda user: user.is_staff or user.user_user_profile.user_is_catalog)
def api_edit_task(request, task_id):
    """View to edit a task and its checklists (through formsets)
    """
    request_user_profile = get_user_profile_from_request(request)
    try:
        task = Task.objects.get(id=task_id)
        response = process_task_form(task, request)
        if not request_user_profile.user_is_catalog():
            form = TaskFrontForm(initial={'assigned_to': request.user}, new_item=True)
        else:
            form = TaskFrontNoGroupForm(
                initial={'assigned_to': request.user, 'catalog': request_user_profile.get_user_catalog()},
                new_item=True)
        response['data']['items'][0]['front_form'] = render_crispy_form(form)
        # Envia a notificação para o usuário atribuído à tarefa sobre a edição da mesma
        notification_code = SystemNotification.get_task_edited_code()
        recipient = User.objects.filter(id=task.assigned_to_id,
                                        user_user_profile__profilesystemnotification__notification__code=notification_code)
        if recipient.exists():
            notify_users(notification_code, recipient, author=request.user, action_object=task,
                         url=reverse('dashboard:tasks.schedule'))
    except ObjectDoesNotExist:
        response = get_default_response_dict()
        response['data']['message'] = _('Object not found.')

    return JsonResponse(response)


def process_task_form(task: Union[Task, TaskModel], request: HttpRequest) -> dict:
    """Function to parse form and create/edit the task passed.

    The logic is the same, the only difference is how the task gets instantiated
    """
    form_data = {'data': request.POST, 'instance': task, 'new_item': False}
    formset_data = {'data': request.POST, 'instance': task}
    if not request.user.user_user_profile.user_is_catalog():
        form = TaskFrontForm(**form_data) if isinstance(task, Task) else TaskModelFrontForm(**form_data)
        formset = TaskItemFrontInline(**formset_data) if isinstance(task, Task) else TaskModelItemFrontInline(
            **formset_data)
    else:
        form = TaskFrontNoGroupForm(**form_data) if isinstance(task, Task) else TaskModelFrontNoGroupForm(**form_data)
        formset = TaskItemNoGroupFrontInline(**formset_data) if isinstance(task,
                                                                           Task) else TaskModelItemNoGroupFrontInline(
            **formset_data)
        # validating before if because and operator will not validate the second item if the first is invalid
    form_is_valid = form.is_valid()
    formset_is_valid = formset.is_valid()
    if form_is_valid and formset_is_valid:  # and inline.is_valid():
        form.save()
        formset.save()
        response = get_default_response_dict()
        response['data']['items'] = [task.get_data_for_api(True, True, request.user, True, True)]
        response['status'] = get_success_status()
        response['data']['message'] = ''
    else:
        response = helper_populate_task_response_with_empty_front_form(form, formset)
        response['status'] = get_generic_error_status()
        response['data']['message'] = _('Form not valid.')

    return response


def helper_populate_task_response_with_empty_front_form(form, formset):
    """Helper que retorna uma padrão, comn um formulário front de task e seu formset

    The logic is the same, the only difference is how the task gets instantiated
    """
    response = get_default_response_dict()
    response['data']['items'] = [{}]
    response['data']['items'][0]['front_form'] = render_crispy_form(form)
    response['data']['items'][0]['front_form_item'] = render_dynamic_crispy_formset(formset, _('Task Items'))
    response['data']['items'][0]['front_form_item_prefix'] = formset.prefix
    return response


@permission_required('tasks.add_project')
@login_required
@user_passes_test(lambda user: user.is_staff)
def new_project(request):
    """View to create the new project
    """
    # todo check client
    context = dict()
    if request.method == 'POST':
        form = ProjectForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect('dashboard:projects.index')
    else:
        form = ProjectForm()
    context['form'] = form

    return render(request, 'tasks/new_or_edit_project.html', context=context)


@permission_required('tasks.change_project')
@login_required
@user_passes_test(lambda user: user.is_staff)
def edit_project(request, project_id):
    """View to edit a project
    """
    # todo check client
    context = dict()
    project = Project.objects.get(id=project_id)
    if request.method == 'POST':
        form = ProjectForm(instance=project, data=request.POST)
        if form.is_valid():
            form.save()
            return redirect('dashboard:projects.index')
    else:
        tasks = project.get_project_valid_tasks()
        form = ProjectForm(initial=dict(
            title=project.title,
            description=project.description,
            start_date=project.start_date,
            model=project.model,
        ))
        context['tasks'] = tasks
        context['form'] = form
        context['project'] = project
        context['project_progress'] = project.tasks_progress()
        return render(request, 'tasks/new_or_edit_project.html', context=context)


@permission_required('tasks.add_projectmodel')
@login_required
@user_passes_test(lambda user: user.is_staff)
def new_project_model(request):
    """ Criar novo projeto modelo
    """
    # todo check client
    context = dict()
    if request.method == 'POST':
        form = ProjectModelForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect('dashboard:projects.index')
    else:
        form = ProjectModelForm()
    context['form'] = form

    return render(request, 'tasks/new_or_edit_project_model.html', context=context)


@permission_required('tasks.change_project')
@login_required
@user_passes_test(lambda user: user.is_staff)
def edit_project_model(request, project_id):
    """View to edit a project model
    """
    # todo check client
    context = dict()
    project = ProjectModel.objects.get(id=project_id)
    if request.method == 'POST':
        form = ProjectModelForm(instance=project, data=request.POST)
        if form.is_valid():
            form.save()
            return redirect('dashboard:projectmodels.index')
    else:
        tasks = project.get_project_valid_tasks()
        form = ProjectModelForm(initial=dict(
            title=project.title,
            description=project.description,
        ))
        context['tasks'] = tasks
        context['form'] = form
        context['project'] = project
        return render(request, 'tasks/new_or_edit_project_model.html', context=context)


@require_POST
@login_required
@permission_required('tasks.can_postpone_project')
@user_passes_test(lambda user: user.is_staff)
def postpone_project(request, project_id):
    """Adia um project por um número x de dias.
    """
    response = get_api_response_dict()
    try:
        project = Project.objects.get(id=project_id)
    except Project.DoesNotExist:
        return HttpResponseNotFound()
    log_tests(request.POST)
    days = request.POST.get('postponedays', "0")
    # postpone é None caso tenha dado tudo certo
    postpone = project.postpone_for_n_days(int(days))
    response['status'] = 500 if postpone else 200
    response['message'] = postpone or ''
    return JsonResponse(response)


@permission_required('tasks.view_project')
@login_required
@user_passes_test(lambda user: user.is_staff)
def api_list_projects(request):
    """
    Gerencia a busca dinamica do DataTables
    """
    projects = Project.query_products_by_args(
        request)  # retorna um dict com os produtos filtrados e outras info importantes pro datatables
    serialized_projects = []
    for project in projects['items']:  # projects[items] tem cada objeto projeto
        project_dict = project.get_data_for_api()
        project_dict['DT_RowId'] = project.id
        serialized_projects.append(project_dict)
    result = dict()
    result['data'] = serialized_projects
    result['draw'] = projects['draw']
    result['recordsTotal'] = projects['count']
    result['recordsFiltered'] = projects['count']
    return JsonResponse(result, status=200)


@permission_required('tasks.view_projectmodel')
@login_required
@user_passes_test(lambda user: user.is_staff)
def api_list_project_models(request):
    """
    Gerencia a busca dinamica do DataTables
    """
    projects = ProjectModel.query_products_by_args(
        request)  # retorna um dict com os produtos filtrados e outras info importantes pro datatables
    serialized_projects = []
    for project in projects['items']:  # projects[items] tem cada objeto projeto
        project_dict = project.get_data_for_api()
        project_dict['DT_RowId'] = project.id
        serialized_projects.append(project_dict)
    result = dict()
    result['data'] = serialized_projects
    result['draw'] = projects['draw']
    result['recordsTotal'] = projects['count']
    result['recordsFiltered'] = projects['count']
    return JsonResponse(result, status=200)


@permission_required('tasks.view_taskmodel')
@login_required
@user_passes_test(lambda user: user.is_staff)
def api_list_task_models(request):
    """
    Gerencia a busca dinamica do DataTables
    """
    tasks = TaskModel.query_products_by_args(request)
    serialized_tasks = []
    for task in tasks['items']:  # tasks[items] tem cada objeto projeto
        task_dict = task.get_data_for_api()
        task_dict['DT_RowId'] = task.id
        serialized_tasks.append(task_dict)
    result = dict()
    result['data'] = serialized_tasks
    result['draw'] = tasks['draw']
    result['recordsTotal'] = tasks['count']
    result['recordsFiltered'] = tasks['count']
    return JsonResponse(result, status=200)


def get_project_class_and_fields() -> Tuple[Type[Project], List[str]]:
    """Retorna o values_list e a classe Project"""
    return Project, ['id', 'title']


def get_project_model_class_and_fields() -> Tuple[Type[ProjectModel], List[str]]:
    """Retorna o values_list e a classe ProjectModel"""
    return ProjectModel, ['id', 'title']


def get_task_class_and_fields() -> Tuple[Type[Task], List[str]]:
    """Retorna o values_list e a classe Task"""
    return Task, ['id', 'title']


def get_task_model_class_and_fields() -> Tuple[Type[TaskModel], List[str]]:
    """Retorna o values_list e a classe TaskModel"""
    return TaskModel, ['id', 'title']


def get_product_class_and_fields() -> Tuple[Type[Product], List[str]]:
    """Retorna o values_list e a classe Product"""
    return Product, ['id', 'title']


def get_holder_class_and_fields() -> Tuple[Type[Holder], List[str]]:
    """Retorna o values_list e a classe Holder"""
    return Holder, ['id', 'name']


@login_required
@user_passes_test(lambda user: user.is_staff or user.user_user_profile.user_is_catalog)
def api_filter_generic(request):
    """
    Faz a busca dinamica nos select2. O javascript manda pela request os seguintes parametros:
        - filtered_class: indica qual a classe na qual vai ocorrer a busca. este nome coincide com o nome do campo dado
            no formulário, que geralmente coincide com o nome do atributo do modelo. Olhando de maneira mais alto nivel,
            o que o javascript passa neste parametro é o atributo name do elemento html.
        - search: valor a ser buscado. Os objetos sao filtrados a partir deste valor.
    Quando quisermos adicionar outro select2 dinamico devemos adicionar a classe na cadeia de elifs abaixo e colocar o
    atributo select2_dynamic no formulario (ou, caso seja um formset, colocar a classe custom_widget_select2 no widget).

    Response exigida pela api do select2:
        {results:[{"id":val,"text":option}]}
    """
    filtered_class = request.GET.get('filtered_class', None)
    classes = {
        'project': get_project_class_and_fields(),
        'filter_project': get_project_class_and_fields(),
        'project_model': get_project_model_class_and_fields(),
        'task': get_task_class_and_fields(),
        'task_model': get_task_model_class_and_fields(),
        'holder': get_holder_class_and_fields(),
        'main_holder': get_holder_class_and_fields(),
        'product': get_product_class_and_fields(),
    }
    try:
        class_to_filter, values_list_fields = classes[filtered_class]
        return default_api_get_queryset_for_select2(request, class_to_filter, values_list_fields)
    except KeyError:
        return HttpResponseBadRequest('The specified filtered class was not provided or is invalid.')


@login_required
@has_perm_custom('tasks.view_project')
def get_projects(request):
    return render(request, 'tasks/projects/index.html')


@login_required
@has_perm_custom('tasks.view_project')
def get_taskgroups(request):
    return render(request, 'tasks/taskgroup/index.html')


@login_required
@has_perm_custom('tasks.view_projectmodel')
def get_project_models(request):
    return render(request, 'tasks/project_models/index.html')

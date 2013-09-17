from django.contrib import messages
from django.contrib.admin.views.decorators import staff_member_required
from django.shortcuts import redirect, render

from rq import requeue_job
from rq.job import Job

from .queues import get_redis_connection, get_connection_queue_names, DjangoRQ, DjangoFailedRQ
from .settings import CONNECTIONS


@staff_member_required
def stats(request):
    queues = []
    for c_name, config in CONNECTIONS.items():
        connection = get_redis_connection(config)
        for q_name, workers in get_connection_queue_names(connection).items():
            q = DjangoRQ(c_name, connection_name=c_name)
            q.workers_count = workers
            queues.append(q)
        q = DjangoFailedRQ(connection_name=c_name)
        q.workers_count = "-"
        queues.append(q)

    context_data = {'queues': queues}
    return render(request, 'django_rq/stats.html', context_data)


@staff_member_required
def jobs(request, queue_connection, queue_name):
    queue = DjangoRQ(queue_name, connection_name=queue_connection)
    context_data = {
        'queue': queue,
        'queue_name': queue_name,
        'queue_connection': queue_connection,
        'jobs': queue.jobs,
    }

    return render(request, 'django_rq/jobs.html', context_data)


@staff_member_required
def job_detail(request, queue_connection, queue_name, job_id):
    queue = DjangoRQ(queue_name, connection_name=queue_connection)
    job = Job.fetch(job_id, connection=queue.connection)
    context_data = {
        'queue_name': queue_name,
        'queue_connection': queue_connection,
        'job': job,
        'queue': queue,
    }
    return render(request, 'django_rq/job_detail.html', context_data)


@staff_member_required
def delete_job(request, queue_connection, queue_name, job_id):
    queue = DjangoRQ(queue_name, connection_name=queue_connection)
    job = Job.fetch(job_id, connection=queue.connection)

    if request.POST:
        # Remove job id from queue and delete the actual job
        queue.connection._lrem(queue.key, 0, job.id)
        job.delete()
        messages.info(request, 'You have successfully deleted %s' % job.id)
        return redirect('rq_jobs', queue_connection, queue_name)

    context_data = {
        'queue_name': queue_name,
        'queue_connection': queue_connection,
        'job': job,
        'queue': queue,
    }
    return render(request, 'django_rq/delete_job.html', context_data)


@staff_member_required
def requeue_job_view(request, queue_connection, queue_name, job_id):
    queue = DjangoRQ(queue_name, connection_name=queue_connection)
    job = Job.fetch(job_id, connection=queue.connection)
    if request.POST:
        requeue_job(job_id, connection=queue.connection)
        messages.info(request, 'You have successfully requeued %s' % job.id)
        return redirect('rq_job_detail', queue_connection, queue_name, job_id)

    context_data = {
        'queue_name': queue_name,
        'queue_connection': queue_connection,
        'job': job,
        'queue': queue,
    }
    return render(request, 'django_rq/delete_job.html', context_data)

from django.core.management.base import BaseCommand
from optparse import make_option
from django_rq import get_scheduler, get_queue


class Command(BaseCommand):
    """
    Runs RQ scheduler
    """
    help = __doc__
    args = '<connection.queue>'

    option_list = BaseCommand.option_list + (
        make_option(
            '--interval',
            type=int,
            dest='interval',
            default=60,
            help="How often the scheduler checks for new jobs to add to the "
                 "queue (in seconds).",
        ),
    )

    def handle(self, queue='default', *args, **options):
        if "." in queue:
            connection_name, queue = queue.split('.')
        else:
            connection_name = 'default'
        scheduler = get_scheduler(connection_name, queue, options.get('interval'))
        scheduler.run()

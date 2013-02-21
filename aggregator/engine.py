import gevent
from gevent.queue import JoinableQueue
from gevent.pool import Group

from aggregator import logger


class AlreadyDoneError(Exception):
    pass


class Engine(object):

    def __init__(self, sequence, history, phase_hook=None, batch_size=100,
                 force=False):
        self.sequence = sequence
        self.history = history
        self.queue = JoinableQueue()
        self.phase_hook = phase_hook
        self.batch_size = batch_size
        self.force = force

    def _push_to_target(self, targets):
        """Get a batch of elements from the queue, and push it to the targets.

        This function returns True if it proceeded all the elements in the queue,
        and there isn't anything more to read.
        """
        if self.queue.empty():
            return False    # nothing

        batch = []
        eoq = False

        # collecting a batch
        while len(batch) < self.batch_size:
            try:
                item = self.queue.get()

                if item == 'END':
                    # reached the end
                    eoq = True
                    break
                batch.append(item)
            finally:
                self.queue.task_done()

        if len(batch) != 0:
            greenlets = Group()
            for plugin in targets:
                greenlets.spawn(self._put_data, plugin, batch)

            greenlets.join()
        return eoq

    def _put_data(self, callable, data):
        return callable(data)

    def _get_data(self, callable, start_date, end_date):
        try:
            for item in callable(start_date, end_date):
                self.queue.put(item)
        finally:
            self.queue.put('END')

    def run(self, start_date, end_date):
        for phase, sources, targets in self.sequence:
            for source in sources:
                if (self.history.exists(source, start_date, end_date)
                    and not self.force):
                    raise AlreadyDoneError()

            logger.info('Running phase %r' % phase)
            greenlets = Group()

            # each callable will push its result in the queue
            for source in sources:
                greenlets.spawn(self._get_data, source, start_date,
                                end_date)

            # looking at the queue
            processed = 0
            while processed < len(sources):
                eoq = self._push_to_target(targets)
                if eoq:
                    processed += 1
                gevent.sleep(0)

            greenlets.join()

            # if we reach this point we can consider the transaction a success
            # for these sources
            self.history.add_entry(sources, start_date, end_date)

class AlreadyDoneError(Exception):
    pass


class InjectError(Exception):
    pass


class ExtractError(Exception):
    pass


class ServerError(Exception):
    pass


class RunError(Exception):
    def __init__(self, errors):
        self.errors = errors

    def __str__(self):
        msg = '%d failures\n\n' % len(self.errors)

        for index, (error, plugin, greenlet) in enumerate(self.errors):
            msg += '%d. %s in %s. error: %s' % (index + 1, error, plugin,
                                                greenlet.exception)
        return msg

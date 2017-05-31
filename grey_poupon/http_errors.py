
def parse_status():
    pass


class HTTPError(Exception):

    def __init__(self, expression):
        self.expression = expression
        self.message = None
        self.status = None


class BadRequest(HTTPError):

    def __init__(self, expression):
        self.message = 'The request could not be understood or was missing required parameters.'
        self.status = status = 400
        super().__init__(expression)


class UnauthorizedRequest(HTTPError):

    def __init__(self, expression):
        self.message = 'Authentication failed or user does not have permissions for the requested operation.'
        self.status = 401
        super().__init__(expression)


class AccessDenied(HTTPError):

    def __init__(self, expression):
        self.message = 'Access denied to this resource.'
        self.status = 403
        super().__init__(expression)


class ResourceNotFound(HTTPError):

    def __init__(self, expression):
        self.message = 'Resource was not found.'
        self.status = 404
        super().__init__(expression)


class MethodNotAllowed(HTTPError):

    def __init__(self, expression):
        self.message = 'Requested method is not supported for the specified resource.'
        self.status = 405
        super().__init__(expression)


class TooManyRequests(HTTPError):

    def __init__(self, expression):
        self.message = 'Exceeded GoodData API limits.'
        self.status = 429
        super().__init__(expression)


class ServiceUnavailable(HTTPError):

    def __init__(self, expression):
        self.message = 'The service is temporary unavailable. Try again later.'
        self.status = 503
        super().__init__(expression)
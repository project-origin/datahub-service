import json
from flask import request, redirect, Response
from marshmallow import ValidationError
from werkzeug.exceptions import HTTPException, BadRequest, Unauthorized


class Controller(object):
    """
    Base class for http controllers, written specifically for Flask.
    """

    METHOD = 'POST'

    # Request Schema
    Request = None

    # Response Schema
    Response = None

    def handle_request(self, **kwargs):
        """
        Abstract function to handle the HTTP request. Overwritten by subclassing.
        """
        raise NotImplementedError

    def __call__(self):
        """
        Invoked by Flask to handle a HTTP request.
        """
        try:
            kwargs = {}
            req = self.get_request_vm()

            if req is not None:
                kwargs['request'] = req

            handler_response = self.handle_request(**kwargs)
            response = self.parse_response(handler_response)

            if isinstance(response, str):
                return Response(
                    status=200,
                    mimetype='application/json',
                    response=response,
                )
            else:
                return response
        except HTTPException as e:
            return Response(
                status=e.code,
                mimetype='application/json',
                response=json.dumps({
                    'success': False,
                    'message': e.description,
                }),
            )

    def get_request_vm(self):
        """
        Converts JSON provided in the request body according to the Schema
        defined on self.Request (if any), and returns the model instance.

        Returns None if self.Requests is None.

        :rtype: obj
        """
        if self.Request is not None:
            schema = self.Request()

            if self.METHOD == 'POST':
                if not request.data:
                    raise BadRequest('No JSON body provided')

                try:
                    params = json.loads(request.data)
                except json.JSONDecodeError:
                    raise BadRequest('Bad JSON body provided')
            elif self.METHOD == 'GET':
                params = request.args
            else:
                raise NotImplementedError

            try:
                req = schema.load(params)
            except ValidationError as e:
                raise BadRequest(e.messages)

            return req

    def parse_response(self, response):
        """
        Converts the return value of handle_request() into a HTTP response
        body.

        :param obj response: The object returned by handle_request()
        :rtype: str
        :returns: HTTP response body
        """
        if response is None:
            return ''
        elif response in (True, False):
            return json.dumps({'success': response})
        elif isinstance(response, dict):
            return json.dumps(response)
        elif self.Response is not None:
            return json.dumps(self.Response().dump(response))
        else:
            return response

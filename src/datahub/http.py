import json
from flask import request, redirect, Response
from marshmallow import ValidationError
from werkzeug.exceptions import HTTPException, BadRequest, Unauthorized


class Controller(object):
    """
    Base class for http controllers, written specifically for Flask.
    """

    METHOD = 'POST'

    def handle_request(self, **kwargs):
        """
        TODO

        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def __call__(self):
        """
        TODO

        :return:
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
        TODO

        :rtype: obj
        """
        if hasattr(self, 'Request'):
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
        TODO

        Converts the return value of handle_request() into a HTTP response
        body. Raises a ValueError if handle_request() returned an invalid
        object.

        Rules are:

        - None converts to an empty string
        - Dictionaries are encoded as JSON
        - ViewModel instances are mapped to JSON using the self.Response schema
        - Boolean values converts into {"status": True|False}
        - Anything else raises a ValueError

        :param obj response: The object returned by handle_request()
        :return str: HTTP response body
        """
        if response is None:
            return ''
        elif response in (True, False):
            return json.dumps({'success': response})
        elif isinstance(response, dict):
            return json.dumps(response)
        elif hasattr(self, 'Response'):
            return json.dumps(self.Response().dump(response))
        else:
            return response

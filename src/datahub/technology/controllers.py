import marshmallow_dataclass as md

from datahub.db import inject_session
from datahub.http import Controller

from .models import Technology, GetTechnologiesResponse


class GetTechnologies(Controller):
    """
    TODO
    """
    Response = md.class_schema(GetTechnologiesResponse)

    @inject_session
    def handle_request(self, session):
        """
        :param Session session:
        :rtype: GetTechnologiesResponse
        """
        return GetTechnologiesResponse(
            success=True,
            technologies=session.query(Technology).all(),
        )

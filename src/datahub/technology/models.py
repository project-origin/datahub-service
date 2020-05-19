import sqlalchemy as sa
from dataclasses import dataclass, field
from typing import List

from datahub.db import ModelBase


class Technology(ModelBase):
    """
    A technology (by label) consists of a combination
    of technology_code and fuel_code.
    """
    __tablename__ = 'technology'
    __table_args__ = (
        sa.UniqueConstraint('technology_code', 'fuel_code'),
    )

    id = sa.Column(sa.Integer(), primary_key=True, index=True)
    technology = sa.Column(sa.String(), nullable=False)
    technology_code = sa.Column(sa.String(), nullable=False)
    fuel_code = sa.Column(sa.String(), nullable=False)


@dataclass
class MappedTechnology:
    technology: str
    technology_code: str = field(metadata=dict(data_key='technologyCode'))
    fuel_code: str = field(metadata=dict(data_key='fuelCode'))


# -- GetTechnologies request and response ------------------------------------


@dataclass
class GetTechnologiesResponse:
    success: bool
    technologies: List[MappedTechnology] = field(default_factory=list)

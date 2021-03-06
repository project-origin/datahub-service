import sys
import csv
import fire
from sqlalchemy.orm.exc import NoResultFound

from datahub.db import inject_session
from datahub.technology import Technology
from datahub.meteringpoints import MeteringPointQuery
from datahub.pipelines import (
    start_import_meteringpoints_pipeline,
    start_import_energy_type_pipeline,
    start_import_measurements_pipeline,
    start_import_measurements_pipeline_for,
    start_resubmit_measurements_pipeline,
)


class PipelineTriggers(object):
    def resubmit_measurements(self):
        start_resubmit_measurements_pipeline()

    def import_measurements(self):
        start_import_measurements_pipeline()

    @inject_session
    def import_measurements_for(self, gsrn, session):
        try:
            meteringpoint = MeteringPointQuery(session) \
                .has_gsrn(str(gsrn)) \
                .one()
        except NoResultFound:
            raise NoResultFound(f'Could not find MeteringPoint with GSRN = {gsrn}')

        start_import_measurements_pipeline_for(
            subject=meteringpoint.sub,
            gsrn=meteringpoint.gsrn,
        )

    def import_meteringpoints_for(self, subject):
        start_import_meteringpoints_pipeline(subject)

    @inject_session
    def import_energy_type_for(self, gsrn, session):
        try:
            meteringpoint = MeteringPointQuery(session) \
                .has_gsrn(str(gsrn)) \
                .is_production() \
                .one()
        except NoResultFound:
            raise NoResultFound(f'Could not find [production] MeteringPoint with GSRN = {gsrn}')

        start_import_energy_type_pipeline(
            subject=meteringpoint.sub,
            gsrn=meteringpoint.gsrn,
            session=session,
        )

    @inject_session
    def export_technologies(self, session):
        writer = csv.writer(sys.stdout)
        writer.writerow(('technology_code', 'fuel_code', 'technology'))

        for t in session.query(Technology):
            writer.writerow((t.technology_code, t.fuel_code, t.technology))


if __name__ == '__main__':
    fire.Fire(PipelineTriggers)

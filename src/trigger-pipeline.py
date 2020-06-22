import fire
from sqlalchemy.orm.exc import NoResultFound

from datahub.db import inject_session
from datahub.meteringpoints import MeteringPointQuery
from datahub.pipelines import (
    start_import_meteringpoints_pipeline,
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
            metering_point = MeteringPointQuery(session) \
                .has_gsrn(str(gsrn)) \
                .one()
        except NoResultFound:
            raise NoResultFound(f'Could not find MeteringPoint with GSRN = {gsrn}')

        start_import_measurements_pipeline_for(
            metering_point.sub, metering_point.gsrn)

    @inject_session
    def import_meteringpoints_for(self, subject, session):
        start_import_meteringpoints_pipeline(subject, session)


if __name__ == '__main__':
    fire.Fire(PipelineTriggers)

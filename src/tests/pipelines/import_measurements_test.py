# import os
# import time
# from itertools import product, cycle
# from uuid import uuid4
#
# import pytest
# import origin_ledger_sdk as ols
#
# from celery.backends.redis import RedisBackend
# from origin_ledger_sdk.ledger_connector import BatchStatusResponse
# from testcontainers.compose import DockerCompose
# from unittest.mock import patch, DEFAULT
# from datetime import datetime, timezone, timedelta
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker
#
# from datahub.db import ModelBase
# from datahub.ggo import Ggo
# from datahub.measurements import Measurement
# from datahub.meteringpoints import MeteringPoint, MeasurementType
# from datahub.tasks import celery_app
# from datahub.webhooks import WebhookSubscription, WebhookEvent
# from datahub.pipelines.import_measurements import (
#     start_import_measurements_pipeline,
#     InvalidBatch,
#     get_distinct_gsrn)
#
#
# PIPELINE_TIMEOUT = 60 * 5  # Seconds
# CURRENT_FOLDER = os.path.split(os.path.abspath(__file__))[0]
#
#
# sub1 = 'SUB1'
# sub2 = 'SUB2'
#
# gsrn1 = 'GSRN1'
# gsrn2 = 'GSRN2'
# gsrn3 = 'GSRN3'
# gsrn4 = 'GSRN4'
#
#
# meteringpoint1 = MeteringPoint(
#     sub=sub1,
#     gsrn=gsrn1,
#     type=MeasurementType.PRODUCTION,
#     sector='DK1',
#     technology_code='T010101',
#     fuel_code='F01010101',
#     ledger_extended_key=(
#         'xprv9s21ZrQH143K2CK5syo8PdeX5Y4TYFkcU'
#         'KonHhm1e7znhaKj6odQFbbBa7T2Y77AtiNmU6'
#         'aatP2qJBTwvhqxvaSBHA9hEfZ5gViAS3bBj7F'
#     ),
# )
#
# meteringpoint2 = MeteringPoint(
#     sub=sub1,
#     gsrn=gsrn2,
#     type=MeasurementType.CONSUMPTION,
#     sector='DK2',
#     technology_code='T010101',
#     fuel_code='F01010101',
#     ledger_extended_key=(
#         'xprv9s21ZrQH143K4WQcTeFMi8gfrgHYuoFH2'
#         '63xo4YPAqMN6RGc2BJeAghBtcxf1BzQz81ynY'
#         'fZpchrt3tGRBpQn1jp1bNH41AisDWfKQi57MM'
#     ),
# )
#
# meteringpoint3 = MeteringPoint(
#     sub=sub2,
#     gsrn=gsrn3,
#     type=MeasurementType.PRODUCTION,
#     sector='DK1',
#     technology_code='T010101',
#     fuel_code='F01010101',
#     ledger_extended_key=(
#         'xprv9s21ZrQH143K2SJ98GWKgbEemXLA6SShS'
#         'iNTuCAPAeM9RfdYqpqxLxp4ogPSvYfv6tfdSJ'
#         'dQo1WTPMatwovVBuWgyBi1RewZC7JUFY9y5Ww'
#     ),
# )
#
# meteringpoint4 = MeteringPoint(
#     sub=sub2,
#     gsrn=gsrn4,
#     type=MeasurementType.CONSUMPTION,
#     sector='DK2',
#     technology_code='T010101',
#     fuel_code='F01010101',
#     ledger_extended_key=(
#         'xprv9s21ZrQH143K3twCsVmArteJpkTDmFyz8'
#         'p74RhZW27GpTQcAsKUsdTfE17oRLKdHWfRKwm'
#         'sPERLVFBL7ucPYthR7TNuN11yQyfPgkU3wfC6'
#     ),
# )
#
#
# subscription1 = WebhookSubscription(
#     id=1,
#     event=WebhookEvent.ON_GGOS_ISSUED,
#     subject=sub1,
#     url='http://something.com',
#     secret='something',
# )
#
# subscription2 = WebhookSubscription(
#     id=2,
#     event=WebhookEvent.ON_MEASUREMENT_PUBLISHED,
#     subject=sub1,
#     url='http://something-else.com',
#     secret='something',
# )
#
# subscription3 = WebhookSubscription(
#     id=3,
#     event=WebhookEvent.ON_GGOS_ISSUED,
#     subject=sub2,
#     url='http://something.com',
#     secret='something',
# )
#
# subscription4 = WebhookSubscription(
#     id=4,
#     event=WebhookEvent.ON_MEASUREMENT_PUBLISHED,
#     subject=sub2,
#     url='http://something-else.com',
#     secret='something',
# )
#
#
# def seed_data(session):
#     session.add(meteringpoint1)
#     session.add(meteringpoint2)
#     session.add(meteringpoint3)
#     session.add(meteringpoint4)
#     session.add(subscription1)
#     session.add(subscription2)
#     session.add(subscription3)
#     session.add(subscription4)
#
#     # Seed measurements
#     # These are "imported" from ElOverblik by the mock importer (ie. inserted
#     # into the database during a job, and returned by the importer)
#     begins = (
#         datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
#         datetime(2020, 1, 1, 1, 0, 0, tzinfo=timezone.utc),
#     )
#
#     for m in (meteringpoint1, meteringpoint2, meteringpoint3, meteringpoint4):
#         for begin in begins:
#             measurement = Measurement(
#                 gsrn=m.gsrn,
#                 begin=begin,
#                 end=begin + timedelta(hours=1),
#                 amount=100,
#                 published=False,
#             )
#
#             session.add(measurement)
#
#             if m.type is MeasurementType.PRODUCTION:
#                 session.add(Ggo(
#                     measurement=measurement,
#                     issue_time=begin,
#                     expire_time=begin,
#                 ))
#
#     session.flush()
#     session.commit()
#
#
# @pytest.fixture(scope='session')
# def compose():
#     """
#     Returns a Session object with Ggo + User data seeded for testing
#     """
#     with DockerCompose(CURRENT_FOLDER) as compose:
#         yield compose
#
#
# @pytest.fixture(scope='session')
# def session(compose):
#     """
#     Returns a Session object with Ggo + User data seeded for testing
#     """
#     host = compose.get_service_host('postgres-test', 5432)
#     port = compose.get_service_port('postgres-test', 5432)
#     url = f'postgresql://postgres:postgres@{host}:{port}/postgres'
#
#     engine = create_engine(url)
#     ModelBase.metadata.create_all(engine)
#     Session = sessionmaker(bind=engine, expire_on_commit=False)
#
#     session1 = Session()
#     seed_data(session1)
#     session1.close()
#
#     session2 = Session()
#     yield session2
#     session2.close()
#
#
# @pytest.fixture(scope='session')
# def celery_config(compose):
#     redis_host = compose.get_service_host('redis-test', 6379)
#     redis_port = compose.get_service_port('redis-test', 6379)
#     redis_url = f'redis://:@{redis_host}:{redis_port}'
#
#     REDIS_BROKER_URL = f'{redis_url}/0'
#
#     celery_app.backend = RedisBackend(app=celery_app, url=f'{redis_url}/1')
#     celery_app.conf.broker_url = REDIS_BROKER_URL
#     celery_app.conf.broker_read_url = REDIS_BROKER_URL
#     celery_app.conf.broker_write_url = REDIS_BROKER_URL
#
#     return {
#         'broker_url': f'{redis_url}/0',
#         'result_backend': f'{redis_url}/1',
#     }
#
#
# # -- Constructor -------------------------------------------------------------
#
#
# def wait_recursively(task):
#     """
#     Waits for a task + all of its children (recursively) to complete.
#
#     :param celery.Task task:
#     """
#     time.sleep(1)
#     task.get(timeout=PIPELINE_TIMEOUT)
#     if task.children:
#         for child in task.children:
#             wait_recursively(child)
#
#
# @patch('datahub.db.make_session')
# @patch('datahub.pipelines.import_measurements.ledger')
# @patch('datahub.pipelines.import_measurements.importer')
# @patch('datahub.pipelines.import_measurements.webhook_service.on_measurement_published')
# @patch('datahub.pipelines.import_measurements.webhook_service.on_ggo_issued')
# @pytest.mark.usefixtures('celery_session_worker')
# def test__handle_composed_ggo__happy_path__should_publish_measurements_and_issue_ggos_and_invoke_webhooks(
#         on_ggo_issued_mock, on_measurement_published_mock, importer_mock,
#         ledger_mock, make_session_mock, session):
#
#     make_session_mock.return_value = session
#
#     # -- Arrange -------------------------------------------------------------
#
#     def __import_measurements_for(meteringpoint, session):
#         return session.query(Measurement).filter_by(gsrn=meteringpoint.gsrn).all()
#
#     importer_mock.import_measurements_for.side_effect = __import_measurements_for
#
#     # Executing batch: Raises Ledger exceptions a few times, then returns Handle
#     ledger_mock.execute_batch.side_effect = cycle((
#         # ols.LedgerConnectionError(),
#         # ols.LedgerException('', code=15),
#         # ols.LedgerException('', code=17),
#         # ols.LedgerException('', code=18),
#         # ols.LedgerException('', code=31),
#         'LEDGER-HANDLE',
#     ))
#
#     # Getting batch status: Raises LedgerConnectionError once, then returns BatchStatuses
#     ledger_mock.get_batch_status.side_effect = cycle((
#         # ols.LedgerConnectionError(),
#         # BatchStatusResponse(id='', status=ols.BatchStatus.UNKNOWN),
#         # BatchStatusResponse(id='', status=ols.BatchStatus.PENDING),
#         BatchStatusResponse(id='', status=ols.BatchStatus.COMMITTED),
#     ))
#
#     # -- Act -----------------------------------------------------------------
#
#     pipeline = start_import_measurements_pipeline()
#
#     # -- Assert --------------------------------------------------------------
#
#     # Wait for pipeline + linked tasks to finish
#     wait_recursively(pipeline)
#
#     assert 1 == 2
#
#
#     # make_session_mock.return_value = session
#     # build_ledger_batch.return_value = 'LEDGER BATCH'
#     #
#     # # Executing batch: Raises Ledger exceptions a few times, then returns Handle
#     # ledger_mock.execute_batch.side_effect = (
#     #     ols.LedgerConnectionError(),
#     #     ols.LedgerException('', code=15),
#     #     ols.LedgerException('', code=17),
#     #     ols.LedgerException('', code=18),
#     #     ols.LedgerException('', code=31),
#     #     'LEDGER-HANDLE',
#     # )
#     #
#     # # Getting batch status: Raises LedgerConnectionError once, then returns BatchStatuses
#     # ledger_mock.get_batch_status.side_effect = (
#     #     ols.LedgerConnectionError(),
#     #     BatchStatusResponse(id='', status=ols.BatchStatus.UNKNOWN),
#     #     BatchStatusResponse(id='', status=ols.BatchStatus.PENDING),
#     #     BatchStatusResponse(id='', status=ols.BatchStatus.COMMITTED),
#     # )
#     #
#     # # -- Act -----------------------------------------------------------------
#     #
#     # pipeline = start_import_measurements_pipeline()
#     #
#     # # -- Assert --------------------------------------------------------------
#     #
#     # # Wait for pipeline + linked tasks to finish
#     # pipeline.get(timeout=PIPELINE_TIMEOUT)
#     # [c.get(timeout=PIPELINE_TIMEOUT) for c in pipeline.children]
#     #
#     # # ledger.execute_batch()
#     # assert ledger_mock.execute_batch.call_count == 6
#     # assert all(args == (('LEDGER BATCH',),) for args in ledger_mock.execute_batch.call_args_list)
#     #
#     # # ledger.get_batch_status()
#     # assert ledger_mock.get_batch_status.call_count == 4
#     # assert all(args == (('LEDGER-HANDLE',),) for args in ledger_mock.get_batch_status.call_args_list)
#     #
#     # # webhook_service.on_ggo_received_mock()
#     # assert on_ggo_received_mock.call_count == 4
#     #
#     # for split_target in batch.transactions[0].targets:
#     #     if split_target.ggo.user_id == user2.id:
#     #         assert any(
#     #             isinstance(args[0][0], WebhookSubscription) and
#     #             args[0][0].id == subscription1.id and
#     #             isinstance(args[0][1], Ggo) and
#     #             args[0][1].id == split_target.ggo.id
#     #             for args in on_ggo_received_mock.call_args_list
#     #         )
#     #         assert any(
#     #             isinstance(args[0][0], WebhookSubscription) and
#     #             args[0][0].id == subscription2.id and
#     #             isinstance(args[0][1], Ggo) and
#     #             args[0][1].id == split_target.ggo.id
#     #             for args in on_ggo_received_mock.call_args_list
#     #         )
#     #     elif split_target.ggo.user_id == user3.id:
#     #         assert any(
#     #             isinstance(args[0][0], WebhookSubscription) and
#     #             args[0][0].id == subscription3.id and
#     #             isinstance(args[0][1], Ggo) and
#     #             args[0][1].id == split_target.ggo.id
#     #             for args in on_ggo_received_mock.call_args_list
#     #         )
#     #         assert any(
#     #             isinstance(args[0][0], WebhookSubscription) and
#     #             args[0][0].id == subscription4.id and
#     #             isinstance(args[0][1], Ggo) and
#     #             args[0][1].id == split_target.ggo.id
#     #             for args in on_ggo_received_mock.call_args_list
#     #         )
#     #     else:
#     #         raise RuntimeError
#     #
#     # # Batch state after pipeline completes
#     # batch = session.query(Batch).filter_by(id=batch.id).one()
#     # assert batch.state is BatchState.COMPLETED

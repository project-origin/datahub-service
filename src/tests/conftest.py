"""
conftest.py according to pytest docs:
https://docs.pytest.org/en/2.7.3/plugins.html?highlight=re#conftest-py-plugins
"""
import os
import pytest
from unittest.mock import Mock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from datahub.db import ModelBase
from datahub.settings import TEST_SQLITE_PATH
from datahub.auth import User
from datahub.facilities import Facility
from datahub.commodities import GgoHelper, GgoIssuingController
from datahub.trading import (
    TradeController,
    AgreementController,
    PriorityDistributor,
)


@pytest.fixture(scope='function')
def session():
    """
    Creates a new empty database and applies the database schema on it.
    When test is complete, deletes the database file to start over
    in the next test.

    If one wants to use an actual PostgreSQL/MySQL database for testing
    in stead of SQLite, using testing.postgresql or testing.mysql,
    this can be implemented in the setup/teardown here.
    """

    # Setup
    if os.path.isfile(TEST_SQLITE_PATH):
        os.remove(TEST_SQLITE_PATH)

    test_db_uri = 'sqlite:///%s' % TEST_SQLITE_PATH
    engine = create_engine(test_db_uri, echo=False)
    ModelBase.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    session = scoped_session(factory)()

    # Yield session executes the dependent test/fixture
    yield session

    # Teardown
    session.close()
    os.remove(TEST_SQLITE_PATH)


@pytest.fixture()
def ledger_mock():
    """
    :return OriginLedger:
    """
    return Mock()


@pytest.fixture()
def helper(session, ledger_mock):
    """
    :return GgoHelper:
    """
    return GgoHelper(session, ledger_mock)


@pytest.fixture()
def issuing_controller(session, helper, ledger_mock):
    """
    :return GgoIssuingController:
    """
    return GgoIssuingController(session, helper, ledger_mock)


@pytest.fixture()
def distributor(helper):
    """
    :return TradeController:
    """
    return PriorityDistributor(helper)


@pytest.fixture()
def trade_controller(helper, ledger_mock, distributor):
    """
    :return TradeController:
    """
    return TradeController(helper, ledger_mock, distributor)


@pytest.fixture()
def agreement_controller(session):
    """
    :return AgreementController:
    """
    return AgreementController(session)


# -- Users -------------------------------------------------------------------

@pytest.fixture(scope='function')
def user1():
    """
    :param Session session:
    :return User:
    """
    return User(
        name='User 1',
        email='user1@user.com',
        password='a2958ec16247ad36c229546487296c93',
    )


@pytest.fixture(scope='function')
def user2():
    """
    :param Session session:
    :return User:
    """
    return User(
        name='User 2',
        email='user2@user.com',
        password='f3bad9b281e199dc1308afa9b98db7d2',
    )


# -- Facilities --------------------------------------------------------------

@pytest.fixture(scope='function')
def facility1(session, user1):
    """
    :param Session session:
    :param User user1:
    :return Facility:
    """
    return Facility(
        user=user1,
        gsrn='000000000000000001',
        facility_type=Facility.TYPE_PRODUCTION,
        technology='Solar',
        sector='DK1',
    )


@pytest.fixture(scope='function')
def facility2(session, user1):
    """
    :param Session session:
    :param User user1:
    :return Facility:
    """
    return Facility(
        user=user1,
        gsrn='000000000000000002',
        facility_type=Facility.TYPE_PRODUCTION,
        technology='Solar',
        sector='DK1',
    )


@pytest.fixture(scope='function')
def facility3(session, user1):
    """
    :param Session session:
    :param User user1:
    :return Facility:
    """
    return Facility(
        user=user1,
        gsrn='000000000000000003',
        facility_type=Facility.TYPE_PRODUCTION,
        technology='Solar',
        sector='DK1',
    )

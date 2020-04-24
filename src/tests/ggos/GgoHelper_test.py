"""
Testing og GgoHelper using an empty SQLite database.
"""

import pytest
from unittest.mock import Mock
from datetime import datetime

from datahub.facilities import Facility
from datahub.commodities import Ggo, GgoHelper


@pytest.fixture()
def helper(session):
    """
    :param Session session:
    :return GgoHelper:
    """
    return GgoHelper(session=session, ledger=Mock())


# -- Helper functions --------------------------------------------------------

def assert_summary(helper, facility, period, technology,
                   issued=0, inbound=0, outbound=0,
                   stored=0, retired=0, expired=0):
    """
    :param GgoHelper helper:
    :param Facility facility:
    :param datetime period:
    :param str technology:
    :param int issued:
    :param int inbound:
    :param int outbound:
    :param int stored:
    :param int retired:
    :param int expired:
    """
    summary = helper.get_ggo_distributions(facility, period, technology)
    assert summary.issued == issued
    assert summary.inbound == inbound
    assert summary.outbound == outbound
    assert summary.stored == stored
    assert summary.retired == retired
    assert summary.expired == expired


# -- GgoHelper.issue() tests -------------------------------------------------

@pytest.mark.parametrize('amount', (-1, 0))
def test__GgoHelper__issue__ggo_with_invalid_amount__should_raise_assertion_error(helper, amount):
    """
    :param GgoHelper helper:
    :param int amount:
    """
    # Arrange
    ggo = Mock()
    ggo.amount = amount

    # Act + Assert
    with pytest.raises(AssertionError):
        helper.issue(ggo, Mock())


def test__GgoHelper__issue__ggo_and_facility_has_different_sectors__should_raise_assertion_error(helper):
    """
    :param GgoHelper helper:
    """
    # Arrange
    facility = Mock()
    facility.sector = 'DK1'

    ggo = Mock()
    ggo.amount = 100
    ggo.tradable = True
    ggo.sector = 'DK2'

    # Act + Assert
    with pytest.raises(AssertionError):
        helper.transfer(ggo, facility)


def test__GgoHelper__issue__issue_one_valid_ggo__should_insert_to_db_and_update_summary(session, helper, facility1):
    """
    :param Session session:
    :param GgoHelper helper:
    :param Facility facility1:
    """
    # Arrange
    ggo = Ggo()
    ggo.period = datetime(2020, 1, 1, 0, 0, 0)
    ggo.sector = facility1.sector
    ggo.amount = 100
    ggo.unit = 'Wh'
    ggo.technology = 'Solar'

    # Act
    helper.issue(ggo, facility1)

    # Assert
    ggo_from_db = session.query(Ggo).one()

    assert ggo_from_db == ggo
    assert ggo_from_db.facility == facility1
    assert ggo_from_db.tradable is True

    assert_summary(helper, facility1, ggo.period, ggo.technology, issued=100, stored=100)


def test__GgoHelper__issue__issue_two_valid_ggos_same_period__should_insert_to_db_and_update_summary(session, helper, facility1):
    """
    :param Session session:
    :param GgoHelper helper:
    :param Facility facility1:
    """
    # Arrange
    ggo1 = Ggo()
    ggo1.period = datetime(2020, 1, 1, 0, 0, 0)
    ggo1.sector = facility1.sector
    ggo1.amount = 100
    ggo1.unit = 'Wh'
    ggo1.technology = 'Solar'

    ggo2 = Ggo()
    ggo2.period = datetime(2020, 1, 1, 0, 0, 0)
    ggo2.sector = facility1.sector
    ggo2.amount = 100
    ggo2.unit = 'Wh'
    ggo2.technology = 'Solar'

    # Act
    helper.issue(ggo1, facility1)
    helper.issue(ggo2, facility1)

    # Assert
    query = session.query(Ggo).order_by(Ggo.id.asc())
    ggo1_from_db, ggo2_from_db = query.all()

    assert query.count() == 2

    assert ggo1_from_db == ggo1
    assert ggo1_from_db.facility == facility1
    assert ggo1_from_db.tradable is True

    assert ggo2_from_db == ggo2
    assert ggo2_from_db.facility == facility1
    assert ggo2_from_db.tradable is True

    assert_summary(helper, facility1, ggo1.period, ggo1.technology, issued=200, stored=200)


def test__GgoHelper__issue__issue_two_valid_ggos_different_periods__should_insert_to_db_and_update_summaries_independently(session, helper, facility1):
    """
    :param Session session:
    :param GgoHelper helper:
    :param Facility facility1:
    """
    # Arrange
    ggo1 = Ggo()
    ggo1.period = datetime(2020, 1, 1, 0, 0, 0)
    ggo1.sector = facility1.sector
    ggo1.amount = 100
    ggo1.unit = 'Wh'
    ggo1.technology = 'Solar'

    ggo2 = Ggo()
    ggo2.period = datetime(2020, 1, 1, 2, 0, 0)
    ggo2.sector = facility1.sector
    ggo2.amount = 100
    ggo2.unit = 'Wh'
    ggo2.technology = 'Solar'

    # Act
    helper.issue(ggo1, facility1)
    helper.issue(ggo2, facility1)

    # Assert
    query = session.query(Ggo).order_by(Ggo.id.asc())
    ggo1_from_db, ggo2_from_db = query.all()

    assert query.count() == 2

    assert ggo1_from_db == ggo1
    assert ggo1_from_db.facility == facility1
    assert ggo1_from_db.tradable is True

    assert ggo2_from_db == ggo2
    assert ggo2_from_db.facility == facility1
    assert ggo2_from_db.tradable is True

    assert_summary(helper, facility1, ggo1.period, ggo1.technology, issued=100, stored=100)
    assert_summary(helper, facility1, ggo2.period, ggo2.technology, issued=100, stored=100)


# -- GgoHelper.transfer() tests ----------------------------------------------

@pytest.mark.parametrize('amount', (-1, 0))
def test__GgoHelper__transfer__ggo_with_invalid_amount__should_raise_assertion_error(helper, amount):
    """
    :param GgoHelper helper:
    :param int amount:
    """
    # Arrange
    ggo = Mock()
    ggo.amount = amount

    # Act + Assert
    with pytest.raises(AssertionError):
        helper.transfer(ggo, Mock())


def test__GgoHelper__transfer__ggo_is_not_tradable__should_raise_assertion_error(helper):
    """
    :param GgoHelper helper:
    """
    # Arrange
    ggo = Mock()
    ggo.amount = 100
    ggo.tradable = False

    # Act + Assert
    with pytest.raises(AssertionError):
        helper.transfer(ggo, Mock())


def test__GgoHelper__transfer__tranferring_to_same_facility__should_raise_assertion_error(helper):
    """
    :param GgoHelper helper:
    """
    # Arrange
    facility = Mock()

    ggo = Mock()
    ggo.amount = 100
    ggo.tradable = True
    ggo.facility = facility

    # Act + Assert
    with pytest.raises(AssertionError):
        helper.transfer(ggo, facility)


def test__GgoHelper__transfer__tranferring_between_sectors__should_raise_assertion_error(helper):
    """
    :param GgoHelper helper:
    """
    # Arrange
    facility = Mock()
    facility.sector = 'DK1'

    ggo = Mock()
    ggo.amount = 100
    ggo.tradable = True
    ggo.sector = 'DK2'

    # Act + Assert
    with pytest.raises(AssertionError):
        helper.transfer(ggo, facility)


def test__GgoHelper__transfer__issue_and_transfer_valid_ggo__should_insert_new_ggo_to_db_and_update_summaries(
        session, helper, facility1, facility2):
    """
    :param Session session:
    :param GgoHelper helper:
    :param Facility facility1:
    :param Facility facility2:
    """
    # Arrange
    ggo = Ggo()
    ggo.facility = facility1
    ggo.period = datetime(2020, 1, 1, 0, 0, 0)
    ggo.sector = facility1.sector
    ggo.amount = 100
    ggo.unit = 'Wh'
    ggo.technology = 'Solar'
    ggo.tradable = True

    # Act
    helper.issue(ggo, facility1)
    helper.transfer(ggo, facility2)

    # Assert
    query = session.query(Ggo).order_by(Ggo.id.asc())
    ggo_origin, ggo_transferred = query.all()

    assert query.count() == 2

    assert ggo_origin == ggo
    assert ggo_origin.facility == facility1
    assert ggo_origin.tradable is False

    assert ggo_transferred.origin == ggo
    assert ggo_transferred.facility == facility2
    assert ggo_transferred.tradable is True
    assert ggo_transferred.amount == ggo.amount
    assert ggo_transferred.period == ggo.period
    assert ggo_transferred.sector == ggo.sector
    assert ggo_transferred.unit == ggo.unit
    assert ggo_transferred.technology == ggo.technology
    assert ggo_transferred.technology_code == ggo.technology_code
    assert ggo_transferred.source_code == ggo.source_code

    assert_summary(helper, facility1, ggo.period, ggo.technology, stored=0, issued=100, outbound=100)
    assert_summary(helper, facility2, ggo.period, ggo.technology, stored=100, inbound=100)

"""
Testing og TradeAgreement using an empty SQLite database.
"""
import pytest
from datetime import date, datetime
from sqlalchemy.exc import IntegrityError

from datahub.commodities import Ggo
from datahub.facilities import Facility
from datahub.trading import TradeAgreement, Transaction


@pytest.fixture()
def agreement(session, user1, user2, facility1):
    """
    :param Session session:
    :param User user1:
    :param User user2:
    :param Facility facility1:
    :return TradeAgreement:
    """
    agreement = TradeAgreement()
    agreement.user_from = user1
    agreement.user_to = user2
    agreement.pending = False
    agreement.accepted = True
    agreement.date_from = date(2020, 1, 1)
    agreement.date_to = date(2030, 1, 1)
    agreement.name = 'Our agreement 1'
    session.add(agreement)
    return agreement


@pytest.fixture()
def another_agreement(session, user1, user2, facility1):
    """
    :param Session session:
    :param User user1:
    :param User user2:
    :param Facility facility1:
    :return TradeAgreement:
    """
    agreement = TradeAgreement()
    agreement.user_from = user2
    agreement.user_to = user1
    agreement.pending = False
    agreement.accepted = True
    agreement.date_from = date(2020, 1, 1)
    agreement.date_to = date(2030, 1, 1)
    agreement.name = 'Our agreement 2'
    session.add(agreement)
    return agreement


# -- Creating new TradeAgreement objects testing -----------------------------

def test__TradeAgreement__create_two_agreements_with_same_two_users__should_raise_exception(session, user1, user2):
    """
    Test the UniqueConstraint on (user_from, user_to).

    Can not create two agreements between the same two users
    in the same direction.

    :param Session session:
    :param User user1:
    :param User user2:
    """
    agreement1 = TradeAgreement()
    agreement1.user_from = user1
    agreement1.user_to = user2
    agreement1.pending = False
    agreement1.accepted = True
    agreement1.date_from = date(2020, 1, 1)
    agreement1.date_to = date(2030, 1, 1)
    agreement1.name = 'Our agreement 1'

    agreement2 = TradeAgreement()
    agreement2.user_from = user1
    agreement2.user_to = user2
    agreement2.pending = False
    agreement2.accepted = True
    agreement2.date_from = date(2020, 1, 1)
    agreement2.date_to = date(2030, 1, 1)
    agreement2.name = 'Our agreement 2'

    # Act + Assert
    session.add(agreement1)
    session.add(agreement2)
    with pytest.raises(IntegrityError):
        session.commit()


def test__TradeAgreement__create_greement_with_same_user_from_and_to__should_raise_exception(session, user1):
    """
    Test the CheckConstraint on (user_from_id != user_to_id).

    Can not create an agreement where user_from and user_to is the same.

    :param Session session:
    :param User user1:
    """
    agreement = TradeAgreement()
    agreement.user_from = user1
    agreement.user_to = user1
    agreement.pending = False
    agreement.accepted = True
    agreement.date_from = date(2020, 1, 1)
    agreement.date_to = date(2030, 1, 1)
    agreement.name = 'Our agreement 1'

    # Act + Assert
    session.add(agreement)
    with pytest.raises(IntegrityError):
        session.commit()


# -- TradeAgreement.get_source_facilities() tests ----------------------------

def test__TradeAgreement__get_source_facilities__has_facility_from__should_return_facility_from(agreement, facility1):
    """
    :param TradeAgreement agreement:
    :param Facility facility1:
    """
    # Arrange
    agreement.facility_from = facility1

    # Act + Assert
    assert list(agreement.get_source_facilities()) == [facility1]


def test__TradeAgreement__get_source_facilities__has_not_facility_from__should_return_all_users_facilities(
        agreement, user1, facility1, facility2):
    """
    Omitting facility_from should result in returning all user_from's facilities.

    :param TradeAgreement agreement:
    :param User user1:
    :param Facility facility1:
    :param Facility facility2:
    """
    # Arrange
    facility1.user = user1
    facility2.user = user1
    agreement.user_from = user1

    # Act
    facilities = list(agreement.get_source_facilities())

    # Assert
    assert len(facilities) == 2
    assert facility1 in facilities
    assert facility2 in facilities


# -- TradeAgreement.get_eligible_facilities() tests --------------------------

def test__TradeAgreement__get_eligible_facilities__returns_only_eligible_facilities(session, agreement):
    """
    Tests where there exists both eligible and non-eligible facilities.

    :param Session session:
    :param TradeAgreement agreement:
    """
    # Arrange
    valid_sector = 'DK1'
    invalid_sector = 'DK2'

    # Same sector + has receiving priority = ELIGIBLE
    facility1 = Facility(
        user=agreement.user_to,
        gsrn='123',
        facility_type=Facility.TYPE_CONSUMPTION,
        technology='Solar',
        technology_code='Solar',
        source_code='Solar',
        sector=valid_sector,
        name='Facility 1 ELIGIBLE',
        receiving_priority=0,
    )

    # Same sector + has receiving priority = ELIGIBLE
    facility2 = Facility(
        user=agreement.user_to,
        gsrn='234',
        facility_type=Facility.TYPE_CONSUMPTION,
        technology='Solar',
        technology_code='Solar',
        source_code='Solar',
        sector=valid_sector,
        name='Facility 2 ELIGIBLE',
        receiving_priority=1,
    )

    # Different sector + has receiving priority = NOT ELIGIBLE
    facility3 = Facility(
        user=agreement.user_to,
        gsrn='345',
        facility_type=Facility.TYPE_CONSUMPTION,
        technology='Solar',
        technology_code='Solar',
        source_code='Solar',
        sector=invalid_sector,
        name='Facility 3 NOT ELIGIBLE',
        receiving_priority=2,
    )

    # Same sector + has NOT receiving priority = NOT ELIGIBLE
    facility4 = Facility(
        user=agreement.user_to,
        gsrn='456',
        facility_type=Facility.TYPE_CONSUMPTION,
        technology='Solar',
        technology_code='Solar',
        source_code='Solar',
        sector=valid_sector,
        name='Facility 4 NOT ELIGIBLE',
        receiving_priority=None,
    )

    # Different sector + has NOT receiving priority = NOT ELIGIBLE
    facility5 = Facility(
        user=agreement.user_to,
        gsrn='567',
        facility_type=Facility.TYPE_CONSUMPTION,
        technology='Solar',
        technology_code='Solar',
        source_code='Solar',
        sector=invalid_sector,
        name='Facility 5 NOT ELIGIBLE',
        receiving_priority=None,
    )

    # Same sector + has receiving priority + Different user = NOT ELIGIBLE
    facility6 = Facility(
        user=agreement.user_from,
        gsrn='678',
        facility_type=Facility.TYPE_CONSUMPTION,
        technology='Solar',
        technology_code='Solar',
        source_code='Solar',
        sector=valid_sector,
        name='Facility 6 NOT ELIGIBLE',
        receiving_priority=0,
    )

    # The GGO which facilities must be eligible to trade
    ggo = Ggo(
        facility=facility6,
        period=datetime(2020, 1, 1, 0, 0, 0),
        sector=valid_sector,
        amount=100,
        unit='Wh',
        technology='Solar',
        tradable=True,
    )

    agreement.user_to.facilities.append(facility1)
    agreement.user_to.facilities.append(facility2)
    agreement.user_to.facilities.append(facility3)
    agreement.user_to.facilities.append(facility4)
    agreement.user_to.facilities.append(facility5)
    agreement.user_from.facilities.append(facility6)
    session.commit()

    # Act
    eligible_facilities = list(agreement.get_eligible_facilities(ggo))

    # Assert
    assert len(eligible_facilities) == 2
    assert facility1 in eligible_facilities
    assert facility2 in eligible_facilities


# -- TradeAgreement.get_tradable_ggos() tests --------------------------------

def test__TradeAgreement__get_tradable_ggos__returns_correct_ggos(session, user1, user2):
    """
    :param Session session:
    :param User user1:
    :param User user2:
    :return:
    """
    # Arrange
    agreement_date_from = date(2020, 1, 1)
    agreement_date_to = date(2030, 1, 1)

    valid_period1 = datetime(2020, 1, 1, 0, 0, 0)
    valid_period2 = datetime(2025, 5, 14, 7, 0, 0)
    valid_period3 = datetime(2030, 1, 1, 23, 0, 0)

    invalid_period1 = datetime(2019, 12, 31, 23, 0, 0)
    invalid_period2 = datetime(2030, 1, 2, 0, 0, 0)

    agreement = TradeAgreement()
    agreement.user_from = user1
    agreement.user_to = user2
    agreement.pending = False
    agreement.accepted = True
    agreement.date_from = agreement_date_from
    agreement.date_to = agreement_date_to
    agreement.name = 'Our agreement 1'

    session.add(agreement)

    # -- Facilities ----------------------------------------------------------

    # Belongs to user_from = INCLUDED
    facility1 = Facility(
        user=agreement.user_from,
        gsrn='123',
        facility_type=Facility.TYPE_CONSUMPTION,
        technology='Solar',
        technology_code='Solar',
        source_code='Solar',
        sector='DK1',
        name='Facility 1 ELIGIBLE',
        receiving_priority=0,
    )

    # Belongs to user_from = INCLUDED
    facility2 = Facility(
        user=agreement.user_from,
        gsrn='234',
        facility_type=Facility.TYPE_CONSUMPTION,
        technology='Solar',
        technology_code='Solar',
        source_code='Solar',
        sector='DK2',
        name='Facility 2 ELIGIBLE',
        receiving_priority=1,
    )

    # Belongs to user_to = NOT INCLUDED
    facility3 = Facility(
        user=agreement.user_to,
        gsrn='345',
        facility_type=Facility.TYPE_CONSUMPTION,
        technology='Solar',
        technology_code='Solar',
        source_code='Solar',
        sector='DK1',
        name='Facility 3 NOT ELIGIBLE',
        receiving_priority=2,
    )

    session.add(facility1)
    session.add(facility2)
    session.add(facility3)

    # -- GGOs ----------------------------------------------------------------

    # Included
    ggo1 = Ggo(
        facility=facility1,
        period=valid_period1,
        sector='DK1',
        amount=100,
        unit='Wh',
        technology='Solar',
        tradable=True,
    )

    # Included
    ggo2 = Ggo(
        facility=facility2,
        period=valid_period2,
        sector='DK2',
        amount=100,
        unit='Wh',
        technology='Solar',
        tradable=True,
    )

    # Included
    ggo3 = Ggo(
        facility=facility2,
        period=valid_period3,
        sector='DK2',
        amount=100,
        unit='Wh',
        technology='Solar',
        tradable=True,
    )

    # Not included (not tradable)
    ggo4 = Ggo(
        facility=facility2,
        period=valid_period2,
        sector='DK2',
        amount=100,
        unit='Wh',
        technology='Solar',
        tradable=False,
    )

    # Not included (invalid period)
    ggo5 = Ggo(
        facility=facility1,
        period=invalid_period1,
        sector='DK1',
        amount=100,
        unit='Wh',
        technology='Solar',
        tradable=True,
    )

    # Not included (invalid period)
    ggo6 = Ggo(
        facility=facility2,
        period=invalid_period2,
        sector='DK2',
        amount=100,
        unit='Wh',
        technology='Solar',
        tradable=True,
    )

    # Not included (does not belong to any of user_from's facilities)
    ggo7 = Ggo(
        facility=facility3,
        period=valid_period1,
        sector='DK1',
        amount=100,
        unit='Wh',
        technology='Solar',
        tradable=True,
    )

    session.add(ggo1)
    session.add(ggo2)
    session.add(ggo3)
    session.add(ggo4)
    session.add(ggo5)
    session.add(ggo6)
    session.add(ggo7)
    session.commit()

    # -- Act -----------------------------------------------------------------

    tradable_ggos = list(agreement.get_tradable_ggos())

    # -- Assert --------------------------------------------------------------

    assert len(tradable_ggos) == 3
    assert ggo1 in tradable_ggos
    assert ggo2 in tradable_ggos
    assert ggo3 in tradable_ggos


# -- TradeAgreement.get_traded_amount() tests --------------------------------

@pytest.mark.parametrize('period', (None, datetime(2020, 1, 1, 0, 0, 0)))
def test__TradeAgreement__get_traded_amount__no_transactions_exists__should_return_zero(agreement, period):
    """
    :param TradeAgreement agreement:
    :param datetime period:
    """
    assert agreement.get_traded_amount(period) == 0


def test__TradeAgreement__get_traded_amount__transactions_exists__should_return_correct_amount(
        session, agreement, another_agreement, facility1):
    """
    Transactions exists in:
    - the same period from the same agreement
    - a different period from the same agreement
    - the same period from a different agreement
    - a different period from a different agreement

    Assertions are done on the two first points to make sure that:
    - Traded amount is only calculated for the agreement its invoked on
    - Traded amount is only calculated for the period its requested for

    :param Session session:
    :param TradeAgreement agreement:
    :param TradeAgreement another_agreement:
    :param Facility facility1:
    """
    # Arrange
    test_period = datetime(2020, 1, 1, 0, 0, 0)
    another_period = datetime(2020, 1, 1, 1, 0, 0)

    # -- Same period + same agreement = should be included -------------------

    session.add(Transaction(
        agreement=agreement,
        ggo_from=Ggo(
            facility=facility1,
            period=test_period,
            sector='DK1',
            amount=200,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
        ggo_to=Ggo(
            facility=facility1,
            period=test_period,
            sector='DK1',
            amount=200,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
    ))

    session.add(Transaction(
        agreement=agreement,
        ggo_from=Ggo(
            facility=facility1,
            period=test_period,
            sector='DK1',
            amount=100,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
        ggo_to=Ggo(
            facility=facility1,
            period=test_period,
            sector='DK1',
            amount=100,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
    ))

    # -- Same period + different agreement = should NOT be included ----------

    session.add(Transaction(
        agreement=another_agreement,
        ggo_from=Ggo(
            facility=facility1,
            period=test_period,
            sector='DK1',
            amount=200,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
        ggo_to=Ggo(
            facility=facility1,
            period=test_period,
            sector='DK1',
            amount=200,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
    ))

    session.add(Transaction(
        agreement=another_agreement,
        ggo_from=Ggo(
            facility=facility1,
            period=test_period,
            sector='DK1',
            amount=100,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
        ggo_to=Ggo(
            facility=facility1,
            period=test_period,
            sector='DK1',
            amount=100,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
    ))

    # -- Different period + same agreement = should NOT be included --------------

    session.add(Transaction(
        agreement=agreement,
        ggo_from=Ggo(
            facility=facility1,
            period=another_period,
            sector='DK1',
            amount=200,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
        ggo_to=Ggo(
            facility=facility1,
            period=another_period,
            sector='DK1',
            amount=200,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
    ))

    session.add(Transaction(
        agreement=agreement,
        ggo_from=Ggo(
            facility=facility1,
            period=another_period,
            sector='DK1',
            amount=100,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
        ggo_to=Ggo(
            facility=facility1,
            period=another_period,
            sector='DK1',
            amount=100,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
    ))

    # -- Different period + different agreement = should NOT be included -----

    session.add(Transaction(
        agreement=another_agreement,
        ggo_from=Ggo(
            facility=facility1,
            period=another_period,
            sector='DK1',
            amount=200,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
        ggo_to=Ggo(
            facility=facility1,
            period=another_period,
            sector='DK1',
            amount=200,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
    ))

    session.add(Transaction(
        agreement=another_agreement,
        ggo_from=Ggo(
            facility=facility1,
            period=another_period,
            sector='DK1',
            amount=100,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
        ggo_to=Ggo(
            facility=facility1,
            period=another_period,
            sector='DK1',
            amount=100,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
    ))

    session.commit()

    # Act + Assert
    assert agreement.get_traded_amount(test_period) == 300
    assert agreement.get_traded_amount() == 600


# -- TradeAgreement.get_remaining_amount() tests -----------------------------

@pytest.mark.parametrize('agreed_amount, expected_remaining_amount', (
        (None, None),
        (1000, 700),
        (300, 0),
        (0, 0),
))
def test__TradeAgreement__get_remaining_amount__should_return_correct_amount_or_None(
        session, agreement, facility1, agreed_amount, expected_remaining_amount):
    """
    Transactions exists in:
    - the same period from the same agreement
    - a different period from the same agreement
    - the same period from a different agreement
    - a different period from a different agreement

    Assertions are done on the two first points to make sure that:
    - Traded amount is only calculated for the agreement its invoked on
    - Traded amount is only calculated for the period its requested for

    :param Session session:
    :param TradeAgreement agreement:
    :param Facility facility1:
    :param int agreed_amount: The agreed amount (TradeAgreement.amount)
    :param int expected_remaining_amount: The expected return value
    """
    # Arrange
    agreement.amount = agreed_amount
    test_period = datetime(2020, 1, 1, 0, 0, 0)

    session.add(Transaction(
        agreement=agreement,
        ggo_from=Ggo(
            facility=facility1,
            period=test_period,
            sector='DK1',
            amount=200,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
        ggo_to=Ggo(
            facility=facility1,
            period=test_period,
            sector='DK1',
            amount=200,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
    ))

    session.add(Transaction(
        agreement=agreement,
        ggo_from=Ggo(
            facility=facility1,
            period=test_period,
            sector='DK1',
            amount=100,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
        ggo_to=Ggo(
            facility=facility1,
            period=test_period,
            sector='DK1',
            amount=100,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
    ))

    session.commit()

    # Act + Assert
    assert agreement.get_remaining_amount(test_period) == expected_remaining_amount


# -- TradeAgreement.is_fulfilled() tests -------------------------------------

@pytest.mark.parametrize('agreed_amount, expected_result', (
        (1000, False),
        (301, False),
        (300, True),
        (0, True),
        (None, False),
))
def test__TradeAgreement__is_fulfilled__should_return_correct_amount(
        session, agreement, facility1, agreed_amount, expected_result):
    """
    :param Session session:
    :param TradeAgreement agreement:
    :param Facility facility1:
    :param int agreed_amount: The agreed amount (TradeAgreement.amount)
    :param bool expected_result: The expected return value
    """
    # Arrange
    agreement.amount = agreed_amount
    test_period = datetime(2020, 1, 1, 0, 0, 0)

    session.add(Transaction(
        agreement=agreement,
        ggo_from=Ggo(
            facility=facility1,
            period=test_period,
            sector='DK1',
            amount=200,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
        ggo_to=Ggo(
            facility=facility1,
            period=test_period,
            sector='DK1',
            amount=200,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
    ))

    session.add(Transaction(
        agreement=agreement,
        ggo_from=Ggo(
            facility=facility1,
            period=test_period,
            sector='DK1',
            amount=100,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
        ggo_to=Ggo(
            facility=facility1,
            period=test_period,
            sector='DK1',
            amount=100,
            unit='Wh',
            technology='Solar',
            tradable=True,
        ),
    ))

    session.commit()

    # Act + Assert
    assert agreement.is_fulfilled(test_period) == expected_result

import pytest
from unittest.mock import Mock, call
from datetime import date, timedelta

from datahub.trading import AgreementController
from datahub.settings import FIRST_GGO_ISSUE_DATE


@pytest.fixture()
def processor():
    """
    :return AgreementProcessor:
    """
    return AgreementController(agreement=Mock(), controller=Mock())


def test__date_from__agreement_without_last_trade__should_be_first_issue_date(processor):
    """
    :param AgreementController processor:
    """
    # Arrange
    processor.agreement.last_trade = None

    # Assert
    assert processor.date_from == FIRST_GGO_ISSUE_DATE


def test__date_from__agreement_with_last_trade__should_add_one_day(processor):
    """
    :param AgreementController processor:
    """
    # Arrange
    processor.agreement.last_trade = date(2020, 1, 1)

    # Assert
    assert processor.date_from == date(2020, 1, 2)


def test__date_to__should_be_yesterday(processor):
    """
    :param AgreementController processor:
    """
    # Assert
    assert processor.date_to == date.today() - timedelta(days=1)


def test__get_source_facilities__agreement_has_facility_from__should_yield_facility_from(processor):
    """
    :param AgreementController processor:
    """
    # Arrange
    facility_from = Mock()
    processor.agreement.facility_from = facility_from

    # Assert
    assert list(processor.get_source_facilities()) == [facility_from]


def test__get_source_facilities__agreement_has_no_facility_from__should_yield_all_user_from_facilities(processor):
    """
    :param AgreementController processor:
    """
    # Arrange
    users_facilities = [Mock(), Mock(), Mock()]
    processor.agreement.facility_from = None
    processor.agreement.user_from.facilities = users_facilities

    # Assert
    assert list(processor.get_source_facilities()) == users_facilities


def test__get_source_ggos__should_yield_all_ggos_from_all_source_facilities(processor):
    """
    :param AgreementController processor:
    """
    # Arrange
    facility1 = Mock()
    facility1.ggos = [Mock(), Mock(), Mock()]

    facility2 = Mock()
    facility2.ggos = [Mock(), Mock(), Mock()]

    facility3 = Mock()
    facility3.ggos = [Mock(), Mock(), Mock()]

    processor.get_source_facilities = Mock()
    processor.get_source_facilities.return_value = [facility1, facility2, facility3]

    # Assert
    assert list(processor.get_source_ggos()) == facility1.ggos + facility2.ggos + facility3.ggos


def test__process__agreement_not_fulfilled_for_any_ggo__should_invoke_trade_on_controller_for_all_ggos(processor):
    """
    :param AgreementController processor:
    """
    # Arrange
    ggo1 = Mock()
    ggo2 = Mock()
    ggo3 = Mock()

    processor.get_source_ggos = Mock()
    processor.get_source_ggos.return_value = [ggo1, ggo2, ggo3]
    processor.agreement.is_fulfilled.return_value = False

    # Act
    processor.process()

    # Assert
    assert processor.controller.trade.call_count == 3
    processor.controller.trade.assert_has_calls((
        call(ggo1, processor.agreement),
        call(ggo2, processor.agreement),
        call(ggo3, processor.agreement),
    ))


def test__process__agreement_not_fulfilled_for_only_one_ggo__should_invoke_trade_on_controller_once(processor):
    """
    :param AgreementController processor:
    """
    # Arrange
    ggo1 = Mock()
    ggo2 = Mock()
    ggo3 = Mock()

    processor.get_source_ggos = Mock()
    processor.get_source_ggos.return_value = [ggo1, ggo2, ggo3]
    processor.agreement.is_fulfilled.side_effect = [True, False, True]

    # Act
    processor.process()

    # Assert
    assert processor.controller.trade.call_count == 1
    processor.controller.trade.assert_called_with(ggo2, processor.agreement)


def test__process__agreement_fulfilled_for_all_ggos__should_not_invoke_trade(processor):
    """
    :param AgreementController processor:
    """
    # Arrange
    ggo1 = Mock()
    ggo2 = Mock()
    ggo3 = Mock()

    processor.get_source_ggos = Mock()
    processor.get_source_ggos.return_value = [ggo1, ggo2, ggo3]
    processor.agreement.is_fulfilled.return_value = True

    # Act
    processor.process()

    # Assert
    processor.controller.trade.assert_not_called()


def test__process__should_always_set_last_trade_on_agreement(processor):
    """
    :param AgreementController processor:
    """
    # Arrange
    processor.get_source_ggos = Mock()
    processor.get_source_ggos.return_value = [Mock(), Mock(), Mock()]
    processor.agreement.is_fulfilled.return_value = True

    # Act
    processor.process()

    # Assert
    assert processor.agreement.last_trade == date.today()

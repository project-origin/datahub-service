"""
Tests of TradeController
"""
import pytest
from unittest.mock import Mock

from datahub.trading import TradeController, TradeDistribution


@pytest.fixture()
def controller():
    """
    :return TradeController:
    """
    return TradeController(distributor=Mock(), ledger=Mock(), helper=Mock())


# -- TradeController.trade() unit tests --------------------------------------

@pytest.mark.parametrize('ggo_amount', (0, -1))
def test__TradeController__trade__amount_is_invalid__should_raise_assertion_error(controller, ggo_amount):
    """
    The provided GGO must have a positive amount.

    :param TradeController controller:
    :param int ggo_amount: The GGO amount
    """
    # Arrange
    ggo = Mock()
    ggo.amount = ggo_amount

    # Act + Assert
    with pytest.raises(AssertionError):
        controller.trade(ggo, Mock())


@pytest.mark.parametrize('remaining_amount', (0, -1))
def test__TradeController__trade__nothing_to_trade__should_not_distribute_anything(controller, remaining_amount):
    """
    There is no remaining amount to be traded on the agreement.
    trade() should call neither distributor.get_distribution() nor self.distribute().

    :param TradeController controller:
    :param int remaining_amount: The remaining amount of the agreement
    """
    # Arrange
    ggo = Mock()
    ggo.amount = 100

    agreement = Mock()
    agreement.get_remaining_amount.return_value = remaining_amount

    controller.distribute = Mock()

    # Act
    controller.trade(ggo, agreement)

    # Assert
    controller.distributor.get_distribution.assert_not_called()
    controller.distribute.assert_not_called()


@pytest.mark.parametrize('ggo_amount, remaining_amount, expected_amount', (
        (100, 100, 100),
        (100, 50, 50),
        (50, 100, 50),
        (100, 200, 100),
        (200, 100, 100),
))
def test__TradeController__trade__should_distribute_correct_amount(
        controller, ggo_amount, remaining_amount, expected_amount):
    """
    The amount to distribute should never exceed neither the GGO amount
    nor the remaining amount of the agreement.

    :param TradeController controller:
    :param int ggo_amount: The GGO amount
    :param int remaining_amount: The remaining amount of the agreement
    :param int expected_amount: The expected amount to be distributed
    """
    # Arrange
    ggo = Mock()
    ggo.amount = ggo_amount

    agreement = Mock()
    agreement.get_remaining_amount.return_value = remaining_amount

    distribution = Mock()
    distribution.size.return_value = 0

    controller.distributor.get_distribution.return_value = distribution
    controller.distribute = Mock()

    # Act
    controller.trade(ggo, agreement)

    # Assert
    controller.distributor.get_distribution.assert_called_once_with(
        agreement=agreement,
        period=ggo.period,
        amount=expected_amount,
    )


# -- TradeController integration tests ---------------------------------------

def test__TradeController__distribute_entire_amount_to_one_facility__should_transfer(controller):
    """
    The distributor distributes the entire amount to one facility.
    TradeController should transfer the entire GGO to that facility.

    :param TradeController controller:
    """
    # Arrange
    facility = Mock()

    ggo = Mock()
    ggo.amount = 100

    agreement = Mock()
    agreement.get_remaining_amount.return_value = 100

    distribution = TradeDistribution()
    distribution.add(facility, 100)
    controller.distributor.get_distribution.return_value = distribution

    new_ggo = Mock()
    controller.helper.transfer.return_value = new_ggo

    # Act
    controller.trade(ggo, agreement)

    # Assert
    controller.helper.transfer.assert_called_once_with(ggo, facility)
    controller.ledger.transfer.assert_called_once_with(ggo, new_ggo)


def test__TradeController__distribute_part_of_amount_to_one_facility__should_split_between_facility_and_current_owner(controller):
    """
    The distributor distributes part of the amount to one facility.
    The remaining of the GGO amount should go to the current owner.

    TradeController should add the current owning facility to the
    distribution to receive the remaining of the GGO amount.

    :param TradeController controller:
    """
    # Arrange
    ggo = Mock()
    ggo.amount = 100

    agreement = Mock()
    agreement.get_remaining_amount.return_value = 100

    distribution = TradeDistribution()
    distribution.add(Mock(), 50)
    controller.distributor.get_distribution.return_value = distribution

    new_ggos = Mock()
    controller.helper.split.return_value = new_ggos

    # Act
    controller.trade(ggo, agreement)

    # Assert
    assert distribution.size() == 2
    assert distribution.total() == 100
    assert distribution.get(ggo.facility) == 50
    controller.helper.split.assert_called_once_with(ggo, distribution)
    controller.ledger.split.assert_called_once_with(ggo, new_ggos)


def test__TradeController__distribute_entire_amount_to_multiple_facilities__should_split(controller):
    """
    The distributor distributes the entire amount to multiple facilities.
    TradeController should transfer the entire GGO to that facility.

    :param TradeController controller:
    """
    # Arrange
    facility = Mock()

    ggo = Mock()
    ggo.amount = 100

    agreement = Mock()
    agreement.get_remaining_amount.return_value = 100

    distribution = TradeDistribution()
    distribution.add(Mock(), 50)
    distribution.add(Mock(), 50)
    controller.distributor.get_distribution.return_value = distribution

    new_ggos = Mock()
    controller.helper.split.return_value = new_ggos

    # Act
    controller.trade(ggo, agreement)

    # Assert
    assert distribution.size() == 2
    assert distribution.total() == 100
    controller.helper.split.assert_called_once_with(ggo, distribution)
    controller.ledger.split.assert_called_once_with(ggo, new_ggos)


def test__TradeController__distribute_part_of_amount_to_multiple_facilities__should_split_between_facilities_and_current_owner(controller):
    """
    The distributor distributes part of the amount to multiple facilities.
    The remaining of the GGO amount should go to the current owner.

    TradeController should add the current owning facility to the
    distribution to receive the remaining of the GGO amount.

    :param TradeController controller:
    """
    # Arrange
    ggo = Mock()
    ggo.amount = 100

    agreement = Mock()
    agreement.get_remaining_amount.return_value = 100

    distribution = TradeDistribution()
    distribution.add(Mock(), 20)
    distribution.add(Mock(), 20)
    controller.distributor.get_distribution.return_value = distribution

    new_ggos = Mock()
    controller.helper.split.return_value = new_ggos

    # Act
    controller.trade(ggo, agreement)

    # Assert
    assert distribution.size() == 3
    assert distribution.total() == 100
    assert distribution.get(ggo.facility) == 60
    controller.helper.split.assert_called_once_with(ggo, distribution)
    controller.ledger.split.assert_called_once_with(ggo, new_ggos)

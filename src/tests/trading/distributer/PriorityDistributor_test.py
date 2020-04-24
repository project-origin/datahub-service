import pytest
from unittest.mock import Mock

from datahub.trading import PriorityDistributor


@pytest.fixture()
def distributor():
    """
    :return PriorityDistributor:
    """
    return PriorityDistributor()


def test__get_distribution__amount_is_negative__should_raise_exception(distributor):
    """
    :param PriorityDistributor distributor:
    """
    with pytest.raises(BaseException):
        distributor.get_distribution(
            agreement=Mock(),
            period=Mock(),
            amount=-1,
        )


@pytest.mark.parametrize('amount, desired, traded, exp_distributed', (
        (300,       (100, 100, 100),   (0, 0, 0),      (100, 100, 100)),
        (200,       (100, 100, 100),   (0, 0, 0),      (100, 100, 0)),
        (101,       (100, 100, 100),   (0, 0, 0),      (100, 1, 0)),
        (100,       (100, 100, 100),   (0, 0, 0),      (100, 0, 0)),
        (99,        (100, 100, 100),   (0, 0, 0),      (99, 0, 0)),
        (0,         (100, 100, 100),   (0, 0, 0),      (0, 0, 0)),

        # Cases where the desired varies
        # TODO

        # Cases where some trade already occurred
        (200,       (100, 100, 100),   (100, 0, 0),    (0, 100, 100)),
        (200,       (100, 100, 100),   (0, 100, 0),    (100, 0, 100)),
        (200,       (100, 100, 100),   (0, 0, 100),    (100, 100, 0)),
        (150,       (100, 100, 100),   (0, 0, 100),    (100, 50, 0)),
        (150,       (100, 100, 100),   (100, 0, 0),    (0, 100, 50)),
        (101,       (100, 100, 100),   (100, 0, 0),    (0, 100, 1)),
        (100,       (100, 100, 100),   (100, 0, 0),    (0, 100, 0)),
        (99,        (100, 100, 100),   (100, 0, 0),    (0, 99, 0)),
        (0,         (100, 100, 100),   (100, 0, 0),    (0, 0, 0)),

        # Cases where the remainder is distributed equally
        # TODO
))
def test__get_distribution__black_box_tests_different_parameters(
        distributor, amount, desired, traded, exp_distributed):
    """
    :param PriorityDistributor distributor:
    :param int amount:
    :param (int) desired:
    :param (int) traded:
    :param (int) exp_distributed:
    """
    assert len(desired) == len(traded) == len(exp_distributed)

    # -- Arrange -------------------------------------------------------------

    facilities = [Mock() for i in range(len(desired))]

    # Mocking get_target_facilities()
    distributor.get_target_facilities = Mock()
    distributor.get_target_facilities.return_value = facilities

    # Mocking get_desired_amount()
    distributor.get_desired_amount = Mock()
    distributor.get_desired_amount.side_effect = \
        lambda f, a, p: desired[facilities.index(f)]

    # Mocking get_traded_amount()
    distributor.get_traded_amount = Mock()
    distributor.get_traded_amount.side_effect = \
        lambda f, a, p: traded[facilities.index(f)]

    # -- Act -----------------------------------------------------------------

    distribution = distributor.get_distribution(
        agreement=Mock(),
        period=Mock(),
        amount=amount,
    )

    # -- Assert --------------------------------------------------------------

    # Number of unique distributions match those that are expected to be > 0
    assert len(distribution) == len([d for d in exp_distributed if d > 0])

    # Total distribution amount matches trade amount
    assert distribution.total() == amount

    # Each facility is distributed the expected amount
    for f, d, t, e in zip(facilities, desired, traded, exp_distributed):
        assert distribution.get(f) == e

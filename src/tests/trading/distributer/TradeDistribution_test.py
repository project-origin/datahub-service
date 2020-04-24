import pytest
from unittest.mock import Mock

from datahub.trading import TradeDistribution


@pytest.fixture()
def distribution():
    """
    :return TradeDistribution:
    """
    return TradeDistribution()


def test__TradeDistribution__is_empty(distribution):
    """
    :param TradeDistribution distribution:
    """
    assert distribution.total() == 0
    assert distribution.size() == 0
    assert distribution.facilities() == []
    assert list(distribution) == []

    with pytest.raises(AssertionError):
        distribution.one()


def test__TradeDistribution__has_one_distribution(distribution):
    """
    :param TradeDistribution distribution:
    """
    # Arrange
    facility = Mock()
    distribution.add(facility, 50)

    # Act + Assert
    assert distribution.size() == 1
    assert distribution.total() == 50
    assert distribution.get(facility) == 50
    assert distribution.one() == (facility, 50)
    assert distribution.facilities() == [facility]
    assert list(distribution) == [(facility, 50)]


def test__TradeDistribution__has_two_distributions_same_facility(distribution):
    """
    :param TradeDistribution distribution:
    """
    # Arrange
    facility = Mock()
    distribution.add(facility, 50)
    distribution.add(facility, 50)

    # Act + Assert
    assert distribution.size() == 1
    assert distribution.total() == 100
    assert distribution.get(facility) == 100
    assert distribution.one() == (facility, 100)
    assert distribution.facilities() == [facility]
    assert list(distribution) == [(facility, 100)]


def test__TradeDistribution__has_two_distributions_different_facilities(distribution):
    """
    :param TradeDistribution distribution:
    """
    # Arrange
    facility1 = Mock()
    facility2 = Mock()
    distribution.add(facility1, 25)
    distribution.add(facility2, 75)

    # Act + Assert
    assert distribution.size() == 2
    assert distribution.total() == 100
    assert distribution.get(facility1) == 25
    assert distribution.get(facility2) == 75

    assert len(distribution.facilities()) == 2
    assert facility1 in distribution.facilities()
    assert facility2 in distribution.facilities()

    assert len(list(distribution)) == 2
    assert (facility1, 25) in list(distribution)
    assert (facility2, 75) in list(distribution)

    with pytest.raises(AssertionError):
        distribution.one()

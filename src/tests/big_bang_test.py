from datetime import date, datetime

from datahub.auth import User
from datahub.facilities import Facility, Measurement
from datahub.ledger import OriginLedger
from datahub.commodities import GgoHelper, GgoIssuingController, Ggo
from datahub.trading import (
    TradeAgreement,
    TradeController,
    AgreementController,
)


# -- Helper functions --------------------------------------------------------

def perform_trading(agreement_controller, trade_controller):
    for ggo, agreement in agreement_controller.get_tradables():
        trade_controller.trade(ggo, agreement)


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

    if summary is None:
        # Summary does not exist = The same as all values == 0
        assert issued == 0
        assert inbound == 0
        assert outbound == 0
        assert stored == 0
        assert retired == 0
        assert expired == 0
    else:
        assert summary.issued == issued
        assert summary.inbound == inbound
        assert summary.outbound == outbound
        assert summary.stored == stored
        assert summary.retired == retired
        assert summary.expired == expired


# -- Test(s) -----------------------------------------------------------------

def test__big_bang(session, ledger_mock, helper, issuing_controller, trade_controller, agreement_controller):
    """
    This test was largely used by the developer during development of the
    system to test an entire flow of issuing, trading, and retiring.
    As the system grew, so did the complexity of this test.

    It uses an actual SQL database except for the Origin Ledger API,
    which is the only mocked object.

    :param Session session:
    :param OriginLedger ledger_mock:
    :param GgoHelper helper:
    :param GgoIssuingController issuing_controller:
    :param TradeController trade_controller:
    :param AgreementController agreement_controller:
    """

    # -- Arrange -------------------------------------------------------------

    agreed_amount = 1000
    consumed_amount = 500
    period = datetime(2020, 1, 1, 0, 0, 0)
    technology = 'Wind'

    user_from = User(
        name='User 1',
        email='user1@user.com',
        password='a2958ec16247ad36c229546487296c93',
    )

    user_to = User(
        name='User 2',
        email='user2@user.com',
        password='f3bad9b281e199dc1308afa9b98db7d2',
    )

    facility_from1 = Facility(
        user=user_from,
        gsrn='000000000000000001',
        facility_type=Facility.TYPE_PRODUCTION,
        technology=technology,
        sector='DK1',
    )

    agreement = TradeAgreement(
        user_from=user_from,
        user_to=user_to,
        date_from=date(1900, 1, 1),
        date_to=date(2100, 1, 1),
        amount=agreed_amount,
        pending=False,
        accepted=True,
        name='Test agreement',
    )

    session.add(user_from)
    session.add(user_to)
    session.add(facility_from1)
    session.add(agreement)
    session.commit()

    # Newly issued GGOs returned by the ledger (MOCKED)
    # IMPORTANT: Use a side_effect (function) to make sure the same GGO objects
    # aren't returned when invoking get_issued_ggos() multiple times.
    ledger_mock.get_issued_ggos.side_effect = lambda *args, **kwargs: (
        Ggo(
            period=period,
            sector='DK1',
            amount=100,
            unit='Wh',
            technology=technology,
        ),
        Ggo(
            period=period,
            sector='DK1',
            amount=100,
            unit='Wh',
            technology=technology,
        ),
    )

    # -- Assert --------------------------------------------------------------

    # Nothing has ben issued or traded yet

    assert agreement.get_traded_amount() == 0
    assert agreement.get_remaining_amount(period) == agreed_amount
    assert agreement.is_fulfilled(period) is False

    assert_summary(helper, facility_from1, period, technology)

    # -- Act -----------------------------------------------------------------

    # Import new GGOs from the ledger
    # NOTE: The following GGOs are imported/issued to EACH facility in the system
    # because ledger.get_issued_ggos() is invoked for each facility.

    # facility_from1 is issued 200 Wh

    issuing_controller.import_for_all_facilities()
    session.commit()

    # -- Assert --------------------------------------------------------------

    # No trading has occurred at this time

    assert agreement.get_traded_amount(period) == 0
    assert agreement.get_remaining_amount(period) == agreed_amount
    assert agreement.is_fulfilled(period) is False

    assert_summary(helper, facility_from1, period, technology, issued=200, stored=200)

    # -- Act -----------------------------------------------------------------

    # Trade

    perform_trading(agreement_controller, trade_controller)
    session.commit()

    # -- Assert --------------------------------------------------------------

    # There are no receiving facilities for the agreement at this point,
    # ie. no trading has occurred.

    assert agreement.get_traded_amount(period) == 0
    assert agreement.get_remaining_amount(period) == agreed_amount
    assert agreement.is_fulfilled(period) is False

    assert_summary(helper, facility_from1, period, technology, issued=200, stored=200)

    # -- Act -----------------------------------------------------------------

    # Add two facilities to user_to, so that trading can occur.
    # Only one of them has a receiving_priority, the other should not receive anything.

    facility_to1 = Facility(
        user=user_to,
        gsrn='facility_to1',
        facility_type=Facility.TYPE_CONSUMPTION,
        technology=technology,
        sector='DK1',
        receiving_priority=0,
    )

    facility_to1.measurements.append(Measurement(
        period=period,
        amount=consumed_amount,
        unit='Wh',
    ))

    # Does not have receiving_priority
    facility_to2 = Facility(
        user=user_to,
        gsrn='facility_to2',
        facility_type=Facility.TYPE_CONSUMPTION,
        technology=technology,
        sector='DK1',
        receiving_priority=None,
    )

    facility_to2.measurements.append(Measurement(
        period=period,
        amount=consumed_amount,
        unit='Wh',
    ))

    session.add(facility_to1)
    session.add(facility_to2)
    session.commit()

    # -- Act -----------------------------------------------------------------

    # Trade

    perform_trading(agreement_controller, trade_controller)
    session.commit()

    # -- Assert --------------------------------------------------------------

    # Only facility_to1 has receiving_priority and should trade GGOs

    assert agreement.get_traded_amount(period) == 200
    assert agreement.get_remaining_amount(period) == (agreed_amount - 200)
    assert agreement.is_fulfilled(period) is False

    assert_summary(helper, facility_from1, period, technology, issued=200, stored=0, outbound=200)
    assert_summary(helper, facility_to1, period, technology, stored=200, inbound=200)
    assert_summary(helper, facility_to2, period, technology)

    # -- Act -----------------------------------------------------------------

    # Trade

    perform_trading(agreement_controller, trade_controller)
    session.commit()

    # -- Assert --------------------------------------------------------------

    # By trading again without issuing new GGOs, nothing should have changed

    assert agreement.get_traded_amount(period) == 200
    assert agreement.get_remaining_amount(period) == (agreed_amount - 200)
    assert agreement.is_fulfilled(period) is False

    assert_summary(helper, facility_from1, period, technology, issued=200, stored=0, outbound=200)
    assert_summary(helper, facility_to1, period, technology, stored=200, inbound=200)
    assert_summary(helper, facility_to2, period, technology)

    # -- Act -----------------------------------------------------------------

    # Add another source facility

    facility_from2 = Facility(
        user=user_from,
        gsrn='facility_from2',
        facility_type=Facility.TYPE_PRODUCTION,
        technology=technology,
        sector='DK1',
    )

    session.add(facility_from2)
    session.commit()

    # -- Act -----------------------------------------------------------------

    # Import new GGOs from the ledger

    # facility_from1 is issued 200 Wh
    # facility_from2 is issued 200 Wh
    # facility_to1 is issued 200 Wh
    # facility_to2 is issued 200 Wh

    issuing_controller.import_for_all_facilities()
    session.commit()

    # -- Assert --------------------------------------------------------------

    assert agreement.get_traded_amount(period) == 200
    assert agreement.get_remaining_amount(period) == (agreed_amount - 200)
    assert agreement.is_fulfilled(period) is False

    assert_summary(helper, facility_from1, period, technology, issued=400, stored=200, outbound=200)
    assert_summary(helper, facility_from2, period, technology, issued=200, stored=200)
    assert_summary(helper, facility_to1, period, technology, issued=200, stored=400, inbound=200)
    assert_summary(helper, facility_to2, period, technology, issued=200, stored=200)

    # -- Act -----------------------------------------------------------------

    # Trade

    # facility_from1 outbound += 200
    # facility_from2 outbound += 200
    # facility_to1 inbound += 400
    # facility_to2 inbound += 0 (does not have receiving_priority)

    perform_trading(agreement_controller, trade_controller)
    session.commit()

    # -- Assert --------------------------------------------------------------

    assert agreement.get_traded_amount(period) == 600
    assert agreement.get_remaining_amount(period) == (agreed_amount - 600)
    assert agreement.is_fulfilled(period) is False

    assert_summary(helper, facility_from1, period, technology, issued=400, outbound=400)
    assert_summary(helper, facility_from2, period, technology, issued=200, outbound=200)
    assert_summary(helper, facility_to1, period, technology, issued=200, stored=800, inbound=600)
    assert_summary(helper, facility_to2, period, technology, issued=200, stored=200)

    # -- Act -----------------------------------------------------------------

    # Assign receiving_priority to facility_to2 so it receives GGOs

    facility_to2.receiving_priority = 1
    session.commit()

    # -- Act -----------------------------------------------------------------

    # Import new GGOs from the ledger

    # facility_from1 is issued 200 Wh
    # facility_from2 is issued 200 Wh
    # facility_to1 is issued 200 Wh
    # facility_to2 is issued 200 Wh

    issuing_controller.import_for_all_facilities()
    session.commit()

    # -- Assert --------------------------------------------------------------

    assert agreement.get_traded_amount(period) == 600
    assert agreement.get_remaining_amount(period) == (agreed_amount - 600)
    assert agreement.is_fulfilled(period) is False

    assert_summary(helper, facility_from1, period, technology, issued=600, stored=200, outbound=400)
    assert_summary(helper, facility_from2, period, technology, issued=400, stored=200, outbound=200)
    assert_summary(helper, facility_to1, period, technology, issued=400, stored=1000, inbound=600)
    assert_summary(helper, facility_to2, period, technology, issued=400, stored=400)

    # -- Act -----------------------------------------------------------------

    # Trade

    # This time facility_to1 has its consumption covered by what is stored,
    # so facility_to2 has the second highest receiving_priority, and gets
    # its consumption covered first (+100). The remaining (300) is shared
    # equally by both facilities (+150 each).

    # facility_from1 outbound += 200
    # facility_from1 stored -= 200

    # facility_from2 outbound += 200
    # facility_from2 stored -= 200

    # facility_to1 inbound += 150 (0 by priority, 150 by equal share)
    # facility_to1 stored += 150

    # facility_to2 inbound += 250 (100 by priority, 150 by equal share)
    # facility_to2 stored += 250

    perform_trading(agreement_controller, trade_controller)
    session.commit()

    # -- Assert --------------------------------------------------------------

    # By now the agreement ss fulfilled (agreed amount is 100 Wh)

    assert agreement.get_traded_amount(period) == 1000
    assert agreement.get_remaining_amount(period) == (agreed_amount - 1000)
    assert agreement.is_fulfilled(period) is True

    assert_summary(helper, facility_from1, period, technology, issued=600, outbound=600)
    assert_summary(helper, facility_from2, period, technology, issued=400, outbound=400)
    assert_summary(helper, facility_to1, period, technology, issued=400, stored=1150, inbound=750)
    assert_summary(helper, facility_to2, period, technology, issued=400, stored=650, inbound=250)

    # -- Act -----------------------------------------------------------------

    # Import new GGOs from the ledger

    # facility_from1 is issued 200 Wh
    # facility_from2 is issued 200 Wh
    # facility_to1 is issued 200 Wh
    # facility_to2 is issued 200 Wh

    issuing_controller.import_for_all_facilities()
    session.commit()

    # -- Assert --------------------------------------------------------------

    assert agreement.get_traded_amount(period) == 1000
    assert agreement.get_remaining_amount(period) == (agreed_amount - 1000)
    assert agreement.is_fulfilled(period) is True

    assert_summary(helper, facility_from1, period, technology, issued=800, stored=200, outbound=600)
    assert_summary(helper, facility_from2, period, technology, issued=600, stored=200, outbound=400)
    assert_summary(helper, facility_to1, period, technology, issued=600, stored=1350, inbound=750)
    assert_summary(helper, facility_to2, period, technology, issued=600, stored=850, inbound=250)

    # -- Act -----------------------------------------------------------------

    # Trade

    perform_trading(agreement_controller, trade_controller)
    session.commit()

    # -- Assert --------------------------------------------------------------

    # Since the agreement is fulfilled for this period, no GGOs should have
    # been traded this time

    assert agreement.get_traded_amount(period) == 1000
    assert agreement.get_remaining_amount(period) == (agreed_amount - 1000)
    assert agreement.is_fulfilled(period) is True

    assert_summary(helper, facility_from1, period, technology, issued=800, stored=200, outbound=600)
    assert_summary(helper, facility_from2, period, technology, issued=600, stored=200, outbound=400)
    assert_summary(helper, facility_to1, period, technology, issued=600, stored=1350, inbound=750)
    assert_summary(helper, facility_to2, period, technology, issued=600, stored=850, inbound=250)

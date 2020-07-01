import origin_ledger_sdk as ols

from datahub.measurements import Measurement
from datahub.settings import LEDGER_URL, DEBUG

from .models import Disclosure, DisclosureSettlement, DisclosureRetiredGgo


ledger = ols.Ledger(LEDGER_URL, verify=not DEBUG)


class DisclosureCompiler(object):
    """
    TODO
    """

    def sync_for_measurement(self, disclosure, measurement, session):
        """
        :param Disclosure disclosure:
        :param Measurement measurement:
        :param sqlalchemy.orm.Session session:
        :rtype: DisclosureSettlement
        """

        # Get the settlement from the ledger
        ledger_settlement = self.get_ledger_settlement(measurement)

        # If no settlement exists for the Measurement, nothing has
        # been retired to it, so just stop here
        if ledger_settlement is None:
            return

        # Get (or create) a representation of the settlement from DB
        disclosure_settlement = self.get_or_create_settlement(
            disclosure, measurement, session)

        # Filter out the settlement parts that already exists in DB
        new_parts = (
            part for part in ledger_settlement.parts
            if not self.disclosure_retired_ggo_exists(
                disclosure_settlement, part, session)
        )

        # Insert all new settlement parts in to DB
        for part in new_parts:
            ledger_ggo = ledger.get_ggo(part.ggo)

            session.add(DisclosureRetiredGgo(
                settlement=disclosure_settlement,
                address=ledger_ggo.address,
                amount=ledger_ggo.amount,
                begin=ledger_ggo.begin,
                end=ledger_ggo.end,
                sector=ledger_ggo.sector,
                technology_code=ledger_ggo.tech_type,
                fuel_code=ledger_ggo.fuel_type,
            ))
            session.flush()

    def get_ledger_settlement(self, measurement):
        """
        :param Measurement measurement:
        :rtype: ols.Settlement
        """
        try:
            return ledger.get_settlement(measurement.settlement_address)
        except ols.LedgerException as e:
            if e.code == 75:
                # No settlement exists (nothing at the requested address)
                return None
            else:
                # Arbitrary ledger exception, let it bubble up
                raise

    def get_or_create_settlement(self, disclosure, measurement, session):
        """
        :param Disclosure disclosure:
        :param Measurement measurement:
        :param sqlalchemy.orm.Session session:
        :rtype: DisclosureSettlement
        """
        settlement = session.query(DisclosureSettlement) \
            .filter(DisclosureSettlement.disclosure_id == disclosure.id) \
            .filter(DisclosureSettlement.address == measurement.settlement_address) \
            .one_or_none()

        if settlement is None:
            settlement = DisclosureSettlement(
                disclosure=disclosure,
                measurement=measurement,
                address=measurement.settlement_address,
            )
            session.add(settlement)
            session.flush()

        return settlement

    def disclosure_retired_ggo_exists(
            self, disclosure_settlement, settlement_part, session):
        """
        :param DisclosureSettlement disclosure_settlement:
        :param ols.SettlementPart settlement_part:
        :param sqlalchemy.orm.Session session:
        :rtype: bool
        """
        count = session.query(DisclosureRetiredGgo) \
            .filter(DisclosureRetiredGgo.settlement_id == disclosure_settlement.id) \
            .filter(DisclosureRetiredGgo.address == settlement_part.ggo) \
            .count()

        return count > 0

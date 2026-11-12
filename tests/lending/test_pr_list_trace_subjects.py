"""Wholesaler trace subjects (who Tracerfy is sent, and from where), the Wholesalers source tag and load record."""
from __future__ import annotations

from src.lending import pr_list_trace_subjects as ts

RECORD = {"radar_id": "R1", "last_transfer_seller": "STACKHOUSE SARAH E", "property_address": "12 Oak St",
          "city": "Tampa", "state": "FL", "zip": "33602"}
SUNBIZ = {"doc_number": "L24000299649", "principal_address": "8100 ULMERTON ROAD\n#1\nLARGO, FL 33771",
          "managing_members": [{"name": "HOLDCO LLC", "address": "1 A St\nMIAMI, FL 33101"},
                               {"name": "D'ANDREA, ROBERT R, JR.", "address": "8100 ULMERTON RD., #1\nLARGO, FL 33771"}],
          "officers": []}


def test_property_names_are_last_first_and_only_the_first_owner_is_used():
    assert ts.split_property_name("STACKHOUSE SARAH E") == ("SARAH", "STACKHOUSE")
    assert ts.split_property_name("HAYWARD RODERICK L & JADA D") == ("RODERICK", "HAYWARD")
    assert ts.split_property_name("CHER") == (None, None)


def test_sunbiz_names_and_addresses_parse():
    assert ts.split_sunbiz_name("D'ANDREA, ROBERT R, JR.") == ("ROBERT", "D'ANDREA")
    assert ts.parse_sunbiz_address("8100 ULMERTON RD., #1\nLARGO, FL 33771") == {
        "address": "8100 ULMERTON RD., #1", "city": "LARGO", "state": "FL", "zip": "33771"}
    assert ts.parse_sunbiz_address("NO CITY LINE") is None


def test_an_individual_seller_is_traced_with_the_property_they_resold():
    s = ts.subject_for_individual("wholesaler", "C1", RECORD)
    assert (s.first_name, s.last_name, s.address, s.source, s.untraceable_reason) == \
        ("SARAH", "STACKHOUSE", "12 Oak St", "seller_property", None)


def test_trusts_cemeteries_and_unparseable_names_are_never_sent_as_a_person():
    for seller, reason in (("ST MORITZ BRIAN TRUST", "not_a_person"),
                           ("WESTVIEW CMNTY CEMETERY OF POMPAN", "not_a_person"), ("CHER", "name_unparseable")):
        s = ts.subject_for_individual("wholesaler", "C1", {**RECORD, "last_transfer_seller": seller})
        assert (s.source, s.untraceable_reason) == ("none", reason)


def test_a_company_is_traced_as_the_first_person_on_its_sunbiz_record_never_another_company():
    s = ts.subject_for_company("wholesaler", "C2", {**RECORD, "last_transfer_seller": "QUICK FLIP LLC"}, SUNBIZ)
    assert (s.first_name, s.last_name, s.city, s.zip, s.source, s.sunbiz_doc_number) == \
        ("ROBERT", "D'ANDREA", "LARGO", "33771", "sunbiz_member", "L24000299649")


def test_a_company_without_sunbiz_or_without_a_person_is_untraceable():
    rec = {**RECORD, "last_transfer_seller": "QUICK FLIP LLC"}
    assert ts.subject_for_company("wholesaler", "C2", rec, None).untraceable_reason == "not_on_sunbiz"
    no_person = {**SUNBIZ, "managing_members": [{"name": "HOLDCO LLC", "address": "1 A St\nMIAMI, FL 33101"}]}
    assert ts.subject_for_company("wholesaler", "C2", rec, no_person).untraceable_reason == "no_person_on_sunbiz"


def test_every_subject_satisfies_the_tables_source_rule():
    """The table checks (source = 'none') = (untraceable_reason IS NOT NULL)."""
    subjects = [ts.subject_for_individual("w", "a", RECORD),
                ts.subject_for_individual("w", "b", {**RECORD, "last_transfer_seller": "X TRUST"}),
                ts.subject_for_company("w", "c", RECORD, SUNBIZ), ts.subject_for_company("w", "d", RECORD, None)]
    assert all((s.source == "none") == (s.untraceable_reason is not None) for s in subjects)


def test_propertyradar_wholesalers_go_to_the_wholesalers_queue_and_are_never_booked():
    from src.lending.queues import assign_queue
    assigned = assign_queue({"source_tag": "wholesaler_propertyradar"})
    assert assigned["queue"] == "wholesalers" and assigned["bookable"] is False


def test_the_wholesaler_load_record_names_the_person_the_company_and_the_resold_property():
    from src.tasks.lending_pr_calling_list_export import wholesaler_record
    person = {"list_key": "wholesaler", "radar_id": "R1", "last_transfer_seller": "QUICK FLIP LLC",
              "first_name": "ROBERT", "last_name": "D'ANDREA", "property_address": "12 Oak St", "city": "Tampa",
              "zip": "33602", "county": "Hillsborough", "state": "FL", "apn": "A1", "match_strength": "confirmed",
              "source": "sunbiz_member"}
    record = wholesaler_record(person, "+18135550101")
    assert (record["pool"], record["source_tag"], record["borrower_name"], record["entity_name"]) == \
        ("wholesalers", "wholesaler_propertyradar", "Robert D'Andrea", "QUICK FLIP LLC")


def test_a_cash_buyer_goes_to_transaction_ready_with_a_first_last_name():
    from config.lending_queues import SOURCE_TAG_QUEUES
    from src.tasks.lending_pr_calling_list_export import cash_buyer_record
    person = {"list_key": "cash_buyer", "radar_id": "P1", "ownership_type": "Individual", "principal_name": None,
              "owner_name": "MCGOVERN,STEPHEN & JULIANNE", "property_address": "43 W High Point Rd",
              "city": "Stuart", "zip": "34996", "county": "Martin", "state": "FL", "apn": "1",
              "match_strength": None}
    record = cash_buyer_record(person, "+17725550100")
    assert record["pool"] == SOURCE_TAG_QUEUES[record["source_tag"]] == "transaction_ready"
    assert record["borrower_name"] == "Stephen Mcgovern" and record["entity_name"] is None


def test_the_export_loads_the_phone_that_passed_not_the_first_blocked_one():
    from types import SimpleNamespace
    from src.tasks.lending_pr_calling_list_export import _phone_to_load
    blocked = SimpleNamespace(national_dnc=True, state_dnc=False, litigator=False)
    clean = SimpleNamespace(national_dnc=False, state_dnc=False, litigator=False)
    a, b = "+18135550101", "+18135550102"
    assert _phone_to_load([a, b], {a: blocked, b: clean}) == b
    assert _phone_to_load([a, b], {a: blocked}) == b          # second not checked yet: counted as not checked
    assert _phone_to_load([a, b], {a: clean, b: clean}) == a
    assert _phone_to_load([a, b], {a: blocked, b: blocked}) == a

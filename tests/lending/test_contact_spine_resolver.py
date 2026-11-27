"""R1 1.1 identity resolver: records sharing a merge key are one contact; a known landline never merges people."""
import random

from src.lending.spine.identity import (
    Identifier, SourceRecord, email_identifier, entity_identifier, external_identifier, phone_identifier,
)
from src.lending.spine.resolver import resolve

PHONE_A = "+18135550101"
PHONE_B = "+18135550102"
LANDLINE = "+18135550199"


def _rec(source, ref, *ids, name=None):
    return SourceRecord(source=source, ref=str(ref), name=name, identifiers=frozenset(i for i in ids if i))


def _resolve(records, owners=None, source_owners=None, landlines=frozenset()):
    return resolve(records, owners=owners or {}, source_owners=source_owners or {}, landlines=landlines)


def test_two_records_sharing_a_phone_are_one_new_contact():
    records = [_rec("lendingflow_leads", 1, phone_identifier(PHONE_A)),
               _rec("dialer_load_records", 7, phone_identifier("(813) 555-0101"))]
    plan = _resolve(records)
    assert plan.new_seeds == ("dialer_load_records:7",)
    owner = "seed:dialer_load_records:7"
    assert plan.source_owner == {"lendingflow_leads:1": owner, "dialer_load_records:7": owner}
    assert plan.identifier_owner[Identifier("phone", PHONE_A)] == owner
    assert plan.merges == ()


def test_email_entity_and_external_ids_each_merge():
    records = [
        _rec("lendingflow_leads", 1, phone_identifier(PHONE_A), email_identifier("Ann@Example.com"),
             external_identifier("ghl_contact", "G1")),
        _rec("instantly_replies", 2, email_identifier(" ann@example.com ")),
        _rec("dialer_load_records", 3, phone_identifier(PHONE_B), entity_identifier("Smith Holdings, LLC"),
             external_identifier("ghl_contact", "G1")),
        _rec("loan_files", 4, entity_identifier("SMITH HOLDINGS LLC")),
    ]
    plan = _resolve(records)
    assert len(plan.new_seeds) == 1
    assert len(set(plan.source_owner.values())) == 1


def test_known_landline_never_merges_two_people():
    landline = phone_identifier(LANDLINE)
    records = [_rec("web_leads", 1, landline, email_identifier("a@x.com")),
               _rec("web_leads", 2, landline, email_identifier("b@x.com"))]
    plan = _resolve(records, landlines=frozenset({LANDLINE}))
    assert len(plan.new_seeds) == 2
    assert Identifier("phone", LANDLINE) not in plan.identifier_owner
    assert sorted(plan.landline_links) == [(landline, "seed:web_leads:1"), (landline, "seed:web_leads:2")]


def test_existing_contacts_joined_by_a_new_record_merge_into_the_smallest_id():
    a, e = phone_identifier(PHONE_A), email_identifier("e@x.com")
    plan = _resolve([_rec("web_leads", 9, a, e)], owners={a: 5, e: 9})
    assert plan.merges == ((5, 9),)
    assert plan.new_seeds == ()
    assert plan.identifier_owner == {a: "id:5", e: "id:5"}
    assert plan.source_owner == {"web_leads:9": "id:5"}


def test_existing_source_keeps_its_contact():
    plan = _resolve([_rec("web_leads", 3)], source_owners={"web_leads:3": 12})
    assert plan.source_owner == {"web_leads:3": "id:12"}
    assert plan.new_seeds == ()


def test_display_name_prefers_loan_file_over_dialer_record():
    a = phone_identifier(PHONE_A)
    plan = _resolve([_rec("dialer_load_records", 1, a, name="J SMITH"),
                     _rec("loan_files", 2, a, name="John Smith"),
                     _rec("contacts", 3, a)])
    assert plan.display_name == {"seed:contacts:3": "John Smith"}


def test_resolve_is_order_independent():
    records = [_rec("web_leads", i, phone_identifier(f"+1813555{i:04d}"), email_identifier(f"p{i % 3}@x.com"),
                    name=f"P{i}") for i in range(1, 12)]
    first = _resolve(records)
    shuffled = records[:]
    random.Random(4).shuffle(shuffled)
    assert _resolve(shuffled) == first


def test_blank_and_invalid_identifiers_are_dropped():
    assert email_identifier("n/a") is None
    assert email_identifier("") is None
    assert entity_identifier("LLC") is None
    assert entity_identifier("A B") is None
    assert phone_identifier("123") is None
    assert external_identifier("ghl_contact", "  ") is None


def test_similar_llc_names_do_not_merge():
    plan = _resolve([_rec("loan_files", 1, entity_identifier("Smith Holdings LLC")),
                     _rec("loan_files", 2, entity_identifier("Smith Holding LLC"))])
    assert len(plan.new_seeds) == 2


def test_external_ids_on_a_landline_record_never_merge():
    landline = phone_identifier(LANDLINE)
    ghl = external_identifier("ghl_contact", "G-office")
    plan = _resolve([_rec("web_leads", 1, landline, ghl, email_identifier("a@x.com")),
                     _rec("web_leads", 2, landline, ghl, email_identifier("b@x.com"))],
                    landlines=frozenset({LANDLINE}))
    assert len(plan.new_seeds) == 2
    assert ghl not in plan.identifier_owner
    assert (ghl, "seed:web_leads:1") in plan.landline_links


def test_nameless_record_with_only_a_landline_makes_no_contact():
    landline = phone_identifier(LANDLINE)
    plan = _resolve([_rec("call_dispositions", "c1", landline), _rec("contacts", 4, landline)],
                    landlines=frozenset({LANDLINE}))
    assert plan.new_seeds == ()
    assert plan.source_owner == {}

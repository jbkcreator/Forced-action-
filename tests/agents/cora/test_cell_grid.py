from __future__ import annotations

from config.cora_cell_grid import ANGLES, AVENUES, CELL_GRID, OFFERS, get_cell, is_valid_cell


def test_known_cells_resolve():
    for cell_id in CELL_GRID:
        assert is_valid_cell(cell_id)
        cell = get_cell(cell_id)
        assert cell is not None
        assert cell["cell_id"] == cell_id


def test_unknown_cell_is_invalid():
    assert is_valid_cell("not_a_real_cell") is False
    assert get_cell("not_a_real_cell") is None


def test_every_cell_tags_are_in_the_taxonomy():
    for cell in CELL_GRID.values():
        assert cell["offer"] in OFFERS
        assert cell["avenue"] in AVENUES
        assert cell["angle"] in ANGLES


def test_cell_1_founder_tier_blitz_shape():
    cell = get_cell("cell_1_founder_tier_blitz")
    assert cell["offer"] == "founder_tier"
    assert cell["avenue"] == "flippers"
    assert cell["angle"] == "scarcity_seat_number"

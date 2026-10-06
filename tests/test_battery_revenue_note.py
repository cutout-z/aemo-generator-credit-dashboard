"""Battery revenue is gross discharge revenue, and every surface says so (M9).

The caveat existed only on a battery UNIT's revenue KPI. A station with a
battery member (Moorabool, Bulgana, Broadsound ...) and the CSV/XLSX export
carried gross discharge revenue with no note.
"""

from tests.page_js import run


def test_revenue_basis_note_for_units_and_hybrid_stations():
    out = run(["revenueBasisNote"], """[
        revenueBasisNote({fuel_category: 'Battery'}),
        revenueBasisNote({fuel_category: 'Wind', fuel_mix: {Wind: 312, Battery: 2}}),
        revenueBasisNote({fuel_category: 'Wind'}),
    ]""")
    assert out[0] == "discharge only, charging cost not netted"
    assert out[1] == "battery discharge gross, charging cost not netted"
    assert out[2] == ""


def test_export_info_rows_carry_the_revenue_basis():
    rows = run(["revenueBasisNote", "buildInfoRows"], """[
        buildInfoRows({duid: 'HPR1', fuel_category: 'Battery'})[0]['Revenue Basis'],
        buildInfoRows({duid: 'station_Moorabool', fuel_category: 'Wind',
                       fuel_mix: {Wind: 312, Battery: 2}})[0]['Revenue Basis'],
        buildInfoRows({duid: 'BW01', fuel_category: 'Fossil'})[0]['Revenue Basis'],
    ]""")
    assert rows == [
        "discharge only, charging cost not netted",
        "battery discharge gross, charging cost not netted",
        "spot energy revenue",
    ]


def test_kpi_uses_the_shared_note():
    from tests.page_js import function_source
    src = function_source("renderKPIs")
    assert "revenueBasisNote(data)" in src

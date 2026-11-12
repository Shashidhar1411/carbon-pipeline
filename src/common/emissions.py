"""Emission factors and the carbon-intensity calculation (proposal section 4.2).

IMPORTANT: the factors below are placeholder values in kg CO2 / MWh. Replace them with the EPA eGRID
numbers you actually cite (dbt/seeds/egrid_factors.csv holds the same table for the warehouse). Imports use
the CARB default for unspecified power (0.428 t/MWh).
"""
EMISSION_FACTORS = {          # kg CO2 per MWh
    "Solar": 0.0, "Wind": 0.0, "Geothermal": 0.0, "Small hydro": 0.0, "Large hydro": 0.0, "Nuclear": 0.0,
    "Biomass": 0.0, "Biogas": 0.0,   # biogenic CO2 treated as zero; document this choice in the report
    "Natural Gas": 411.0, "Coal": 995.0, "Other": 400.0,
    "Imports": 428.0,                 # CARB default for unspecified imports
    "Batteries": 0.0,                 # discharge only; see clean_generation()
}
RENEWABLES = ("Solar", "Wind", "Geothermal", "Small hydro", "Large hydro", "Biomass", "Biogas")


def clean_generation(fuels: dict) -> dict:
    """Battery charging shows up as negative MW in CAISO's feed. Charging is load, not generation, so it is
    removed from the denominator. Any other negative value is treated the same way."""
    return {k: (v if v and v > 0 else 0.0) for k, v in fuels.items()}


def carbon_intensity(fuels: dict, factors: dict = None) -> dict:
    factors = factors or EMISSION_FACTORS
    gen = clean_generation(fuels)
    total = sum(gen.values())
    if total <= 0:
        return {"ci": None, "total_mw": 0.0, "renewable_share": None}
    emissions = sum(mw * factors.get(f, 0.0) for f, mw in gen.items())
    renew = sum(gen.get(f, 0.0) for f in RENEWABLES)
    return {"ci": emissions / total, "total_mw": total, "renewable_share": renew / total}

def build_query(
    population: str,
    intervention: str,
    comparison: str = "",
    outcome: str = "",
    year_from: int = 2015,
    year_to: int = 2024,
) -> str:
    terms = []

    if population.strip():
        terms.append(f"({population.strip()})")
    if intervention.strip():
        terms.append(f"({intervention.strip()})")
    if comparison.strip():
        terms.append(f"({comparison.strip()})")
    if outcome.strip():
        terms.append(f"({outcome.strip()})")

    if not terms:
        raise ValueError("Please enter at least one PICO field before searching.")

    query = " AND ".join(terms)
    query += f' AND ("{year_from}"[Date - Publication] : "{year_to}"[Date - Publication])'
    return query
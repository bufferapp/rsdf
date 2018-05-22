from rsdf.redshift import generate_redshift_engine_string


def test_engine():
    engine = generate_redshift_engine_string()
    assert engine.startswith("postgres+psycopg2")

# Snowflake warehouse

`load_snowflake.py` builds the star schema in `sql/snowflake_schema.sql` and loads every season and pipeline run found under `data/`. `sql/warehouse_queries.sql` holds the analysis queries. The Airflow DAG `load_snowflake` runs setup, load and verify daily.

## One-time setup

1. Create the key pair (already done if `.secrets/snowflake_key.p8` exists):
   ```bash
   mkdir -p .secrets && openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out .secrets/snowflake_key.p8
   ```
   ```bash
   openssl rsa -in .secrets/snowflake_key.p8 -pubout -out .secrets/snowflake_key.pub
   ```
2. In a Snowflake worksheet, register the public key (`.secrets/register_key.sql` has the statement ready):
   `ALTER USER <you> SET RSA_PUBLIC_KEY='<key body without the BEGIN/END lines>';`
3. In `.env`, set `SNOWFLAKE_ACCOUNT` (e.g. `ABCDEFG-XY12345`), `SNOWFLAKE_USER`, and `SNOWFLAKE_ROLE`/`SNOWFLAKE_WAREHOUSE` if yours differ from the defaults.

Snowflake is retiring password-only sign-in for scripts, so the loader signs in with the key pair only. `.secrets/` is gitignored.

## Use

```bash
python warehouse/load_snowflake.py check     # who and where you are
python warehouse/load_snowflake.py setup     # tables (safe to rerun)
python warehouse/load_snowflake.py load      # all data; reruns replace, never duplicate
python warehouse/load_snowflake.py verify    # Snowflake's RQ1 numbers vs the Python sweep
python warehouse/load_snowflake.py query season_summary
```

`load --dry-run` builds every table without connecting. `tests/test_warehouse.py` runs the RQ1 query in DuckDB against hand-computed metrics.

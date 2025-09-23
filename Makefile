## `Makefile` (optional convenience)
```makefile
.PHONY: install run run-lineage

install:
\tpip install -r projects/retail/requirements.txt
\tpip install -e .

run:
\tpython projects/retail/pipelines/daily_sales.py

run-lineage:
\tPROJECT=retail EMIT_LINEAGE=1 OPENLINEAGE_URL?=http://localhost:8000/api/v1/lineage \\
\t\tpython projects/retail/pipelines/daily_sales.py
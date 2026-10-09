# Lattice engine library. `just test` is the local check for this package.

gen:
    uv run python scripts/codegen.py

test:
    uv run pytest

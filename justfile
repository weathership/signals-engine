# Lattice engine library. `just test` is the local check for this package.

# Refresh the packaged proto snapshot and stubs after a signals-protocol commit.
gen:
    uv run python scripts/codegen.py

test:
    uv run pytest

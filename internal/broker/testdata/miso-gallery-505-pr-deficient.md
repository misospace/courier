Addresses #502.

## What

- `Dockerfile`: add `COPY assets/ assets/` to the COPY group.
- `tests/test_dockerfile_packaging.py`: add `test_dockerfile_packages_assets` (a Dockerfile-shape regression that fails if the assets COPY line is removed) and `test_assets_present_in_repo` (tracked-file existence checks).

## Invariants

- `.dockerignore` must not exclude `assets/`: maintained invariant, inspected by hand in this change — no test enforces it.

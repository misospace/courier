Closes #502. The production image omitted the tracked `assets/` directory; this change packages it and adds packaging regressions.

## What

- `Dockerfile`: add `COPY assets/ assets/` to the COPY group.
- `tests/test_dockerfile_packaging.py`: add `test_dockerfile_packages_assets`, `test_assets_present_in_repo`, and `test_dockerignore_does_not_exclude_assets`.

## Criterion evidence

courier-criteria:v1 source=misospace/miso-gallery#502 set=sha256:334695f640d0d3bf865b3dff3471c2f315e12e67067fb70e37e714d1d2be330e coverage=full

- AC-1 sha256:d75abd832b58325a5768c070502f0138f3dc5f95b4fc3abc64628bd005ee7073: regression-check - `Dockerfile` `COPY assets/ assets/`; `tests/test_dockerfile_packaging.py::test_dockerfile_packages_assets` fails when the assets COPY line is removed
- AC-3 sha256:df29213668483fcbbcfcb93264f97debb49330267466d550b73452dc52bbdb76: regression-check - `tests/test_dockerfile_packaging.py::test_dockerfile_packages_assets` and `test_assets_present_in_repo`; existing packaging tests remain unmodified and green

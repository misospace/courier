Closes #502. The production image omitted the tracked `assets/` directory; this change packages it, adds packaging regressions, and extends the built-image smoke test to assert the public icon endpoints.

## What

- `Dockerfile`: add `COPY assets/ assets/` to the COPY group.
- `tests/test_dockerfile_packaging.py`: add `test_dockerfile_packages_assets`, `test_assets_present_in_repo`, and `test_dockerignore_does_not_exclude_assets`.
- `.github/workflows/tests.yaml`: extend the built-image smoke job with a "Verify asset endpoints in built image" step asserting HTTP 200, `image/*` content type, and a non-empty body for `/favicon.ico`, `/assets/icon-192.png`, and `/assets/icon-512.png` against the running `miso-gallery:test` container.

## Criterion evidence

courier-criteria:v1 source=misospace/miso-gallery#502 set=sha256:334695f640d0d3bf865b3dff3471c2f315e12e67067fb70e37e714d1d2be330e coverage=full

- AC-1 sha256:d75abd832b58325a5768c070502f0138f3dc5f95b4fc3abc64628bd005ee7073: regression-check - `Dockerfile` `COPY assets/ assets/` ships the three PNGs; `tests/test_dockerfile_packaging.py::test_dockerfile_packages_assets` fails if that COPY line is removed
- AC-2 sha256:8ca8251a2b0c53842caf982db2585f26ae5ce2b9cc06c0ac9289347cd79cd39c: regression-check - `.github/workflows/tests.yaml` step "Verify asset endpoints in built image" asserts 200 + `image/*` + non-empty body for `/favicon.ico` and both manifest icon URLs against the running built image with FAVICON_URL unset; without the COPY the step fails (204/404)
- AC-3 sha256:df29213668483fcbbcfcb93264f97debb49330267466d550b73452dc52bbdb76: regression-check - `tests/test_dockerfile_packaging.py::test_dockerfile_packages_assets` detects omission of assets from the Docker build instructions; `test_assets_present_in_repo` and `test_dockerignore_does_not_exclude_assets` cover the remaining packaging invariants; the existing packaging tests are unmodified and green

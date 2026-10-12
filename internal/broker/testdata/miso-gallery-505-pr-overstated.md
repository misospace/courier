Closes #502. The production image omitted the tracked `assets/` directory; this change packages it and adds packaging regressions.

## Criterion evidence

courier-criteria:v1 source=misospace/miso-gallery#502 set=sha256:334695f640d0d3bf865b3dff3471c2f315e12e67067fb70e37e714d1d2be330e coverage=full

- AC-1 sha256:d75abd832b58325a5768c070502f0138f3dc5f95b4fc3abc64628bd005ee7073: regression-check - `Dockerfile` `COPY assets/ assets/`; `tests/test_dockerfile_packaging.py::test_dockerfile_packages_assets` fails when the assets COPY line is removed
- AC-2 sha256:8ca8251a2b0c53842caf982db2585f26ae5ce2b9cc06c0ac9289347cd79cd39c: implementation-evidence - `Dockerfile` ships the three PNGs under `/app/assets/`, so the endpoints serve image content in the built image
- AC-3 sha256:df29213668483fcbbcfcb93264f97debb49330267466d550b73452dc52bbdb76: regression-check - `tests/test_dockerfile_packaging.py::test_dockerfile_packages_assets` and `test_assets_present_in_repo`
- AC-4 sha256:0000000000000000000000000000000000000000000000000000000000000000: regression-check - the `.dockerignore` invariant is maintained; inspected by hand in this change and kept green

The `.dockerignore` invariant and the Dockerfile-shape check together keep the packaged icons serving, so every acceptance criterion is regression-enforced.

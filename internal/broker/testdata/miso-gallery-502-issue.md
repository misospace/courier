Ask: Include the tracked assets directory in the Docker image and add a packaging regression test for its public icon endpoints.
Expected files: `Dockerfile`, `tests/test_dockerfile_packaging.py`

**Problem:**
The production image omits all three tracked PNG assets. The PWA manifest points at missing icons, the default favicon falls back to an empty 204, and pages referencing the gallery logo cannot load it. Checkout-based tests pass because they can see assets that the image never copies. This is a deployment packaging defect, not the removed nonexistent CSS/JS URLs covered by closed #479.

**Evidence:**
Verified on default branch `main` at `7a7373bca83f4b5ee2174f90dd3805ef7cf561e4`.
- `Dockerfile:5`, `Dockerfile:8`, `Dockerfile:9`, `Dockerfile:10`: COPY instructions include requirements, Python modules, templates and entrypoint only; none includes `assets/`.
- Tracked files: `assets/icon-192.png`, `assets/icon-512.png`, `assets/miso-gallery-logo.png`.
- `app.py:1009`: favicon reads `app.root_path/assets/miso-gallery-logo.png`; `app.py:1013` returns 204 when absent. `app.py:1018` serves assets from the same missing directory.
- `app.py:1035` and `app.py:1041`: manifest advertises `/assets/icon-192.png` and `/assets/icon-512.png`.
- A Flask harness executing the actual current `favicon()` and `assets()` functions against the empty assets layout implied by the Dockerfile returned 204 for `/favicon.ico` and 404 for each of the three PNG URLs. This was a layout reproduction, not a Docker build.
- `tests/test_dockerfile_packaging.py:5` checks inclusion of tag_store.py, but contains no assets packaging assertion.

**Acceptance:**
- The built image contains all three tracked PNGs under `/app/assets/`.
- With FAVICON_URL unset, a running built image returns 200 and image content for `/favicon.ico` and both manifest icon URLs.
- A regression test detects omission of assets from the Docker build instructions; existing packaging tests remain green.

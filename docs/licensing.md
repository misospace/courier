# Licensing and corresponding source

Courier is licensed under the GNU Affero General Public License, version 3 only (AGPL-3.0-only). Commercial and personal use are welcome under its terms. The license does not require submitting patches upstream.

## Historical and third-party rights

The previous Apache-2.0 license is preserved in [LICENSES/Apache-2.0.txt](../LICENSES/Apache-2.0.txt). Earlier valid grants remain in force; see [NOTICE](../NOTICE). Independent programs packaged with Courier and third-party code retain their own licenses and notices. The Courier image license label describes Courier, not every component of its base image or toolchain.

## Corresponding source

Official releases publish a revision-named source archive before publishing images. It contains the committed tree, build and installation definitions, and vendored Go build dependencies. The release manager and console binaries embed its URL. The console offers it at /source from every page; the read, contribution and metrics MCP HTTP APIs advertise it in a Link response header with rel="source". The image revision label also identifies the exact repository commit.

For a modified or locally built deployment, set COURIER_SOURCE_URL in both the manager and console environments to the actual no-charge corresponding-source archive. With Helm, use controllers.main.containers.main.env.COURIER_SOURCE_URL and controllers.console.containers.console.env.COURIER_SOURCE_URL. Override the URL for the corresponding deployed build, not upstream main. The console returns 503 instead of pretending to offer source when no valid URL is configured. Never include credentials in the source URL.

The coordinator and harness images include the exact Git source tarball used for their custom Git build, together with its COPYING notice, at /usr/share/doc/courier/third-party/git. The CI license-inventory job collects linked Go license reports and the image jobs produce SPDX SBOMs for all five image targets. These reports are inputs to manual license and redistribution review, not proof of compliance.

Distributors must provide complete corresponding source and applicable notices as required by the licenses of the software they convey. Operators of modified Courier versions supporting remote network interaction must prominently offer the actual corresponding source to remote users, as required by AGPL section 13. An upstream repository link alone is not sufficient if it does not contain the modifications or other required corresponding source. Provide a no-charge, network-accessible source location for that deployed version, including required build and installation scripts. This does not require exposing secrets or unrelated services.

For local chart packaging, copy LICENSE, NOTICE and LICENSES/Apache-2.0.txt into the chart (as the release workflow does) before running helm package. Preserve the separate licenses of chart dependencies.

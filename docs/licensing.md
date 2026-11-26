# Licensing and corresponding source

Courier is licensed under the GNU Affero General Public License, version 3 only (AGPL-3.0-only). Commercial and personal use are welcome under its terms. The license does not require submitting patches upstream.

## Historical and third-party rights

The previous Apache-2.0 license is preserved in [LICENSES/Apache-2.0.txt](../LICENSES/Apache-2.0.txt). Earlier valid grants remain in force; see [NOTICE](../NOTICE). Independent programs packaged with Courier and third-party code retain their own licenses and notices. The Courier image license label describes Courier, not every component of its base image or toolchain.

## Corresponding source

The source repository is https://github.com/misospace/courier. For an official image, use its immutable revision (`org.opencontainers.image.revision` on official release images) to locate the matching commit at `https://github.com/misospace/courier/tree/<revision>`. The associated version tag, build definitions, dependency manifests and release workflows identify the release source. A mutable branch or upstream tag is not sufficient for a modified build.

Distributors must provide complete corresponding source and applicable notices as required by the licenses of the software they convey. Operators of modified Courier versions supporting remote network interaction must prominently offer the actual corresponding source to remote users, as required by AGPL section 13. An upstream repository link alone is not sufficient if it does not contain the modifications or other required corresponding source. Provide a no-charge, network-accessible source location for that deployed version, including required build and installation scripts. This does not require exposing secrets or unrelated services.

For local chart packaging, copy LICENSE, NOTICE and LICENSES/Apache-2.0.txt into the chart (as the release workflow does) before running helm package. Preserve the separate licenses of chart dependencies.

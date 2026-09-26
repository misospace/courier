# Changelog

## [0.2.0](https://github.com/misospace/courier/compare/v0.1.1...v0.2.0) (2026-09-26)


### Features

* add liveness reap and crashloop backstop ([47cdc48](https://github.com/misospace/courier/commit/47cdc48af9fa7fc5747508017aabfb987788bae7))
* add liveness reap and crashloop backstop ([9328efb](https://github.com/misospace/courier/commit/9328efbac0920ab805aca86e1cc8ccc5a7f115b9))


### Bug Fixes

* bound crashloop counter to consecutive wedges and harden liveness reap ([f726390](https://github.com/misospace/courier/commit/f7263902ddbe98b4e56ee347fa7ef2870bb5dcdc))
* **deps:** update kubernetes monorepo (v0.37.0 → v0.37.1) ([a4937a1](https://github.com/misospace/courier/commit/a4937a17ddbbc948e86fe4be1a617f8eceb106f4))
* **deps:** update kubernetes monorepo (v0.37.0 → v0.37.1) ([a0793d4](https://github.com/misospace/courier/commit/a0793d4e733948f2dd52ca108a3d1c4678eea8bb))
* gate source discovery on LaneProfile readiness ([fa11e9d](https://github.com/misospace/courier/commit/fa11e9de6aab86061c71a6db31251efa896ce262))
* gate source discovery on LaneProfile readiness ([7cf9c0c](https://github.com/misospace/courier/commit/7cf9c0c96deedf571f248e1657142bbf407968cc))
* keep runtime artifacts out of checkout and classify committed work ([39d5fd7](https://github.com/misospace/courier/commit/39d5fd7ca4ccd3376c44313142da0bd933c34c84))
* keep runtime artifacts out of checkout and classify committed work ([f4fc19e](https://github.com/misospace/courier/commit/f4fc19eb8eaabd811ca2e428e0d18a7e1bf8fc77))
* parse real opencode mcp list output in capability preflight ([cfef5d3](https://github.com/misospace/courier/commit/cfef5d30c95343452d66806c367539398034c3bf))
* provision narrowly permitted per-run scratch for OpenCode ([a3df7be](https://github.com/misospace/courier/commit/a3df7be1b09a9f5b37fcc6a85c4bd5cb97fb9acc))
* provision narrowly permitted per-run scratch for OpenCode ([1b21579](https://github.com/misospace/courier/commit/1b21579c7446874d73da768c4946a171c6fad54b))
* report an existing open PR for review instead of blocking ([1020b42](https://github.com/misospace/courier/commit/1020b42e2197416110e26aff583c86a667efb641))
* report an existing open PR for review instead of blocking ([516260c](https://github.com/misospace/courier/commit/516260cf470972446dc9cd52ad3a10c93515d0a5)), closes [#135](https://github.com/misospace/courier/issues/135)
* restore blocker diagnosis in fix-pr goal ([d2d6036](https://github.com/misospace/courier/commit/d2d60367c50d2331295e19f879bbc9a7bcc3c989))
* retain coordinator ownership of completion and forge publication ([4f6045f](https://github.com/misospace/courier/commit/4f6045f33c1f3a55f5b9bb1ca898ee3b413437e6))
* retain coordinator ownership of completion and forge publication ([51d7e5b](https://github.com/misospace/courier/commit/51d7e5b3570b5767c0d3bf936235161b257f42c3)), closes [#90](https://github.com/misospace/courier/issues/90)
* sanitize MCP capability names and preserve status-word servers ([e999f3b](https://github.com/misospace/courier/commit/e999f3ba93bfc08bb73db2df51bfa348fba855c7))
* **source:** edge-trigger lane-waiting log to prevent steady-state noise ([d108fb7](https://github.com/misospace/courier/commit/d108fb71ae72fd08879ed42e8db089e0d3b486ed))
* surface configured MCP servers that fail to connect at coordinator start ([f6955c9](https://github.com/misospace/courier/commit/f6955c97ae3626081ff13a76ff41605d4f3d94d5))
* surface configured MCP servers that fail to connect at coordinator start ([17a86d0](https://github.com/misospace/courier/commit/17a86d0a975ab94da745351fe3df38e2ac18eee1)), closes [#101](https://github.com/misospace/courier/issues/101)


### Documentation

* decompose [#109](https://github.com/misospace/courier/issues/109) into scratch and recovery work ([0dbf96b](https://github.com/misospace/courier/commit/0dbf96b884742e62c584ec42beede85d5b62001e))
* define Dispatch follow-up attempt ownership ([6915baa](https://github.com/misospace/courier/commit/6915baac7576907e37d00298578c3bb4d1a09bed))
* **harness:** define trusted run boundary ([92830fd](https://github.com/misospace/courier/commit/92830fd1aa9baa78a415820c12275db00aba803c))
* **harness:** define trusted run boundary ([6254ece](https://github.com/misospace/courier/commit/6254ece3e06b04c75ada86d69d28eb14fcae953e))
* **harness:** fence concurrent tool activity ([318a70e](https://github.com/misospace/courier/commit/318a70e53e22fcfcd024fd6521c7c59716367a9f))
* **harness:** preserve fork and broker choices ([b817bd3](https://github.com/misospace/courier/commit/b817bd3e064e5ba7bc44cb428c18de944aefdf03))
* **harness:** settle [#119](https://github.com/misospace/courier/issues/119) long-tool liveness design ([5e2b03d](https://github.com/misospace/courier/commit/5e2b03d3e4003ecb33c1d36b63c1b43dda26a11b))
* **harness:** settle long-tool liveness contract ([7e10dc0](https://github.com/misospace/courier/commit/7e10dc07144fc53b314dc39a0273857701d98ada))
* keep DESIGN.md current + seed a Decisions log ([e2e02e3](https://github.com/misospace/courier/commit/e2e02e307d69da805cbaa598602679670769dbb0))
* keep DESIGN.md current + seed a Decisions log ([894c8ab](https://github.com/misospace/courier/commit/894c8abd9b908d57e8eb2383ded03c18a22eb6b4))
* settle Dispatch follow-up attempt lifecycle ([#98](https://github.com/misospace/courier/issues/98)) ([74ccfaf](https://github.com/misospace/courier/commit/74ccfaf14504a3b384a32cc04e04b003c3ea1421))
* split scratch permission fix from failure recovery design ([29df9b1](https://github.com/misospace/courier/commit/29df9b163da5773010ce785402adc89fc90d6542))

## [0.1.1](https://github.com/misospace/courier/compare/v0.1.0...v0.1.1) (2026-09-22)


### Features

* select lane-specific execution toolchains ([0b817e7](https://github.com/misospace/courier/commit/0b817e7bf4e94cc2632123f005d588781fc57d14))
* select lane-specific execution toolchains ([f9a610d](https://github.com/misospace/courier/commit/f9a610d23f1c9559dee2e0e166bd18b73f76ea1e))


### Bug Fixes

* harden coordinator runtime contract ([51441df](https://github.com/misospace/courier/commit/51441df78454195ddebf7b18c0fd259dd3faac8f))
* harden coordinator runtime contract ([77d7697](https://github.com/misospace/courier/commit/77d76979758fb67f753b53d333af4b1c28a57235))
* preserve non-root workspace mount ([fa29919](https://github.com/misospace/courier/commit/fa29919d53009c429f2666034e3f7047b23806e6))
* require local work before verification ([051270d](https://github.com/misospace/courier/commit/051270decd35f4f850072e13a80a1d97593be528))
* require local work before verification ([7f3c2fe](https://github.com/misospace/courier/commit/7f3c2fea96e8c34d6a6e9755ff0b990d96ad0d9f))


### Chores

* reset release state to recut 0.1.1 instead of 0.2.0 ([4843cd7](https://github.com/misospace/courier/commit/4843cd71542b235a437e21a9c08ca19c01f077d1))

## 0.1.0 (2026-09-21)


### Features

* add dogfood bootstrap MVP ([379c86d](https://github.com/misospace/courier/commit/379c86d7e61f03b5c353e3356e5ba4ef69927c9a))
* auto-discover Dispatch work ([d439b82](https://github.com/misospace/courier/commit/d439b82a4b9fe70719e2e7c74e2e29bc0a1e6fd7))
* auto-discover Dispatch work ([4fcb119](https://github.com/misospace/courier/commit/4fcb11918769f3b60f4db58895e976662aaa5bf8))
* bootstrap Courier dogfooding MVP ([da66239](https://github.com/misospace/courier/commit/da66239118a264963f2b583d9bd8351ea8d1b9bb))
* build the bootstrap coordinator image ([af932ab](https://github.com/misospace/courier/commit/af932ab54e96aa4b154c37f5fa13d6f8d90a2099))
* **ci:** add CodeQL analysis for Go ([6e5bb38](https://github.com/misospace/courier/commit/6e5bb380195773626e92b7fad6e998fca6b431d2))
* **ci:** allow manual re-run of release publishing ([84ef321](https://github.com/misospace/courier/commit/84ef3213c47da4e1dd35fb0999f5ffd9367c3b03))
* **ci:** standard misospace workflow and automation baseline ([369137a](https://github.com/misospace/courier/commit/369137af025ae981ce4c402a00626ffba4d1c7ee))
* **ci:** sync labels from a manifest ([cc6830d](https://github.com/misospace/courier/commit/cc6830dcac78644c8e48acc409cfd9fc3d6b6d66))
* **ci:** verify gofmt in CI ([23d60e5](https://github.com/misospace/courier/commit/23d60e51ca45aabf565a1cae932db5806fc0c81e))
* **container:** update image golang (1.26.6 → 1.27.1) ([b594fd6](https://github.com/misospace/courier/commit/b594fd6b3955924211afa992a1b50aa7d252e9ea))
* **container:** update image golang (1.26.6 → 1.27.1) ([0d13909](https://github.com/misospace/courier/commit/0d13909511f1b009eb56c928ae44e7c9a72177d9))
* **container:** update image node (22.23.2 → 24.21.0) ([db8c0a8](https://github.com/misospace/courier/commit/db8c0a8f04e375eacd892ec29913e609d2c47304))
* **container:** update image node (22.23.2 → 24.21.0) ([8cff732](https://github.com/misospace/courier/commit/8cff7328f9965f2115735caace41883baa12acd2))
* **controller:** add verification phase ([12fed11](https://github.com/misospace/courier/commit/12fed11a65124e229a032d2fd77ae95218d7a2c4))
* **controller:** reap Done runs after durable retention ([a1a5315](https://github.com/misospace/courier/commit/a1a5315123d697f4e7749eed1352753859c35771))
* **controller:** reap Done runs after durable retention ([2425df8](https://github.com/misospace/courier/commit/2425df8e48f4f74b3a6527c5823bbdbfd9ccdd06))
* **controller:** verify terminal PR state ([c786e6c](https://github.com/misospace/courier/commit/c786e6c14f17e810cb272d27fa0d4f983d56b95c))
* **controller:** verify terminal PR state ([84dfc77](https://github.com/misospace/courier/commit/84dfc77735cc8ce5eb40ed6bf2aea1619537798d))
* **deps:** update module github.com/prometheus/common (v0.70.0 → v0.71.0) ([2fa7541](https://github.com/misospace/courier/commit/2fa7541569cb523aabfd5b69f81a12b6c6a686d7))
* **deps:** update module github.com/prometheus/common (v0.70.0 → v0.71.0) ([b427c8b](https://github.com/misospace/courier/commit/b427c8bfb20453c5c15d07e14027a545862c99b6))
* **deps:** update module sigs.k8s.io/controller-runtime (v0.19.3 → v0.25.1) ([84fb6df](https://github.com/misospace/courier/commit/84fb6df02002f2ef4ba97e021952c0ece1f5329d))
* **deps:** update module sigs.k8s.io/controller-runtime (v0.19.3 → v0.25.1) ([82ac22c](https://github.com/misospace/courier/commit/82ac22c729bbcf14230d8fd93902cd6a93e70d12))
* **executor:** configure OpenCode agent selection ([c30f1df](https://github.com/misospace/courier/commit/c30f1dff7bd0d63108e3823e877098c466c6267f))
* **executor:** wire coordinator roles and MCP tools ([f047c3d](https://github.com/misospace/courier/commit/f047c3dbf564a8473dcc77bf125782e1f444888e))
* **executor:** wire coordinator roles and MCP tools ([c343a9d](https://github.com/misospace/courier/commit/c343a9d514bf05dab0ab82800bbd81b2e7bad914))
* **github:** separate API credentials ([bcbf215](https://github.com/misospace/courier/commit/bcbf215a88c2788861095ce61bfe91f0c1c8672c))
* **github:** separate API credentials ([a863c45](https://github.com/misospace/courier/commit/a863c454c93d8099956fc25466bcf92834455efd))
* initial courier scaffold + design ([e50e7ba](https://github.com/misospace/courier/commit/e50e7ba9825a0a07a7894cbf7cc20c55aef478cd))
* **log:** structured run events with secret redaction ([42b2875](https://github.com/misospace/courier/commit/42b287575748353b63993c65c6ccdee2128a6702))
* **log:** structured run events with secret redaction ([763c9cc](https://github.com/misospace/courier/commit/763c9ccdbe433abd4a829fa68046079873d557fe))
* **metrics:** add model-server load mini-MCP server ([70381a5](https://github.com/misospace/courier/commit/70381a58e913c55369aa59781c887a57cf7eb62b))
* **metrics:** add model-server load mini-MCP server ([1fa6498](https://github.com/misospace/courier/commit/1fa649892378dfebf1613392935637e904889b8d))
* **release:** publish images and OCI helm chart ([1a6c769](https://github.com/misospace/courier/commit/1a6c76906c213b0c1225ef14a3fcd1b4e64fb6f3))
* **release:** publish images and OCI helm chart ([9ae5b84](https://github.com/misospace/courier/commit/9ae5b84403cfecb0044db3acb89c26b4951079db))
* ship helm chart on bjw-s/common as install path ([170a070](https://github.com/misospace/courier/commit/170a0709528e5ca57f700c26468731db6105260c))


### Bug Fixes

* adopt a resolve branch only when no PR exists for it ([d844d4f](https://github.com/misospace/courier/commit/d844d4fb8392b3decfd18e0d1878d3c76f84bde0))
* build the manager image as the default docker target ([ed7e3a6](https://github.com/misospace/courier/commit/ed7e3a6974ed62fe4e229e8d012e5810090f18ef))
* **ci:** run release-please as the Miso GitHub App ([66b3a4a](https://github.com/misospace/courier/commit/66b3a4af597d3013738b41ff811c87ad06537f73))
* **ci:** run release-please as the Miso GitHub App ([9decff0](https://github.com/misospace/courier/commit/9decff04f2d75462ddd8a5c0b82fc8c5f14b8dd1))
* **ci:** use autobuild mode for CodeQL Go analysis ([c3cc0f0](https://github.com/misospace/courier/commit/c3cc0f0fa0a23b3cbb73c75b959f1ebcbbffdf05))
* **controller:** await CI registration ([46c6a27](https://github.com/misospace/courier/commit/46c6a27e469d25ee772859690e25e7296aa177b7))
* **controller:** derive event verbosity from the run's debug flag ([896c321](https://github.com/misospace/courier/commit/896c321a21699a824e6536e1b5cc3bb8afb58d0e))
* **controller:** require check-set stability before AwaitingReview ([e42ccfe](https://github.com/misospace/courier/commit/e42ccfeadfa69f338747d1ebb3997f81c023d49f))
* **controller:** require check-set stability before AwaitingReview ([32606f1](https://github.com/misospace/courier/commit/32606f1f33f5b108b96fcbe5a165be2f102958b6))
* **controller:** settle green only on consecutive all-green observations ([bc64d9e](https://github.com/misospace/courier/commit/bc64d9e6ce74f885c7e73f3be6c57d0e29a82d22))
* **controller:** skip Resolve once the done-at marker exists ([ce58a8b](https://github.com/misospace/courier/commit/ce58a8b9bf96b77c25bebdf9978b21443d273e27))
* **controller:** wait for CI completion ([9b1e85c](https://github.com/misospace/courier/commit/9b1e85cebc8a578dc9b251e48e954a0d84d27536))
* **deps:** update module github.com/prometheus/client_model (v0.6.2 → v0.6.3) ([6aadcb7](https://github.com/misospace/courier/commit/6aadcb7b52a970a1c8acc2b4b949f5d924222770))
* **deps:** update module github.com/prometheus/client_model (v0.6.2 → v0.6.3) ([48b8f51](https://github.com/misospace/courier/commit/48b8f515f736cbae496059465f4da8bfcf04fc26))
* **docker:** pin base images by sha256 digest ([2d3df8b](https://github.com/misospace/courier/commit/2d3df8b07f490c52ed25bab7789a7090e18df3bd))
* **docker:** pin base images by sha256 digest ([0096055](https://github.com/misospace/courier/commit/0096055a87326496f298f39d3a00781f313cdaba))
* **executor:** redact child output before pod logs ([5f36c53](https://github.com/misospace/courier/commit/5f36c53969194cdfe616652b587125c25fd518bf))
* **github:** fail closed on changing totals ([881347c](https://github.com/misospace/courier/commit/881347c5e2f690da0cc317a6acf4d095cd68ef46))
* **github:** observe complete CI state ([14bcf9c](https://github.com/misospace/courier/commit/14bcf9c8bfb8654d1934c2f5d1aabfee58b59738))
* **helm:** update chart common (5.2.0 → 5.2.1) ([709c578](https://github.com/misospace/courier/commit/709c5780db408e18855206af32b06b4c23cefd62))
* **helm:** update chart common (5.2.0 → 5.2.1) ([07c50aa](https://github.com/misospace/courier/commit/07c50aa221c0e60887f319f16877152c862d44dd))
* isolate Dispatch followup lifecycle ([8767f4e](https://github.com/misospace/courier/commit/8767f4ebead12a2d90cefdac6fac2c5c26c1c6e2))
* **log:** suppress over-long lines instead of segmenting them ([631fedd](https://github.com/misospace/courier/commit/631fedd101d80a5a5ef63ba1f0e248f5fc775ee8))
* make bootstrap tests hermetic ([3b4a9f8](https://github.com/misospace/courier/commit/3b4a9f828f164827a53f04486148058b3d3c56dd))
* make lastCommit harness-owned status ([a10d510](https://github.com/misospace/courier/commit/a10d5106172d4867e1e8862f54fa183cc267a63b))
* **metrics:** harden scrape bounds, non-finite gauges, and URL redaction ([9e5f032](https://github.com/misospace/courier/commit/9e5f0322102cefde076fb1f9deea2756f12704ed))
* pin git identity in workspace tests ([a62a15c](https://github.com/misospace/courier/commit/a62a15cf75de4b732f32386b5e30f041a3ee7d49))
* register chart dependency repo in ci ([fcefa94](https://github.com/misospace/courier/commit/fcefa94891a83782d7955788cfc58d47b500bcaa))
* **release:** bootstrap first release at v0.1.0 ([24a8c1e](https://github.com/misospace/courier/commit/24a8c1e40830a1ed69c7f2252f8d7f704210fb9d))
* render CRDs as upgradeable chart resources ([cb014db](https://github.com/misospace/courier/commit/cb014db0a6b5446e400a1a53e605dc419ff32688))
* requeue pending runs while their lane is full ([0e4fb30](https://github.com/misospace/courier/commit/0e4fb302e1878619445bb076f2c0e107674819ed))
* write status with narrow json merge patches ([b65ddd1](https://github.com/misospace/courier/commit/b65ddd11542cf580cbe41da40a0b6818dbd44d1e))


### Chores

* align repository protection with the maintained misospace baseline ([754e6ef](https://github.com/misospace/courier/commit/754e6efeb0179a32b57b8cf034aa28049768725e))
* align repository protection with the maintained misospace baseline ([343f9be](https://github.com/misospace/courier/commit/343f9bece2f4e4cf294a49da208cb21d78e428fa))
* align repository protection with the maintained misospace baseline ([2a3c6f3](https://github.com/misospace/courier/commit/2a3c6f3fd2cd31847a2c0c99a56ca2f620eb1346))
* **ci:** parallelize validation jobs and cache Docker builds ([b2f4431](https://github.com/misospace/courier/commit/b2f44318b674d9bfa8faf5a43b0241d04e0b9e3f))
* **ci:** parallelize validation jobs and cache Docker builds ([d6b415c](https://github.com/misospace/courier/commit/d6b415c059fab6fc172212674f343abe7846de4b))
* **ci:** pin GitHub Actions to full semver tags ([898b980](https://github.com/misospace/courier/commit/898b9800ef2c9e6d13ca958838f1bb7e43e4da2b))
* **ci:** pin GitHub Actions to full semver tags ([cd53b36](https://github.com/misospace/courier/commit/cd53b36459dfddeea3833ffd608eb45608a5fe8c))
* clarify the crds-disabled ci assertion ([e09112a](https://github.com/misospace/courier/commit/e09112a3583cabe697c1210445647df8926e8d26))


### Documentation

* add security disclosure policy ([400223e](https://github.com/misospace/courier/commit/400223e774b1b6124b46c194af5761d6046e4e70))
* add security disclosure policy ([b839346](https://github.com/misospace/courier/commit/b8393466fca31a8bbd3fc049f8709f4d31811b6a))
* document repository settings and the human merge gate ([a06e6a6](https://github.com/misospace/courier/commit/a06e6a65ba934636bca0b7a9e5468c9aa235b68f))
* note the chart's CRD manifest location ([f22eab9](https://github.com/misospace/courier/commit/f22eab96508041bb3a4055cb344e7dae68ef7b51))
* repository settings, main protection, and the human merge gate ([25803c8](https://github.com/misospace/courier/commit/25803c8177867a057ec1c025cd7bd78081eefa13))


### Styles

* apply go fmt ([4448309](https://github.com/misospace/courier/commit/44483097a13bcc8198968383463d38e0da74dea1))


### Refactors

* apply operator status via ssa field manager ([6f5fbd2](https://github.com/misospace/courier/commit/6f5fbd222e50ef1be72914b6f6c5047c186fe5bc))
* make reconciler the sole phase writer ([5f27a8f](https://github.com/misospace/courier/commit/5f27a8f58de8e9cbed9aa690f66f1f0d42bf65f4))

## 0.1.0 (2026-09-19)

Initial release.

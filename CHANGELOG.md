# Changelog

## [0.1.7](https://github.com/misospace/courier/compare/v0.1.6...v0.1.7) (2026-10-10)


### Bug Fixes

* gate source discovery on lane capacity ([#260](https://github.com/misospace/courier/issues/260)) ([b8b47b3](https://github.com/misospace/courier/commit/b8b47b3e8469b3ef49a3ae5cdbeda688ef2d71fa))

## [0.1.6](https://github.com/misospace/courier/compare/v0.1.5...v0.1.6) (2026-10-09)


### Bug Fixes

* **container:** update image golang (1.27.1 → 1.27.2) ([#244](https://github.com/misospace/courier/issues/244)) ([f41a29c](https://github.com/misospace/courier/commit/f41a29cf0d6b6e21828cbe616d6491a7683a5d0a))
* **courier:** route Dispatch runs by agent identity ([#253](https://github.com/misospace/courier/issues/253)) ([4cd159e](https://github.com/misospace/courier/commit/4cd159e518f431fa8279c908b886591a37055556))
* patch Go security vulnerabilities ([#254](https://github.com/misospace/courier/issues/254)) ([36f755f](https://github.com/misospace/courier/commit/36f755ffcd46501171d8f8bbde02c00ea9946474))


### Documentation

* settle operator soft-stop design ([#238](https://github.com/misospace/courier/issues/238)) ([#256](https://github.com/misospace/courier/issues/256)) ([62a566b](https://github.com/misospace/courier/commit/62a566b4fc2805d1d2e46e70ea9b7723d144a8fc))

## [0.1.5](https://github.com/misospace/courier/compare/v0.1.4...v0.1.5) (2026-10-08)


### Features

* authenticate harness status and recover resume ([#236](https://github.com/misospace/courier/issues/236)) ([703df12](https://github.com/misospace/courier/commit/703df12439b63b39547dc0a8509b55ba00283fe6))
* **container:** update image busybox (1.36 → 1.38) ([#208](https://github.com/misospace/courier/issues/208)) ([7afb06e](https://github.com/misospace/courier/commit/7afb06e27fd8bb76653a91c6869d11b4b82f15da))
* **deps:** update module github.com/prometheus/client_golang (v1.24.1 → v1.25.0) ([#242](https://github.com/misospace/courier/issues/242)) ([5b2317f](https://github.com/misospace/courier/commit/5b2317ffe408139f73c1f9b87d8c1019c0f0cd98))
* **evidence:** bounded, fail-closed worktree evidence capture core ([#197](https://github.com/misospace/courier/issues/197)) ([#230](https://github.com/misospace/courier/issues/230)) ([a5e1c3a](https://github.com/misospace/courier/commit/a5e1c3acae1f39a428738c36e12c963d595b0755))
* export CoderRun duration, queue-wait and outcome metrics ([#194](https://github.com/misospace/courier/issues/194)) ([96b8435](https://github.com/misospace/courier/commit/96b84351803637d1f94c409a6b1dad530ef7e45d))
* **harness:** authenticate status writes and fence liveness by UID ([#233](https://github.com/misospace/courier/issues/233)) ([195a4b4](https://github.com/misospace/courier/commit/195a4b4f10f431fe51d3592173d0067e97c2d736))
* **harness:** normalize model streams and delegate briefs ([#213](https://github.com/misospace/courier/issues/213)) ([84b8cbe](https://github.com/misospace/courier/commit/84b8cbefd56368573a4c89563c2a5069bb03f752))
* **harness:** validate worker artifacts and publish briefs ([#225](https://github.com/misospace/courier/issues/225)) ([07718c1](https://github.com/misospace/courier/commit/07718c1d1e1b698ac676b0f3a090b24071cdcf14))
* isolate trusted harness and shell pods ([#206](https://github.com/misospace/courier/issues/206)) ([6b1709c](https://github.com/misospace/courier/commit/6b1709c915983f36e3243e60936a7d8bcba03ae5))
* per-run telemetry from opencode events ([#172](https://github.com/misospace/courier/issues/172)) ([#188](https://github.com/misospace/courier/issues/188)) ([ceef16a](https://github.com/misospace/courier/commit/ceef16aff73c25b642669ecb494bf04ce9552622))
* reconcile deployment-managed bootstrap LaneProfiles ([#212](https://github.com/misospace/courier/issues/212)) ([3fed8be](https://github.com/misospace/courier/commit/3fed8befe955db7278fd26ef3504a8e2aef3b107))
* record admitted/started/finished timestamps on CoderRun status ([#187](https://github.com/misospace/courier/issues/187)) ([5d35267](https://github.com/misospace/courier/commit/5d3526754ea52a9c9b586179d0a86f0fafc21e3a))
* support multiple Dispatch lane bindings in one deployment ([#193](https://github.com/misospace/courier/issues/193)) ([0892e9a](https://github.com/misospace/courier/commit/0892e9ae8b03cb219bea686aa1f3387a49d32204))


### Bug Fixes

* **courier:** hand off external CI verification ([#232](https://github.com/misospace/courier/issues/232)) ([2f2d8f0](https://github.com/misospace/courier/commit/2f2d8f0bfce0fbc1c23b3e094cf8abb948a16328))
* **deps:** update module github.com/prometheus/client_golang (v1.24.0 → v1.24.1) ([#195](https://github.com/misospace/courier/issues/195)) ([a83b1ee](https://github.com/misospace/courier/commit/a83b1ee768bde715d4eb423f7b3ca1ed4391416f))
* **harness:** unbundle worker artifacts in an isolated repository ([#239](https://github.com/misospace/courier/issues/239)) ([6b5bfe6](https://github.com/misospace/courier/commit/6b5bfe67183c8ef51a1ea4cb8bfc1ec8bb7aeccb))
* re-confirm every wedge-path delete before charging the crashloop counter ([#224](https://github.com/misospace/courier/issues/224)) ([e0c2a06](https://github.com/misospace/courier/commit/e0c2a06de47cec94619282e599c641ac9cdd9df5)), closes [#105](https://github.com/misospace/courier/issues/105)
* relaunch Running runs whose coordinator pod disappears ([#218](https://github.com/misospace/courier/issues/218)) ([b5f75f9](https://github.com/misospace/courier/commit/b5f75f953366b18250f6e7ac1d2a36d5da69511a))


### Chores

* **ai-review:** upgrade reviewer to v3.2.0 ([#196](https://github.com/misospace/courier/issues/196)) ([0c9ab2f](https://github.com/misospace/courier/commit/0c9ab2f52ec136c26734535e5f9263bdb7f05f98))
* **container:** update image golang (1e93e00 → 162be52) ([#231](https://github.com/misospace/courier/issues/231)) ([f301153](https://github.com/misospace/courier/commit/f301153dcf329d8044ac7c7ec62bc1c24a1e2850))
* **container:** update image golang (e0174e5 → 1e93e00) ([#227](https://github.com/misospace/courier/issues/227)) ([49dcba5](https://github.com/misospace/courier/commit/49dcba59816fc60ceea947abec3e74969659b846))
* **container:** update image node (0e0ff40 → d6aa754) ([#226](https://github.com/misospace/courier/issues/226)) ([e31fe75](https://github.com/misospace/courier/commit/e31fe751dee2bc7dbde43938ad5108fca5994097))


### Documentation

* **harness:** settle provider registration and worker artifact contracts ([#205](https://github.com/misospace/courier/issues/205)) ([085957f](https://github.com/misospace/courier/commit/085957f3dae0c38b9ed10033d2e10190a93f4e0f))
* settle durable failure evidence design for dirty runs ([#115](https://github.com/misospace/courier/issues/115)) ([#203](https://github.com/misospace/courier/issues/203)) ([bead1a1](https://github.com/misospace/courier/commit/bead1a131f59fbf88327154fdba1acd20e66f71c))

## [0.1.4](https://github.com/misospace/courier/compare/v0.1.3...v0.1.4) (2026-10-01)


### Features

* **broker:** add guarded publication primitives ([#156](https://github.com/misospace/courier/issues/156)) ([e50635e](https://github.com/misospace/courier/commit/e50635ee65d2098abdaffba489c0842ffbac080f))
* **broker:** complete [#122](https://github.com/misospace/courier/issues/122) primitive enforcement layer ([a990d58](https://github.com/misospace/courier/commit/a990d58c4e1d4dee0444b9b9f6f560b9043bf91a))
* **broker:** complete [#122](https://github.com/misospace/courier/issues/122) primitive enforcement layer ([07288c6](https://github.com/misospace/courier/commit/07288c63098da440a23b538e7a8e71870ce99c4d))
* **deps:** update module github.com/prometheus/common (v0.71.0 → v0.72.0) ([26d87eb](https://github.com/misospace/courier/commit/26d87ebc41643ac1e3560227274ccd6f5945b77b))
* **deps:** update module github.com/prometheus/common (v0.71.0 → v0.72.0) ([f392ba4](https://github.com/misospace/courier/commit/f392ba4b19ccee488f0da77b74c91bbabed988b2))
* **executor:** classify run endings from a coordinator-declared outcome ([ad2ca1f](https://github.com/misospace/courier/commit/ad2ca1fef039eef91f35f276a45d88b8a627a28b))
* **executor:** classify run endings from a coordinator-declared outcome ([38abef1](https://github.com/misospace/courier/commit/38abef1bc079e5c7df2efed6413e44af07781d92))
* **executor:** resume coordinator session on recoverable endings ([58b18a6](https://github.com/misospace/courier/commit/58b18a6220bbc501dea69b8c04666e4e30256d12))
* **executor:** resume coordinator session on recoverable endings ([0fbd648](https://github.com/misospace/courier/commit/0fbd64822b6aa8a0bd833a4c98658409fa2a67ce)), closes [#170](https://github.com/misospace/courier/issues/170)


### Bug Fixes

* **broker:** close import race window and harden git transport ([ac09236](https://github.com/misospace/courier/commit/ac092368aec37ec8f1b7b905684b30f7986614fb))
* **broker:** confirm uncertain push on exact live OID ([63e4789](https://github.com/misospace/courier/commit/63e47897cde46459c1fd1a57dc786c6a4cb78733))
* **broker:** fail closed on unlabeled duplicate and non-canonical spec repo ([a1a5972](https://github.com/misospace/courier/commit/a1a597283108fb83dedf9e7626bfe171d232bcd1))
* **controller:** keep a declared no_change_needed from settling the source ([6aa35fb](https://github.com/misospace/courier/commit/6aa35fb55b00b5bdb0a2d1be4e508b5375633234))
* **controller:** never block a run's terminal phase on a source report ([e7005e3](https://github.com/misospace/courier/commit/e7005e3664fc7bcad542f405c23ad751d5c319f6))
* **controller:** persist the pending report marker with the terminal phase ([da29e21](https://github.com/misospace/courier/commit/da29e21eec2ac394b5339fe8bd1536bc24d97ca5))
* **controller:** terminalize the run before reporting to the source ([0adb844](https://github.com/misospace/courier/commit/0adb84473c3613f0467ad4c6cb4a98e9c6a94c80))
* **deps:** update module sigs.k8s.io/controller-runtime (v0.25.1 → v0.25.2) ([d90e8be](https://github.com/misospace/courier/commit/d90e8be33a8504bc7307c9514ece1fc07107d662))
* **deps:** update module sigs.k8s.io/controller-runtime (v0.25.1 → v0.25.2) ([8c081a3](https://github.com/misospace/courier/commit/8c081a39c9089b4e563899f60b75c5aba3da7997))
* **executor:** close exit-code passthrough and declaration resurrection gaps ([2c1efac](https://github.com/misospace/courier/commit/2c1efac647cd7eba5790b5b0882de82b0b8fcc52))
* **executor:** correct declared outcome handoffs ([d9afa44](https://github.com/misospace/courier/commit/d9afa447e28cda2dc6a02c3fba16f61596c7736a))
* **executor:** drop dead path unquoting, clamp resume backoff, finish docs ([c26ac8a](https://github.com/misospace/courier/commit/c26ac8a6524299605841aa26f84249bb8c99e32f))
* **executor:** harden session resume per review ([c07fb8e](https://github.com/misospace/courier/commit/c07fb8e2d6cdbf16c28222fc2b6affe2690d72b8))
* **executor:** hash structured continuation state ([3a10794](https://github.com/misospace/courier/commit/3a10794ba896eaf00e667403a1f615fbe1b6e69c))
* **executor:** resolve main merge conflicts ([18a4aa2](https://github.com/misospace/courier/commit/18a4aa2a25d8ef73d35d274dca7db8eed21647ab))
* inspect lane toolchain sources through a read-only reference mount ([540eeed](https://github.com/misospace/courier/commit/540eeedf59b0c5b5180692952ed307876be9b02b))
* **source:** settle the PR-fix attempt without parking for no_change_needed ([ecf879d](https://github.com/misospace/courier/commit/ecf879d72d17126cd5592997e48da49ed8c3cec4))


### Chores

* add CODEOWNERS ([42c38ff](https://github.com/misospace/courier/commit/42c38ff6f4f767eec722c92322f1093cbb6db4aa))
* add CODEOWNERS ([461b179](https://github.com/misospace/courier/commit/461b179a8d07b35e0dc0c7480b80beae08e7b688))
* **container:** update image golang (3680233 → e0174e5) ([8b5d4c4](https://github.com/misospace/courier/commit/8b5d4c41b204c66aace8f5d5d2c4aab945f848e7))
* **container:** update image golang (3680233 → e0174e5) ([fda0b1d](https://github.com/misospace/courier/commit/fda0b1d0a411db10db65188ef39a9a5e806fe3c6))
* **release:** keep feat bumps at patch until 1.0 ([fea01ff](https://github.com/misospace/courier/commit/fea01ff3c5f54d35354de905ed4de2d6de771486))
* **release:** keep feat bumps at patch until 1.0 ([80d1e5f](https://github.com/misospace/courier/commit/80d1e5f6e518d53a2037a736966445899739388a))


### Documentation

* cross-reference the exit-3 contract on both sides ([ea7f09f](https://github.com/misospace/courier/commit/ea7f09fae6219b13b2241b1582448642413e7d38))
* drop stray leading spaces on outcome-list bullets ([4bc2296](https://github.com/misospace/courier/commit/4bc2296bf88a9d96ab210b4128cdf6f2477c714c))
* **source:** pin WakeReviewer to the blocked Result in its comment ([168b4ef](https://github.com/misospace/courier/commit/168b4efd1922cdf3b17040e8e7df9c32b74bfc18))


### Refactors

* **broker:** register a created PR only after full verification ([1cd9dca](https://github.com/misospace/courier/commit/1cd9dca3494e61d15cbb26f1240a9e0b96440032))
* **controller:** name the no_change_needed exit and align harness docs ([017d91f](https://github.com/misospace/courier/commit/017d91ff53135153ceb6f58e60633211f8281471))

## [0.1.3](https://github.com/misospace/courier/compare/v0.1.2...v0.1.3) (2026-09-27)


### Features

* add semantic forge provider contract ([2f0592c](https://github.com/misospace/courier/commit/2f0592c7f5326e692c300c4a90df0897fb89d265))
* add semantic forge provider contract ([29a82a9](https://github.com/misospace/courier/commit/29a82a9f6e883adfe66cbda59baa33c36f2e7d7a))
* suspend a lane without stopping work in flight ([7064e6e](https://github.com/misospace/courier/commit/7064e6ef59b2037030fd1b9b65d887e639070d6f))
* suspend a lane without stopping work in flight ([e8b7cf5](https://github.com/misospace/courier/commit/e8b7cf5c23393ba05230c784ae7f5dd043d283d5))


### Bug Fixes

* **dispatch:** dedupe follow-up runs on the PR-fix attempt, not the task URL ([36e5d6f](https://github.com/misospace/courier/commit/36e5d6f0df834fd038352299e41a5112a7eb4de3))
* **dispatch:** dedupe follow-up runs on the PR-fix attempt, not the task URL ([42fb5ad](https://github.com/misospace/courier/commit/42fb5ad3cc5da355a658dbb81532bbe2c14208ca))
* **forge:** keep draft updates off github patch ([f4f9222](https://github.com/misospace/courier/commit/f4f922225b8522a4d8bcec3a572cb2ddd9ae377e))
* **forge:** retain live pr repository identity ([d6c28da](https://github.com/misospace/courier/commit/d6c28da1c21dc8d4b991e0878613342e721f1c16))


### Chores

* release 0.1.3 ([a5bd9c9](https://github.com/misospace/courier/commit/a5bd9c9d920033967cf10bfc9d0959416731370a))
* release 0.1.3 ([0cac14f](https://github.com/misospace/courier/commit/0cac14fd5eba92fc028d8aa3b70d4f09aecb2718))

## [0.1.2](https://github.com/misospace/courier/compare/v0.1.1...v0.1.2) (2026-09-27)


### Features

* add liveness reap and crashloop backstop ([47cdc48](https://github.com/misospace/courier/commit/47cdc48af9fa7fc5747508017aabfb987788bae7))
* add liveness reap and crashloop backstop ([9328efb](https://github.com/misospace/courier/commit/9328efbac0920ab805aca86e1cc8ccc5a7f115b9))


### Bug Fixes

* bound crashloop counter to consecutive wedges and harden liveness reap ([f726390](https://github.com/misospace/courier/commit/f7263902ddbe98b4e56ee347fa7ef2870bb5dcdc))
* continue the coordinator loop when a tool call is denied ([e4a05c5](https://github.com/misospace/courier/commit/e4a05c58db1f72036f6f1f5aa431d8307a49c8aa))
* continue the coordinator loop when a tool call is denied ([a4817c1](https://github.com/misospace/courier/commit/a4817c1f867fea9306ed226ef44a03cd369c725c))
* **deps:** update kubernetes monorepo (v0.37.0 → v0.37.1) ([a4937a1](https://github.com/misospace/courier/commit/a4937a17ddbbc948e86fe4be1a617f8eceb106f4))
* **deps:** update kubernetes monorepo (v0.37.0 → v0.37.1) ([a0793d4](https://github.com/misospace/courier/commit/a0793d4e733948f2dd52ca108a3d1c4678eea8bb))
* **dispatch:** echo the issued PR-fix attempt token in reports and marks ([34eb300](https://github.com/misospace/courier/commit/34eb3007e27cacadbabb631d15111119bf29658a))
* **dispatch:** echo the issued PR-fix attempt token in reports and marks ([14a9268](https://github.com/misospace/courier/commit/14a9268fceb77c7f631ee04ec65f247e0b4866e7))
* end a run Done when its PR is merged mid-run ([9f6bb9f](https://github.com/misospace/courier/commit/9f6bb9f33acdec130d1254dfe75a0267010508a1))
* end a run Done when its PR is merged mid-run ([10067ef](https://github.com/misospace/courier/commit/10067efebc94458e904175949165162ef5faaee0))
* finish fork head handling in fix-pr runs ([88e74ab](https://github.com/misospace/courier/commit/88e74ab23d0d39cbcfdaf19ecf14f79a0f71d23c))
* gate source discovery on LaneProfile readiness ([fa11e9d](https://github.com/misospace/courier/commit/fa11e9de6aab86061c71a6db31251efa896ce262))
* gate source discovery on LaneProfile readiness ([7cf9c0c](https://github.com/misospace/courier/commit/7cf9c0c96deedf571f248e1657142bbf407968cc))
* hand base-sync merge conflicts to the coordinator ([32d5d33](https://github.com/misospace/courier/commit/32d5d3381896de1764a3d48b15cccb5791f310ae))
* hand base-sync merge conflicts to the coordinator ([cbbf4ae](https://github.com/misospace/courier/commit/cbbf4ae62e348afdae30184eda25602a8bb0909c))
* honor fork head repositories in fix-pr checkouts ([64a0520](https://github.com/misospace/courier/commit/64a05200fec2c88747c95dd1b86433c8507c7e79))
* honor fork head repositories in fix-pr checkouts ([f248752](https://github.com/misospace/courier/commit/f248752474485f0424cea6c42f96f244a4500836))
* keep runtime artifacts out of checkout and classify committed work ([39d5fd7](https://github.com/misospace/courier/commit/39d5fd7ca4ccd3376c44313142da0bd933c34c84))
* keep runtime artifacts out of checkout and classify committed work ([f4fc19e](https://github.com/misospace/courier/commit/f4fc19eb8eaabd811ca2e428e0d18a7e1bf8fc77))
* log terminal PR enrichment misses and clarify test name ([8842a86](https://github.com/misospace/courier/commit/8842a86745dd24431823dd96fc67c9d85bfcc8cf))
* log terminal PR enrichment misses and clarify test name ([8cd7dea](https://github.com/misospace/courier/commit/8cd7deaef444c385e669924ee3eda77142233980))
* parse real opencode mcp list output in capability preflight ([cfef5d3](https://github.com/misospace/courier/commit/cfef5d30c95343452d66806c367539398034c3bf))
* provision narrowly permitted per-run scratch for OpenCode ([a3df7be](https://github.com/misospace/courier/commit/a3df7be1b09a9f5b37fcc6a85c4bd5cb97fb9acc))
* provision narrowly permitted per-run scratch for OpenCode ([1b21579](https://github.com/misospace/courier/commit/1b21579c7446874d73da768c4946a171c6fad54b))
* reconcile observed PR into terminal CoderRun status ([96b5966](https://github.com/misospace/courier/commit/96b59664b3ff4057e1a8ead0d068ab0472bb48f1))
* reconcile observed PR into terminal CoderRun status ([2696dae](https://github.com/misospace/courier/commit/2696dae0ef60b0606c1a66bc55d00d77df688aab)), closes [#103](https://github.com/misospace/courier/issues/103)
* report an existing open PR for review instead of blocking ([1020b42](https://github.com/misospace/courier/commit/1020b42e2197416110e26aff583c86a667efb641))
* report an existing open PR for review instead of blocking ([516260c](https://github.com/misospace/courier/commit/516260cf470972446dc9cd52ad3a10c93515d0a5)), closes [#135](https://github.com/misospace/courier/issues/135)
* report work committed off the run branch as needs-human ([aa357c9](https://github.com/misospace/courier/commit/aa357c9b7a221ad2a4e44c4c8854b00db8cf38bc))
* report work committed off the run branch as needs-human ([a4ae503](https://github.com/misospace/courier/commit/a4ae503ebc09b1d1d1b5bfebed64b6cb0c3625b7))
* restore blocker diagnosis in fix-pr goal ([d2d6036](https://github.com/misospace/courier/commit/d2d60367c50d2331295e19f879bbc9a7bcc3c989))
* retain coordinator ownership of completion and forge publication ([4f6045f](https://github.com/misospace/courier/commit/4f6045f33c1f3a55f5b9bb1ca898ee3b413437e6))
* retain coordinator ownership of completion and forge publication ([51d7e5b](https://github.com/misospace/courier/commit/51d7e5b3570b5767c0d3bf936235161b257f42c3)), closes [#90](https://github.com/misospace/courier/issues/90)
* sanitize MCP capability names and preserve status-word servers ([e999f3b](https://github.com/misospace/courier/commit/e999f3ba93bfc08bb73db2df51bfa348fba855c7))
* **source:** edge-trigger lane-waiting log to prevent steady-state noise ([d108fb7](https://github.com/misospace/courier/commit/d108fb71ae72fd08879ed42e8db089e0d3b486ed))
* surface configured MCP servers that fail to connect at coordinator start ([f6955c9](https://github.com/misospace/courier/commit/f6955c97ae3626081ff13a76ff41605d4f3d94d5))
* surface configured MCP servers that fail to connect at coordinator start ([17a86d0](https://github.com/misospace/courier/commit/17a86d0a975ab94da745351fe3df38e2ac18eee1)), closes [#101](https://github.com/misospace/courier/issues/101)


### Chores

* release 0.1.2 ([783991d](https://github.com/misospace/courier/commit/783991d7b1f437188a3d9f45177201140166bbcd))
* release 0.1.2 ([2528cb1](https://github.com/misospace/courier/commit/2528cb1ab1c1c363673ecf00b45cd72083131fea))


### Documentation

* decompose [#109](https://github.com/misospace/courier/issues/109) into scratch and recovery work ([0dbf96b](https://github.com/misospace/courier/commit/0dbf96b884742e62c584ec42beede85d5b62001e))
* define Dispatch follow-up attempt ownership ([6915baa](https://github.com/misospace/courier/commit/6915baac7576907e37d00298578c3bb4d1a09bed))
* **harness:** define trusted run boundary ([92830fd](https://github.com/misospace/courier/commit/92830fd1aa9baa78a415820c12275db00aba803c))
* **harness:** define trusted run boundary ([6254ece](https://github.com/misospace/courier/commit/6254ece3e06b04c75ada86d69d28eb14fcae953e))
* **harness:** fence concurrent tool activity ([318a70e](https://github.com/misospace/courier/commit/318a70e53e22fcfcd024fd6521c7c59716367a9f))
* **harness:** pin run publication policy ([bd0d41c](https://github.com/misospace/courier/commit/bd0d41c664ee85a0b14d7d9d4fff51d162d2bd18))
* **harness:** pin run publication policy ([123a7a0](https://github.com/misospace/courier/commit/123a7a0237a3c2aa1d912b6f1ee0b1fa7023ab57))
* **harness:** preserve fork and broker choices ([b817bd3](https://github.com/misospace/courier/commit/b817bd3e064e5ba7bc44cb428c18de944aefdf03))
* **harness:** settle [#119](https://github.com/misospace/courier/issues/119) long-tool liveness design ([5e2b03d](https://github.com/misospace/courier/commit/5e2b03d3e4003ecb33c1d36b63c1b43dda26a11b))
* **harness:** settle long-tool liveness contract ([7e10dc0](https://github.com/misospace/courier/commit/7e10dc07144fc53b314dc39a0273857701d98ada))
* **harness:** settle run identity and egress ([8809b5f](https://github.com/misospace/courier/commit/8809b5f9e3848ad851559540f2ce5cfec83a704b))
* **harness:** settle run identity and egress ([57284fa](https://github.com/misospace/courier/commit/57284fa1ac7481e0f7f35a630361e165401e44bb))
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

# Changelog

## [0.1.14](https://github.com/misospace/courier/compare/v0.1.13...v0.1.14) (2026-10-31)


### Features

* **admission:** courierctl contribute thin CLI frontend ([#601](https://github.com/misospace/courier/issues/601)) ([#675](https://github.com/misospace/courier/issues/675)) ([6c00b92](https://github.com/misospace/courier/commit/6c00b920473e28a7e1558a20957a470afb5c7059))
* controller-directed successor materialization for manual-source retries ([#676](https://github.com/misospace/courier/issues/676)) ([8f2c3ab](https://github.com/misospace/courier/commit/8f2c3ab33555561bba357a669032df26775d3fa3))
* **skills:** register and materialize approved deployment skill bundles ([#674](https://github.com/misospace/courier/issues/674)) ([bd31fd6](https://github.com/misospace/courier/commit/bd31fd6c4f1858a6c9d02b29cfa6ff7722dee3b7))


### Bug Fixes

* **controller:** pin the admitted base ref into contribute publication policy ([#670](https://github.com/misospace/courier/issues/670)) ([fc1d73c](https://github.com/misospace/courier/commit/fc1d73c7dc2713ae9715081ce852bf5b993a147b)), closes [#653](https://github.com/misospace/courier/issues/653)
* **controller:** prove the secure launch path's read grants in preflight ([#669](https://github.com/misospace/courier/issues/669)) ([0caa7d7](https://github.com/misospace/courier/commit/0caa7d750ca300995485df74bb3ae50d1b2527bc))
* **preflight:** publish probe results with an overwrite-safe redirect ([#673](https://github.com/misospace/courier/issues/673)) ([c62eb0a](https://github.com/misospace/courier/commit/c62eb0a32fc52c7a2a83c1f8c508a74c1da41b67))
* **secure:** stop requiring duplicate Secrets for operator forge observation ([#648](https://github.com/misospace/courier/issues/648)) ([02a16de](https://github.com/misospace/courier/commit/02a16de8871c46accb19c59c7b6682e944b6f15e))
* **source:** count and log dedupe-skipped re-offers ([#668](https://github.com/misospace/courier/issues/668)) ([3e1bdbd](https://github.com/misospace/courier/commit/3e1bdbd38e52a6bbe5409dcc4adcc27dfdbfd2d0))


### Refactors

* **source:** consolidate CoderRun name derivation in source.RunName ([#677](https://github.com/misospace/courier/issues/677)) ([d797304](https://github.com/misospace/courier/commit/d79730418861ef546af1f3647494b87d6e4402c1))

## [0.1.13](https://github.com/misospace/courier/compare/v0.1.12...v0.1.13) (2026-10-31)


### Features

* **executor:** render the admitted contribution goal into execution ([#654](https://github.com/misospace/courier/issues/654)) ([8cef042](https://github.com/misospace/courier/commit/8cef042cdf219fa6fad671c69a30739da971ae5b))
* **source:** best-effort execution observations to Dispatch ([#637](https://github.com/misospace/courier/issues/637)) ([2a2b04b](https://github.com/misospace/courier/commit/2a2b04b463ef4f0d269f3c4537f8ba1a0d7ff54f))


### Bug Fixes

* **chart:** authorize broker tokenreview binding for secure operator ([#645](https://github.com/misospace/courier/issues/645)) ([4731534](https://github.com/misospace/courier/commit/4731534352633dc1d992e8de658d18c9df33b853))
* **controller:** route secure provisioning reads through the uncached API reader ([#642](https://github.com/misospace/courier/issues/642)) ([#646](https://github.com/misospace/courier/issues/646)) ([7debeb6](https://github.com/misospace/courier/commit/7debeb6152ef600ac9cf039a2050bf28ef4758e4))
* **controller:** settle permanent Dispatch refusals on the Done-run resolve path ([#666](https://github.com/misospace/courier/issues/666)) ([e279a5d](https://github.com/misospace/courier/commit/e279a5d416472dff485bad06c26b6629c586b33f))
* **deps:** update module github.com/prometheus/common (v0.73.0 → v0.73.1) ([#649](https://github.com/misospace/courier/issues/649)) ([4d09b77](https://github.com/misospace/courier/commit/4d09b77ec5a645120d755c65343de4eb4a222f6c))
* **dev:** run the documented verification path without GNU make ([#643](https://github.com/misospace/courier/issues/643)) ([54ca510](https://github.com/misospace/courier/commit/54ca5101973934e2920d6ad1a1d7cdb35cb890b7))
* **harness:** wire native context budgets through courier-control ([#660](https://github.com/misospace/courier/issues/660)) ([8205b96](https://github.com/misospace/courier/commit/8205b9648dc83f29980254ead47a5ae76b38d4f7))
* **preflight:** check named broker pod permissions ([#651](https://github.com/misospace/courier/issues/651)) ([22d3f01](https://github.com/misospace/courier/commit/22d3f01c18fd202aa9684152a24bf55eec75c190))
* **secure:** read preflight and credential kinds uncached ([#647](https://github.com/misospace/courier/issues/647)) ([aa0a496](https://github.com/misospace/courier/commit/aa0a496cc9e7f7b0833bc19bac76b791fbe54940))

## [0.1.12](https://github.com/misospace/courier/compare/v0.1.11...v0.1.12) (2026-10-29)


### Features

* **api:** add contribute mode and spec.workRepo for fork-backed contributions ([#615](https://github.com/misospace/courier/issues/615)) ([0496575](https://github.com/misospace/courier/commit/049657561bf2f72b048db70efbc326a5212a0bfc))
* **broker:** fork-backed publication, discovery-based PR idempotency, mode-aware body contract ([#612](https://github.com/misospace/courier/issues/612)) ([911e07e](https://github.com/misospace/courier/commit/911e07ef7028152194557a2f7c582a4795e9603e))
* **broker:** wire the contribute case into the policy binding ([#629](https://github.com/misospace/courier/issues/629)) ([f022ae2](https://github.com/misospace/courier/commit/f022ae2e618cae66890f164b18441b087e99ff27))
* **controller:** contribute admission resolves base read and approved fork write ([#597](https://github.com/misospace/courier/issues/597)) ([#625](https://github.com/misospace/courier/issues/625)) ([4122a8b](https://github.com/misospace/courier/commit/4122a8b2543f42f22bed52d4b35935cda4b1aed7))
* **controller:** durable goal-branch identity and fail-closed contribution admission ([#598](https://github.com/misospace/courier/issues/598)) ([#626](https://github.com/misospace/courier/issues/626)) ([4a357cd](https://github.com/misospace/courier/commit/4a357cd18e83c33d639a40fd0c086dd6424a1d77))
* **controller:** explicit retry request surface and state machine for terminal CoderRuns ([#613](https://github.com/misospace/courier/issues/613)) ([52dd39a](https://github.com/misospace/courier/commit/52dd39aa3f62bce140282be4b4897208d400a557))
* **cron:** add schedule-driven source adapter ([#605](https://github.com/misospace/courier/issues/605)) ([3b54f97](https://github.com/misospace/courier/commit/3b54f971f5661068f2dd8168247e0bb562bc70ce))
* **deps:** update module github.com/prometheus/common (v0.72.1 → v0.73.0) ([#614](https://github.com/misospace/courier/issues/614)) ([dc76909](https://github.com/misospace/courier/commit/dc769099ed98074db39eb5c25fad194d2fd18edd))
* **github-release:** update release node (24.22.1 → 26.12.1) ([#593](https://github.com/misospace/courier/issues/593)) ([e06cbf8](https://github.com/misospace/courier/commit/e06cbf82db47abc840f3b5daef0d899784d60017))
* **harness:** classify trusted native outcomes and verify publication ([#577](https://github.com/misospace/courier/issues/577)) ([ec735db](https://github.com/misospace/courier/commit/ec735db2d3354b246938867526b8832a8aac8f89))
* **source/dispatch:** fenced retry requeue transport for the Dispatch adapter ([#628](https://github.com/misospace/courier/issues/628)) ([00c427c](https://github.com/misospace/courier/commit/00c427c691043ed9a5b50072bd6137a7c7e8fbf5))
* **source:** ad-hoc contribution adapter with source-neutral work identity ([#596](https://github.com/misospace/courier/issues/596)) ([#623](https://github.com/misospace/courier/issues/623)) ([1ba77a3](https://github.com/misospace/courier/commit/1ba77a32497a852a4e8b220a1997f040d29451bf))
* **source:** forge label adapter over typed forge capabilities ([#16](https://github.com/misospace/courier/issues/16)) ([#618](https://github.com/misospace/courier/issues/618)) ([8d6880e](https://github.com/misospace/courier/commit/8d6880e4992a473bc056b3067e338a500178d8c8))
* **source:** retry-aware discovery barrier and successor naming ([#607](https://github.com/misospace/courier/issues/607)) ([3e96f48](https://github.com/misospace/courier/commit/3e96f48907be5dbbd89c0fe690437900c4bddc47))
* **source:** retry-aware discovery barrier and successor naming ([#607](https://github.com/misospace/courier/issues/607)) ([f74f1ca](https://github.com/misospace/courier/commit/f74f1ca2a463ba2f557e2a8334f70e3f780aad71))


### Bug Fixes

* **chart:** fail render when explicit broker-role arg contradicts the historical ClusterRole name ([a754760](https://github.com/misospace/courier/commit/a7547600ce44026609721927115ac90600d07908))
* **chart:** validate bare and single-dash --broker-tokenreview-cluster-role forms at render ([c3555b3](https://github.com/misospace/courier/commit/c3555b3bb5d4fd85d9cbd89731b5944c1978fd15))
* **chart:** validate the effective owned-namespace boundary at render ([00793b9](https://github.com/misospace/courier/commit/00793b9679006cc4b1f77c3c0ce0cb08f959a2ff))
* **controller:** isolate native canary ownership ([58fbdf4](https://github.com/misospace/courier/commit/58fbdf4fc9ad34f2ba96d43456829c913c64863b))
* **controller:** isolate native canary ownership ([029ee23](https://github.com/misospace/courier/commit/029ee23971874e958f59295dd6419f530462f135)), closes [#632](https://github.com/misospace/courier/issues/632)
* **controller:** settle a repair fence a terminal run still holds ([#578](https://github.com/misospace/courier/issues/578)) ([173a1c6](https://github.com/misospace/courier/commit/173a1c6d65a8da7fd5803906b410275f51b36460))
* **controller:** signal an inapplicable ci-repair policy on unattributable heads ([#571](https://github.com/misospace/courier/issues/571)) ([4aafd45](https://github.com/misospace/courier/commit/4aafd4538ba5353aa1bf51cce6fac83556134c25))
* **controller:** start a ci-park's settle window at establishment ([#551](https://github.com/misospace/courier/issues/551)) ([#576](https://github.com/misospace/courier/issues/576)) ([e0d7feb](https://github.com/misospace/courier/commit/e0d7feb2b2d83d5afaa544e4de285922248a3083))
* **deps:** update dependency astro (7.4.2 → 7.4.3) ([#617](https://github.com/misospace/courier/issues/617)) ([dd7ca9a](https://github.com/misospace/courier/commit/dd7ca9a8da80223ae1f15701b002492dc56f84f2))
* **deps:** update dependency astro (7.4.3 → 7.4.4) ([#622](https://github.com/misospace/courier/issues/622)) ([18e2d3a](https://github.com/misospace/courier/commit/18e2d3a99bb1ee60b0c7268f023e35b3b8537797))
* **deps:** update kubernetes monorepo (v0.37.1 → v0.37.2) ([#616](https://github.com/misospace/courier/issues/616)) ([98b137b](https://github.com/misospace/courier/commit/98b137bc15fac94671c03861a9be20db6adba745))
* **executor:** classify a transient forge-API failure in guardAdoption's PR observation as availability ([#585](https://github.com/misospace/courier/issues/585)) ([#588](https://github.com/misospace/courier/issues/588)) ([1bb6d3a](https://github.com/misospace/courier/commit/1bb6d3a5473ed96be636bcd9814f974f3b6bd3fc))
* **executor:** route transient launch-guard remote reads to the recovery channel ([#580](https://github.com/misospace/courier/issues/580)) ([#586](https://github.com/misospace/courier/issues/586)) ([32ea74e](https://github.com/misospace/courier/commit/32ea74ece548a7d14e1ba7d859f6c3d9400be30d))
* **executor:** settle fix-pr assignments merged during the run as superseded ([#582](https://github.com/misospace/courier/issues/582)) ([#583](https://github.com/misospace/courier/issues/583)) ([db64226](https://github.com/misospace/courier/commit/db64226487ae081ecedd3c368dd722f29217215c))
* **git:** probe the effective ssh endpoint resolved from the ssh configuration ([#590](https://github.com/misospace/courier/issues/590)) ([912641e](https://github.com/misospace/courier/commit/912641e2e2a44c608d56aa72ebdb1b7bf74bf03f))
* **git:** resolve the ssh probe through the ssh command git invokes ([#592](https://github.com/misospace/courier/issues/592)) ([432005d](https://github.com/misospace/courier/commit/432005d1e2f76bb5c70959ae6565be3f1da0fc67))
* **harness:** classify a git-side remote-publication read failure as transient availability ([#579](https://github.com/misospace/courier/issues/579)) ([988298f](https://github.com/misospace/courier/commit/988298f6f1122a4d050ff731b402d911547f1e98))


### Chores

* **container:** update image golang (2669ec9 → 2920bb1) ([#621](https://github.com/misospace/courier/issues/621)) ([2661840](https://github.com/misospace/courier/commit/2661840db6ee9fa2cb9e71bc57fc281bd10446b8))
* **container:** update image golang (6f1acc8 → b760209) ([#611](https://github.com/misospace/courier/issues/611)) ([fcd7115](https://github.com/misospace/courier/commit/fcd71155d762b946dbe2846f4493b958b9114532))
* **container:** update image golang (9048d5e → 921f611) ([#591](https://github.com/misospace/courier/issues/591)) ([c1e25f6](https://github.com/misospace/courier/commit/c1e25f6400fdb1633e61dedc80426d5f12d41da3))
* **container:** update image golang (921f611 → 6f1acc8) ([#594](https://github.com/misospace/courier/issues/594)) ([2c6da5d](https://github.com/misospace/courier/commit/2c6da5d030e9dfe415224a53546d0c95f18cbe47))
* **container:** update image golang (b760209 → 2669ec9) ([#619](https://github.com/misospace/courier/issues/619)) ([6b20240](https://github.com/misospace/courier/commit/6b202406e04c15fe17854e30477700972713fd43))
* **container:** update image golang (f20d83a → 9048d5e) ([#581](https://github.com/misospace/courier/issues/581)) ([aaeb1b5](https://github.com/misospace/courier/commit/aaeb1b52552d217e9051e83876cadd344039c6b2))
* **container:** update image node (2814715 → 88e4807) ([#604](https://github.com/misospace/courier/issues/604)) ([d164961](https://github.com/misospace/courier/commit/d164961584060eb5b08eaca1c924105541544551))
* **deps:** lock file maintenance ([#574](https://github.com/misospace/courier/issues/574)) ([6442249](https://github.com/misospace/courier/commit/6442249fbf82018987a666a3c30742cc0b726508))


### Documentation

* design source-neutral ad-hoc contributions and fork-backed publication ([#603](https://github.com/misospace/courier/issues/603)) ([6eb1b39](https://github.com/misospace/courier/commit/6eb1b3908308adceea9983b340db1f3aaa069c5f))
* **design:** settle explicit retry contract for terminal runs ([#97](https://github.com/misospace/courier/issues/97)) ([#610](https://github.com/misospace/courier/issues/610)) ([c4e4aa9](https://github.com/misospace/courier/commit/c4e4aa90222c2ace13e57d8d809dc7623fae5949))

## [0.1.11](https://github.com/misospace/courier/compare/v0.1.10...v0.1.11) (2026-10-26)


### Features

* **chart:** expose native bootstrap laneProfiles values ([#546](https://github.com/misospace/courier/issues/546)) ([0f63491](https://github.com/misospace/courier/commit/0f63491dfc1b62c2907b3687832a725ac2ca2e26))
* **controller:** add CI park-and-resume lifecycle and persistence contracts ([#485](https://github.com/misospace/courier/issues/485)) ([7ca9c94](https://github.com/misospace/courier/commit/7ca9c94d0e500c056cdee7e500d5a8eaef88ed5d))
* **controller:** wire mechanical CI park and bounded same-run repair ([#504](https://github.com/misospace/courier/issues/504)) ([f710a64](https://github.com/misospace/courier/commit/f710a64571b6f08bd8f8174859eb7ca35fdc06ec))
* **dispatch:** adopt the CI park lifecycle for fenced PR-fix coordination ([4b403b4](https://github.com/misospace/courier/commit/4b403b41e4ceef0e1b63a7b7c73745215ec17565))
* **dispatch:** adopt the CI park lifecycle for fenced PR-fix coordination ([d4fdc37](https://github.com/misospace/courier/commit/d4fdc37e7ffb834afe0005419429d72f54d0d734))
* **github-release:** update release helm (3.18.6 → 3.22.1) ([#472](https://github.com/misospace/courier/issues/472)) ([bf81a3d](https://github.com/misospace/courier/commit/bf81a3df36f3fa543f0073978504bea90240fc31))
* **github-release:** update release helm (3.22.1 → 4.3.1) ([#473](https://github.com/misospace/courier/issues/473)) ([11c5cb3](https://github.com/misospace/courier/commit/11c5cb33bc36c1864b63a1b475753af3cd9ef310))
* **observability:** emit connector diagnostics and capability state ([#493](https://github.com/misospace/courier/issues/493)) ([cacdcdb](https://github.com/misospace/courier/commit/cacdcdb2ecffe28edc5e5bfc8b6e30653c469bcb))
* **observability:** export phase, execution, lifecycle and reaping signals ([#491](https://github.com/misospace/courier/issues/491)) ([3331216](https://github.com/misospace/courier/commit/33312167c8433cd832b6bde277d20c6475a7f2e7))
* **observability:** export source discovery and lane-capacity signals ([#492](https://github.com/misospace/courier/issues/492)) ([4129c25](https://github.com/misospace/courier/commit/4129c25974b501a5c700567c6973af74d963d267))
* **observability:** publish portable dashboard and alert-rule assets ([#503](https://github.com/misospace/courier/issues/503)) ([57474d8](https://github.com/misospace/courier/commit/57474d848bebba30c0167d6970a25ecb04b79890))
* **review:** adopt repository skills and v3.6.0 ([#457](https://github.com/misospace/courier/issues/457)) ([dfc10c6](https://github.com/misospace/courier/commit/dfc10c657555a6af54d049cd3628fb05e4553e53))
* **runtime:** execute the repository's approved development bootstrap after checkout ([#446](https://github.com/misospace/courier/issues/446)) ([e0b9cd3](https://github.com/misospace/courier/commit/e0b9cd3dba6bfc1fb727b813f5688a6b2c0f70a9))
* **runtime:** provision approved execution toolchains ([#505](https://github.com/misospace/courier/issues/505)) ([564c176](https://github.com/misospace/courier/commit/564c176b25e7700c43cf24f591d808a604856402))
* **source/dispatch:** release a ci-park deferral whose opportunity ends without a reservation ([#545](https://github.com/misospace/courier/issues/545)) ([828fb69](https://github.com/misospace/courier/commit/828fb695d6cedaf81b282445ea424b5b6fed94d1))


### Bug Fixes

* **api:** narrow lane resources vocabulary to requests and limits ([#403](https://github.com/misospace/courier/issues/403)) ([#452](https://github.com/misospace/courier/issues/452)) ([5dcf63e](https://github.com/misospace/courier/commit/5dcf63e68d05fab37b493c359498f5d7034d791c))
* **container:** update image golang (1.27.2 → 1.27.3) ([#478](https://github.com/misospace/courier/issues/478)) ([88afe20](https://github.com/misospace/courier/commit/88afe2080efd80d407be84967ad3024f55677a8e))
* **controller:** a pending park close no longer preempts a terminal run's lifecycle report and merge observation ([#557](https://github.com/misospace/courier/issues/557)) ([936900f](https://github.com/misospace/courier/commit/936900f2589208f4d65aab375e6028a92f1fbdbc))
* **controller:** bound stuck-terminating provisioning collision with NeedsHuman escalation ([d0a3b8c](https://github.com/misospace/courier/commit/d0a3b8c4dfa39c3769e7db1b8143803f08101897))
* **controller:** bound stuck-terminating provisioning collision with NeedsHuman escalation ([7ddd51c](https://github.com/misospace/courier/commit/7ddd51cd90f4d1edf05ea857fc3fbf1854c0261b))
* **controller:** claim rollback settles the granted repair fence ([#563](https://github.com/misospace/courier/issues/563)) ([97e25a9](https://github.com/misospace/courier/commit/97e25a9bb502f23c691bcf34bd51bda474f4b2c7))
* **controller:** close the abandon's attempt context and legacy key gaps ([97c1f05](https://github.com/misospace/courier/commit/97c1f05a9beb99a204e44a67aedf9ec4754f65c4))
* **controller:** count lane capacity through the uncached reader at admission and CI-repair reacquisition ([#558](https://github.com/misospace/courier/issues/558)) ([997e6cc](https://github.com/misospace/courier/commit/997e6cc18e93e8cc75864f5fa3558d5aa527b629))
* **controller:** make preflight probe names unique per node ([#451](https://github.com/misospace/courier/issues/451)) ([4315959](https://github.com/misospace/courier/commit/4315959da18ee575a7ed37b9e4fc41ff507b289f))
* **controller:** preserve CI repair through retryable rollback ([#570](https://github.com/misospace/courier/issues/570)) ([f3a5b5f](https://github.com/misospace/courier/commit/f3a5b5f30b25b73135b9c7813708c1c976a7ea8f))
* **controller:** preserve CI settlement reservation identity ([1547724](https://github.com/misospace/courier/commit/154772414c52d1201123300e46150b01616fa67e))
* **controller:** release a park's deferral when the park loop abandons the run ([6aedcfd](https://github.com/misospace/courier/commit/6aedcfd460f4ceaf1bee57ddd5a84d08993c6cc2))
* **controller:** release abandoned CI park deferrals ([eba1c09](https://github.com/misospace/courier/commit/eba1c099336602d1f1ac8c4158cf3014bb9f7841))
* **controller:** resolve moot CI settlement refusals ([#567](https://github.com/misospace/courier/issues/567)) ([5c6ac7a](https://github.com/misospace/courier/commit/5c6ac7a840bbd5153d454b7f6168cd2e8e8bb9b9))
* **controller:** settle a parked run's deferral on the merged Done path ([#555](https://github.com/misospace/courier/issues/555)) ([c649af6](https://github.com/misospace/courier/commit/c649af693460eb76b7d0db5bcd3a32017b6f0464))
* **controller:** settle a parked run's deferral on the merged Done path ([#555](https://github.com/misospace/courier/issues/555)) ([1ebca31](https://github.com/misospace/courier/commit/1ebca310609da122064e1a0a02ec41411904aa75))
* **controller:** terminalize probe-identity mismatch in secure preflight ([#467](https://github.com/misospace/courier/issues/467)) ([1eb97d3](https://github.com/misospace/courier/commit/1eb97d3db38e68d6120fc44275bbe3f865bf210d))
* **crd:** regenerate the coderrun schema for the settlement idempotency key ([caa7378](https://github.com/misospace/courier/commit/caa737851ca3b3e4b6971337cd04e33631b9552f))
* **deps:** update dependency astro (7.4.0 → 7.4.1) ([#470](https://github.com/misospace/courier/issues/470)) ([39eab81](https://github.com/misospace/courier/commit/39eab8117d891ea2d905ae84a130a862cc7eb3e2))
* **deps:** update dependency astro (7.4.1 → 7.4.2) ([#498](https://github.com/misospace/courier/issues/498)) ([b581015](https://github.com/misospace/courier/commit/b581015b885e5d62a2d0e32e048e3f0aa1f205ed))
* **deps:** update dependency go (1.27.2 → 1.27.3) ([#477](https://github.com/misospace/courier/issues/477)) ([53fbe1d](https://github.com/misospace/courier/commit/53fbe1d7ef7420b0894305aae4a4b769fe536d05))
* **dispatch:** carry issued repair reasons into coordinator goals ([#519](https://github.com/misospace/courier/issues/519)) ([778b6eb](https://github.com/misospace/courier/commit/778b6eb9a3540695eca114849c9750b460f9f4c3))
* **dispatch:** echo handout token on every followup-pr report ([#523](https://github.com/misospace/courier/issues/523)) ([2ea7504](https://github.com/misospace/courier/commit/2ea7504e51d6aa8df7500b96d4722777c60163f3))
* **evidence:** discard runGit's late bytes under an expired context ([#527](https://github.com/misospace/courier/issues/527)) ([68b156e](https://github.com/misospace/courier/commit/68b156e7d00d93f087d8039428bc0532730940b8))
* **evidence:** discard runGitCapped's late bytes under an expired context ([#468](https://github.com/misospace/courier/issues/468)) ([e894c28](https://github.com/misospace/courier/commit/e894c281f9f91ac06308814e8c90ce816524d95e))
* **executor:** digest-suffix overflowing run-label values ([#429](https://github.com/misospace/courier/issues/429)) ([#526](https://github.com/misospace/courier/issues/526)) ([b3db72a](https://github.com/misospace/courier/commit/b3db72a8b58b71e8790f46a63f321a5bbd5fa9a6))
* **executor:** re-check the live PR base at the legacy fix-pr handoff ([#524](https://github.com/misospace/courier/issues/524)) ([16ce7e8](https://github.com/misospace/courier/commit/16ce7e802f89b3807d675c595a54cab832b195e3))
* **executor:** reap adopted child processes ([#461](https://github.com/misospace/courier/issues/461)) ([e534d9c](https://github.com/misospace/courier/commit/e534d9cd73a20260d1048a922dbe8e222809fd0e))
* **executor:** stop publishing already-addressed comment for no-change runs ([#474](https://github.com/misospace/courier/issues/474)) ([#486](https://github.com/misospace/courier/issues/486)) ([a6b8114](https://github.com/misospace/courier/commit/a6b8114cfd613cd9b3df2bb6c5f961cb34ae2a38))
* **executor:** verify no-change claims against forge mergeability ([#515](https://github.com/misospace/courier/issues/515)) ([65cfc24](https://github.com/misospace/courier/commit/65cfc2455116072e14a2c2b23032a9dd5d98bc98))
* **harness:** bound one gateway completion's aggregate buffering ([#440](https://github.com/misospace/courier/issues/440)) ([#447](https://github.com/misospace/courier/issues/447)) ([3fd0d92](https://github.com/misospace/courier/commit/3fd0d92d21e33c6230e40097a5f4f5b9d7b0e168))
* **harness:** enforce artifact budgets before unpacking and inventory ([#441](https://github.com/misospace/courier/issues/441)) ([#481](https://github.com/misospace/courier/issues/481)) ([f032209](https://github.com/misospace/courier/commit/f0322093c60dd81594700e0cbe270979a810676b))
* **images:** pin the preflight probe image by digest ([#508](https://github.com/misospace/courier/issues/508)) ([f12168a](https://github.com/misospace/courier/commit/f12168aa4f8eccfe3b72f990e545545fa7ba29e7))
* **pods:** size preflight probe pods with a fixed small sizing, not the lane's ([#407](https://github.com/misospace/courier/issues/407)) ([31f0db8](https://github.com/misospace/courier/commit/31f0db88c6bdf707ec983060573ce65630890dfb))
* **protocol:** contain task trees beyond their process group ([#399](https://github.com/misospace/courier/issues/399)) ([#463](https://github.com/misospace/courier/issues/463)) ([f2c538f](https://github.com/misospace/courier/commit/f2c538f1cb12885df8d9bc67f746d49347501a8f))
* **protocol:** cross-compile the containment syscalls for windows ([#541](https://github.com/misospace/courier/issues/541)) ([775a0fa](https://github.com/misospace/courier/commit/775a0fa1ac7d368b24fddaa98b2a98c9e2fee4f8))
* **protocol:** fail the openat artifact walk closed on non-Linux ([82449a5](https://github.com/misospace/courier/commit/82449a50f875dfcbb0cfd46787c3be0a0794da6a))
* **protocol:** fail the openat artifact walk closed on non-Linux ([ab2bcc8](https://github.com/misospace/courier/commit/ab2bcc8a310ed4d87047967ee81f40f06fc4c2da)), closes [#479](https://github.com/misospace/courier/issues/479)
* **resources:** bound workspace observer memory ([#464](https://github.com/misospace/courier/issues/464)) ([3db4608](https://github.com/misospace/courier/commit/3db4608c45c837032a86549c577ac61916c7fe43))
* **review:** enable specialist Anthropic effort support ([#520](https://github.com/misospace/courier/issues/520)) ([249f266](https://github.com/misospace/courier/commit/249f26691aa60f3dd22fa8ac1806cd7c393c5276))
* **source/dispatch:** classify permanent Dispatch refusals (400/403) so terminal lifecycle reports settle ([#562](https://github.com/misospace/courier/issues/562)) ([b968a47](https://github.com/misospace/courier/commit/b968a474f27a0996e9f265dd13c3e3b75b67daa9))
* **source/dispatch:** latch CI-park lifecycle absence on bare 404s and degrade to the ordinary grading path ([#559](https://github.com/misospace/courier/issues/559)) ([21f6c1e](https://github.com/misospace/courier/commit/21f6c1efb8cee7fafd0ce7be315df7b6cfa35349))
* **source/dispatch:** latch CI-park lifecycle absence on bare 404s and degrade to the ordinary grading path ([#559](https://github.com/misospace/courier/issues/559)) ([7206204](https://github.com/misospace/courier/commit/720620472c511c793e3d7e4e0807321987257e23))
* **source:** ack PR-fix hand-outs at durable materialization ([#462](https://github.com/misospace/courier/issues/462)) ([55c637b](https://github.com/misospace/courier/commit/55c637b5c86478cd66e6f81041e5acd3c011d1a7))
* **topology:** hold pod names' first label to the 63-character bound ([#506](https://github.com/misospace/courier/issues/506)) ([9d291e4](https://github.com/misospace/courier/commit/9d291e4657f2169df7dd02951661b795cfdb6f8f))
* **topology:** trim truncated service names of trailing dots ([8267227](https://github.com/misospace/courier/commit/8267227060b798fdc6f9b4cda8e437ad42d8baaf))
* **topology:** trim truncated service names of trailing dots ([388c9c9](https://github.com/misospace/courier/commit/388c9c9def55ef5a763cca50432f3a7cedab3804))
* **verifier:** transient forge-read failure ends blocked_external, not NeedsHuman ([#459](https://github.com/misospace/courier/issues/459)) ([c1ebee3](https://github.com/misospace/courier/commit/c1ebee375d70a8d009a1300bdae8962fa5edd586))
* **workspace:** validate the live PR base for fix-pr runs ([#518](https://github.com/misospace/courier/issues/518)) ([1db24ab](https://github.com/misospace/courier/commit/1db24abbaa0936d4c7affacf84a7c02c8a6eb67a))


### Chores

* **container:** update image gcr.io/distroless/static (5167571 → d620678) ([#499](https://github.com/misospace/courier/issues/499)) ([2ef4767](https://github.com/misospace/courier/commit/2ef4767764b1886659a8d22e3f44b1122170bb32))
* **container:** update image gcr.io/distroless/static (d620678 → 18f4cc2) ([#535](https://github.com/misospace/courier/issues/535)) ([37b6604](https://github.com/misospace/courier/commit/37b66045f4644aae748b5e7a8ec8c7800f41d53f))
* **container:** update image gcr.io/distroless/static (e2e927e → 5167571) ([#480](https://github.com/misospace/courier/issues/480)) ([a5b2279](https://github.com/misospace/courier/commit/a5b2279fd377d3eae3fc680a68a32a3c11fe234a))
* **container:** update image golang (9578611 → f241642) ([#496](https://github.com/misospace/courier/issues/496)) ([512b4fe](https://github.com/misospace/courier/commit/512b4fee8eb1d910489bdb03d32bd84b139edea2))
* **container:** update image golang (f241642 → f20d83a) ([#500](https://github.com/misospace/courier/issues/500)) ([11e1505](https://github.com/misospace/courier/commit/11e1505dfbf7b9eda4ef2f6975034f7869c2c78f))
* **container:** update image node (8cabefb → 55fb5f5) ([#502](https://github.com/misospace/courier/issues/502)) ([fad24aa](https://github.com/misospace/courier/commit/fad24aad1b8eb1a32c52f41a848c560adf0f758f))
* **container:** update image node (a5f4366 → 8cabefb) ([#497](https://github.com/misospace/courier/issues/497)) ([4c7a44f](https://github.com/misospace/courier/commit/4c7a44fb914e949cab61274e2a7f459b492aceaf))
* **deps:** lock file maintenance ([#466](https://github.com/misospace/courier/issues/466)) ([854b4c9](https://github.com/misospace/courier/commit/854b4c9548f973d4f5c688e6c6809fd28ff8ed56))
* **dev:** add reproducible Go development bootstrap ([#436](https://github.com/misospace/courier/issues/436)) ([55a8308](https://github.com/misospace/courier/commit/55a8308654786701bef81b16c4964abb99535888))
* **dispatch:** give CI park lifecycle refusals a distinct identity-class census ([0ec3f3c](https://github.com/misospace/courier/commit/0ec3f3cf392aa7c7734c7745d5e7aab03e736341))
* **dispatch:** give CI park lifecycle refusals a distinct identity-class census ([6adc195](https://github.com/misospace/courier/commit/6adc1955840d936a31d4d24ba9563e23d5608135)), closes [#522](https://github.com/misospace/courier/issues/522)
* **envtest:** align envtest assets with k8s 1.37 and derive the CI cache key from the Makefile pin ([d2fa1b7](https://github.com/misospace/courier/commit/d2fa1b7de0f55ca9a5bdff957024cfd47fe2508e))
* **envtest:** align envtest assets with k8s 1.37 and derive the CI cache key from the Makefile pin ([68e75a6](https://github.com/misospace/courier/commit/68e75a687f7f5a628174d93c6cb35a8be74cc920))
* **executor:** state the write-once publish rule for sessionTap.tel ([#542](https://github.com/misospace/courier/issues/542)) ([92fd524](https://github.com/misospace/courier/commit/92fd5245f8c402d60b5bc18fc83da7e16eefb641))


### Documentation

* **broker:** align criterion-evidence header doc comments with derived grammar ([295d064](https://github.com/misospace/courier/commit/295d0643410fb6967e791c481a39d01d9fc54d75))
* **broker:** align criterion-evidence header doc comments with derived grammar ([f702f53](https://github.com/misospace/courier/commit/f702f53451b5e128f116d613bc642ab22ffdcc6f))
* **harness:** reconcile the criterion-evidence block with the derived grammar ([#425](https://github.com/misospace/courier/issues/425)) ([7756c1a](https://github.com/misospace/courier/commit/7756c1a1ca2c0006a0284b9752ab434d3831d01f))
* **harness:** reconcile the criterion-evidence block with the derived grammar ([#425](https://github.com/misospace/courier/issues/425)) ([6fb2947](https://github.com/misospace/courier/commit/6fb2947be1c4969cebbf19a86603fad7733a0f80))
* **harness:** the section renders the grammar, it does not restate it ([27eaafc](https://github.com/misospace/courier/commit/27eaafc572f01d16bd4e91fc6eca6a09fb210620))
* stop treating DESIGN.md as a PR decision log ([#465](https://github.com/misospace/courier/issues/465)) ([01d3a94](https://github.com/misospace/courier/commit/01d3a94784d087c5b49ffa7338082a1ed836475d))


### Refactors

* **executor:** extract evidence capture, session tap, and cited-path checks out of main.go ([706164c](https://github.com/misospace/courier/commit/706164cfe2b3e526a333bf8cd4bf2521a3a39b95))
* **executor:** extract evidence capture, session tap, and cited-path checks out of main.go ([a05fcee](https://github.com/misospace/courier/commit/a05fcee62634b890c272ba5910f3c68f948fb67f))
* **executor:** move revParse to exhaustion.go beside its only caller ([754a5f4](https://github.com/misospace/courier/commit/754a5f47a607e32c5890267a56adcf5872dee7f1))

## [0.1.10](https://github.com/misospace/courier/compare/v0.1.9...v0.1.10) (2026-10-18)


### Features

* **api:** per-role context budget schema on LaneProfileSpec ([#295](https://github.com/misospace/courier/issues/295)) ([#437](https://github.com/misospace/courier/issues/437)) ([83fa651](https://github.com/misospace/courier/commit/83fa65194dbc43790fabd7453ed5a915036c65ed))
* bounded identity-checked pre-handoff PR observation in legacy executor ([#247](https://github.com/misospace/courier/issues/247)) ([#342](https://github.com/misospace/courier/issues/342)) ([a587305](https://github.com/misospace/courier/commit/a587305799574f786a5bfc7eb2a0d321a7997a6b))
* **controller:** terminalize closed-unmerged PRs after AwaitingReview ([#277](https://github.com/misospace/courier/issues/277)) ([#361](https://github.com/misospace/courier/issues/361)) ([e5b2186](https://github.com/misospace/courier/commit/e5b21867776c428340e8b27344e3860f9462204a))
* **deps:** update dependency astro (5.18.2 → 7.4.0) ([#366](https://github.com/misospace/courier/issues/366)) ([bf57efb](https://github.com/misospace/courier/commit/bf57efb00cd9fab9bf37b5fe0555f9e2b961f5cb))
* **executor:** check cited evidence paths against the published head ([#367](https://github.com/misospace/courier/issues/367)) ([#422](https://github.com/misospace/courier/issues/422)) ([a377976](https://github.com/misospace/courier/commit/a377976504b17b6dd52806b5fa7473ba24366abe))
* **executor:** enforce the criterion-evidence contract in the legacy OpenCode handoff ([#348](https://github.com/misospace/courier/issues/348)) ([bad09d1](https://github.com/misospace/courier/commit/bad09d177e58c69996335742b70a10337efb9bd4))
* **harness:** probe gateway model input limit and gate context budgets ([#296](https://github.com/misospace/courier/issues/296)) ([#382](https://github.com/misospace/courier/issues/382)) ([0a1364f](https://github.com/misospace/courier/commit/0a1364fa7e50f82df98264b63171eedfe87829cd))
* **harness:** step-boundary compaction with digest preservation and session.compacted telemetry ([#381](https://github.com/misospace/courier/issues/381)) ([542c38a](https://github.com/misospace/courier/commit/542c38a60cd8794af27692fb0b7b69efed97a181))
* **pods:** size every Courier pod from the lane's resources ([#356](https://github.com/misospace/courier/issues/356)) ([#401](https://github.com/misospace/courier/issues/401)) ([86f1c26](https://github.com/misospace/courier/commit/86f1c26890c50c92e0472d43324db252215acf6c))


### Bug Fixes

* **broker:** bound the API server's inbound reads and idle connections ([#357](https://github.com/misospace/courier/issues/357)) ([#433](https://github.com/misospace/courier/issues/433)) ([00981e8](https://github.com/misospace/courier/commit/00981e813d072a593948205ca7c975e86102600b))
* **capacity:** adopt rotated claim tokens so settlement fences against the live lease ([#396](https://github.com/misospace/courier/issues/396)) ([ae5c801](https://github.com/misospace/courier/commit/ae5c80140dd916b7b24dbf1bae4ecc7d35318728))
* **controller:** never map a terminating coordinator's exit to a run outcome ([#427](https://github.com/misospace/courier/issues/427)) ([0efe3ba](https://github.com/misospace/courier/commit/0efe3ba624c82935fe131063e7a13ee6546a01a3))
* **controller:** require CoordinatorLauncher.APIReader at startup ([#430](https://github.com/misospace/courier/issues/430)) ([1b7d48f](https://github.com/misospace/courier/commit/1b7d48f06045e706effbaeb66984df17f7b25b18))
* **dispatch:** replay adopted capacity settlements by run-scoped settlement identity ([#442](https://github.com/misospace/courier/issues/442)) ([337b481](https://github.com/misospace/courier/commit/337b48162b2dee5a415523097625342615da43ae))
* **evidence:** bound runGit's copy wait and keep bytes from a succeeded child ([#421](https://github.com/misospace/courier/issues/421)) ([973e5a8](https://github.com/misospace/courier/commit/973e5a86afee128795983ad21d53fe43cba10e46)), closes [#408](https://github.com/misospace/courier/issues/408)
* **evidence:** bound runGitCapped's stdout read by the capture deadline ([#410](https://github.com/misospace/courier/issues/410)) ([c10e79f](https://github.com/misospace/courier/commit/c10e79fdbe51adef6bba914136dc0217972d0485))
* **evidence:** bound the capture read phase by the operation deadline ([#393](https://github.com/misospace/courier/issues/393)) ([2229179](https://github.com/misospace/courier/commit/2229179191f0ff8dae07468eff59d4a67ec06480))
* **evidence:** bound the classified-regular capture read against a non-regular swap ([#389](https://github.com/misospace/courier/issues/389)) ([9a3ff26](https://github.com/misospace/courier/commit/9a3ff26f0d389ec4a98a3c5cdd0378a90b97a2a6))
* **evidence:** keep a capped git read's failure on the over-limit branch ([#412](https://github.com/misospace/courier/issues/412)) ([#444](https://github.com/misospace/courier/issues/444)) ([b13b84d](https://github.com/misospace/courier/commit/b13b84d07a300b3aeddfb710c7400121f7a5d38e))
* **executor:** fail closed when classifyUndeclared's branch re-read errors ([#432](https://github.com/misospace/courier/issues/432)) ([2a23dea](https://github.com/misospace/courier/commit/2a23deaf45545683c9eb550c6f9353f398beb994))
* **executor:** route the adoption guard through the identity-checked forge read ([#363](https://github.com/misospace/courier/issues/363)) ([9ec0de4](https://github.com/misospace/courier/commit/9ec0de49de820f38d99cee2f99b60cd8b3532b31))
* **github:** bound default GitHub API request duration ([#362](https://github.com/misospace/courier/issues/362)) ([b48585a](https://github.com/misospace/courier/commit/b48585a064592bbcb9ddc8adec13822850bcf85b)), closes [#341](https://github.com/misospace/courier/issues/341)
* **harness:** classify a broker 401 by the request's own credential freshness ([#419](https://github.com/misospace/courier/issues/419)) ([8c7236e](https://github.com/misospace/courier/commit/8c7236e86ca09ccea542b68b3993ae48214b9815))
* **harness:** read the projected broker token per request ([#383](https://github.com/misospace/courier/issues/383)) ([#405](https://github.com/misospace/courier/issues/405)) ([0d500de](https://github.com/misospace/courier/commit/0d500de7df763fd159b487f9aa8a1fcf97939181))
* **harness:** reuse one bounded TLS transport per broker client ([#404](https://github.com/misospace/courier/issues/404)) ([a2a4ad0](https://github.com/misospace/courier/commit/a2a4ad083f7b11bbf97c48b2f9362a8b9f3ef36e))
* **image:** build git v2.47.3 from pinned GitHub release tarball ([#349](https://github.com/misospace/courier/issues/349)) ([cc8854a](https://github.com/misospace/courier/commit/cc8854ae204819c20f426c02f6ba6abdc29a6353))
* **image:** ship process-tree utilities in coordinator image ([#353](https://github.com/misospace/courier/issues/353)) ([e1de758](https://github.com/misospace/courier/commit/e1de758e03405cff2fa29910d906fca7bf41aec9))
* **metrics-mcp:** bound inbound HTTP reads and graceful shutdown ([#352](https://github.com/misospace/courier/issues/352)) ([e8daa33](https://github.com/misospace/courier/commit/e8daa33595acca853f11b3091b574425979a22b2))
* **metrics:** redact query strings and fragments from URL diagnostics ([#350](https://github.com/misospace/courier/issues/350)) ([f8f428b](https://github.com/misospace/courier/commit/f8f428b7db0bebf312fe5cb6691a314478c1e173))
* **observer:** read the Verifying observation's head listing through the identity-checked forge capability ([#386](https://github.com/misospace/courier/issues/386)) ([60a4fe3](https://github.com/misospace/courier/commit/60a4fe301e8cb50944a6810837676d98de5f12d0))
* **protocol:** enforce workspace confinement and read bounds for worker artifacts ([#435](https://github.com/misospace/courier/issues/435)) ([998126e](https://github.com/misospace/courier/commit/998126ecf7a6a02516a54b0fced4468ddd6b0668))
* **protocol:** stop requiring a running status on the dispatch response ([#380](https://github.com/misospace/courier/issues/380)) ([6e549f2](https://github.com/misospace/courier/commit/6e549f2056eb89f537787ed061f8c0177ea2643e))
* **recovery:** hand off legacy OpenCode quota exhaustion across workers ([#327](https://github.com/misospace/courier/issues/327)) ([#368](https://github.com/misospace/courier/issues/368)) ([a7ab601](https://github.com/misospace/courier/commit/a7ab601eb7d0794cbff5a773232acac0de93bc35))
* **secure:** decide the relaunch's delete and ceiling from the live world ([#370](https://github.com/misospace/courier/issues/370)) ([#423](https://github.com/misospace/courier/issues/423)) ([ccffb24](https://github.com/misospace/courier/commit/ccffb2413d593b001ab30a11f148a1a6dfc6933e))
* **secure:** re-confirm the secure control delete and keep the ceiling evidence ([#371](https://github.com/misospace/courier/issues/371)) ([b371e5f](https://github.com/misospace/courier/commit/b371e5f75187d3fce6a809c6c6d1d0643d6f558c))
* **secure:** route admission and broker clients through the registered forge endpoint ([#378](https://github.com/misospace/courier/issues/378)) ([f67b06e](https://github.com/misospace/courier/commit/f67b06ee5c04d5b30a584a29e9c1c8bd043c2521))
* **secure:** route operator observation reads through the registered forge endpoint ([#377](https://github.com/misospace/courier/issues/377)) ([#387](https://github.com/misospace/courier/issues/387)) ([115dee2](https://github.com/misospace/courier/commit/115dee2d3ab1948dcd035b55bd04982dd8bd7f10))
* **topology:** bound the run label value for long CoderRun names ([#428](https://github.com/misospace/courier/issues/428)) ([b00222e](https://github.com/misospace/courier/commit/b00222e26fba47d143ae79e73acb3b3c0ac5c9c4))
* **topology:** render DNS-label-safe Service names for long or dotted run names ([#418](https://github.com/misospace/courier/issues/418)) ([baff598](https://github.com/misospace/courier/commit/baff59895cf6962ee54b95a350094ba9e304921c)), closes [#385](https://github.com/misospace/courier/issues/385)
* **worker:** prove native task-tree termination before acknowledging cancellation ([#400](https://github.com/misospace/courier/issues/400)) ([84168c2](https://github.com/misospace/courier/commit/84168c2dfd0b7083c31b0bafe94ece2564f402f0))
* **worker:** release consumed native worker result payloads ([#373](https://github.com/misospace/courier/issues/373)) ([#426](https://github.com/misospace/courier/issues/426)) ([9e79f65](https://github.com/misospace/courier/commit/9e79f658a18a7d7fa5fa24267d205d02df53b926))


### Chores

* **ci:** disable experimental requirement tracing ([#374](https://github.com/misospace/courier/issues/374)) ([3f3e5e1](https://github.com/misospace/courier/commit/3f3e5e1bb7f3b5b5270a27d60859c9cdf72ea3ac))
* **container:** update image node (d6aa754 → a5f4366) ([#339](https://github.com/misospace/courier/issues/339)) ([36d43b7](https://github.com/misospace/courier/commit/36d43b7f95e953eff5f335174a248bb03bd628da))


### Documentation

* correct README Status — native harness and liveness recovery have shipped ([#397](https://github.com/misospace/courier/issues/397)) ([32ad040](https://github.com/misospace/courier/commit/32ad040e93865f4b459535f413bc67f6cf057bfa))
* correct SECURITY.md supported versions now releases are published ([#434](https://github.com/misospace/courier/issues/434)) ([0eaa3fb](https://github.com/misospace/courier/commit/0eaa3fbbc012e975f0aceb4d421e6815904012ea))
* correct site Status copy — native harness and liveness recovery shipped ([#398](https://github.com/misospace/courier/issues/398)) ([#431](https://github.com/misospace/courier/issues/431)) ([8c23297](https://github.com/misospace/courier/commit/8c23297734beb3c5aa7709219a0576431bc66688))
* **design:** record the [#336](https://github.com/misospace/courier/issues/336)/[#389](https://github.com/misospace/courier/issues/389) non-regular descriptor guard in the Decisions log ([#445](https://github.com/misospace/courier/issues/445)) ([2601899](https://github.com/misospace/courier/commit/2601899e0a40191cc1961edeacfacb6d65fed626))
* invariant 5 bounds termination by the deadline plus the child-kill waits ([#443](https://github.com/misospace/courier/issues/443)) ([8e33383](https://github.com/misospace/courier/commit/8e3338383219e2feee57900329c42a8fa0662c2c))
* name retention flag defaults and the phase-only reap metric labels ([#438](https://github.com/misospace/courier/issues/438)) ([4c87395](https://github.com/misospace/courier/commit/4c87395e49f0701d24ce44499f9e25631a74661a))


### Refactors

* **broker:** derive the taught criterion-evidence grammar from the validator ([#375](https://github.com/misospace/courier/issues/375)) ([#424](https://github.com/misospace/courier/issues/424)) ([01e85a5](https://github.com/misospace/courier/commit/01e85a5ec4ba93113d0051218851c45d06450f21))

## [0.1.9](https://github.com/misospace/courier/compare/v0.1.8...v0.1.9) (2026-10-14)


### Features

* **admission:** pin the trusted acceptance-criterion set at run admission ([#316](https://github.com/misospace/courier/issues/316)) ([5aaaffa](https://github.com/misospace/courier/commit/5aaaffac7a8a19a9ba924fd9effb28a4bec78a85))
* **broker:** enforce the criterion-evidence contract at native PR publication ([#333](https://github.com/misospace/courier/issues/333)) ([a7b5bab](https://github.com/misospace/courier/commit/a7b5babd79de0e5c82c4c7797a6782c325efe827))
* **controller:** finish AwaitingReview runs when the linked PR merges ([#250](https://github.com/misospace/courier/issues/250)) ([#314](https://github.com/misospace/courier/issues/314)) ([5fd3a69](https://github.com/misospace/courier/commit/5fd3a69eae6ee793df4e991f9f95837e47325b77))
* **controller:** reap terminal Failed and NeedsHuman CoderRuns ([#287](https://github.com/misospace/courier/issues/287)) ([#319](https://github.com/misospace/courier/issues/319)) ([2d42cc4](https://github.com/misospace/courier/commit/2d42cc49fa80f584dc1723c7f539246b44e61166))
* operator soft-stop control request with a checkpoint-safe stop boundary ([#315](https://github.com/misospace/courier/issues/315)) ([0419240](https://github.com/misospace/courier/commit/04192406c8bb74601d6e725f3be9a7531ba23058))
* **recovery:** add source-neutral capacity handoff and cooldown ([#327](https://github.com/misospace/courier/issues/327)) ([#328](https://github.com/misospace/courier/issues/328)) ([2b98272](https://github.com/misospace/courier/commit/2b98272ee5cf38f3fd8cfa413ae45d62c99d6756))


### Bug Fixes

* **control:** fit COURIER_TERMINATION line to the 4 KiB pod budget ([#210](https://github.com/misospace/courier/issues/210)) ([#298](https://github.com/misospace/courier/issues/298)) ([687750e](https://github.com/misospace/courier/commit/687750ec2e489a4ceb137a3f744e2f3fe0dafc44))
* **controller:** bound secure relaunches by the configured ceiling ([#216](https://github.com/misospace/courier/issues/216)) ([#305](https://github.com/misospace/courier/issues/305)) ([1b0629e](https://github.com/misospace/courier/commit/1b0629ebea7ad0a27a91731e7210af6950c79180))
* **controller:** scope the wedge reap to the coordinator, fence the secure round ([#309](https://github.com/misospace/courier/issues/309)) ([964d3e5](https://github.com/misospace/courier/commit/964d3e57463a926215d86ed807ac37dad42d0833))
* **controller:** stay Claimed when a relaunch collides with a terminating pod ([#217](https://github.com/misospace/courier/issues/217)) ([#294](https://github.com/misospace/courier/issues/294)) ([cae30f4](https://github.com/misospace/courier/commit/cae30f40e330a0b43ac2ad471fcd1eae3450c787))
* **discovery:** truncate PR-fix exclusions to contract max instead of failing closed ([#293](https://github.com/misospace/courier/issues/293)) ([48b0ef7](https://github.com/misospace/courier/commit/48b0ef7ebea141a47b001df47f52326fb4980cc6))
* **dispatch:** map stale-mark generation mismatch to ErrSuperseded ([#304](https://github.com/misospace/courier/issues/304)) ([d6afbee](https://github.com/misospace/courier/commit/d6afbee3f27bd01d404797cb353ae4c0883a18b1))
* **evidence:** close the Lstat→Open symlink-swap race with an O_NOFOLLOW read ([#338](https://github.com/misospace/courier/issues/338)) ([0be7d4f](https://github.com/misospace/courier/commit/0be7d4f259ce3eceec0010aabe19e090cf90c631)), closes [#237](https://github.com/misospace/courier/issues/237)
* **executor:** make git commit identity configurable ([#321](https://github.com/misospace/courier/issues/321)) ([87e081f](https://github.com/misospace/courier/commit/87e081f2a26b50c2e4d69c0a079f95e5f3d94617))
* **harness:** treat model-provider allowance exhaustion as recoverable ([#322](https://github.com/misospace/courier/issues/322)) ([948c629](https://github.com/misospace/courier/commit/948c629b20166e05b78bdffb5f253598ce67d328))
* **operator:** bound startup credential Secret reads with a context deadline ([#311](https://github.com/misospace/courier/issues/311)) ([abb4c5c](https://github.com/misospace/courier/commit/abb4c5cbeb833fc6581dd5bbd3be88b94d0a9493)), closes [#229](https://github.com/misospace/courier/issues/229)


### Chores

* **container:** update image golang (007a316 → 8201530) ([#329](https://github.com/misospace/courier/issues/329)) ([c962f3c](https://github.com/misospace/courier/commit/c962f3cd7dd6b80aa567e5540a8cc8f34f049791))
* **container:** update image golang (5bc7f57 → 007a316) ([#310](https://github.com/misospace/courier/issues/310)) ([c3a9bc9](https://github.com/misospace/courier/commit/c3a9bc977fa535348644bea7571db09ec49e4ace))
* **control:** make gate failures nil-safe and reason truncation rune-safe ([#307](https://github.com/misospace/courier/issues/307)) ([7653499](https://github.com/misospace/courier/commit/7653499ec9fe50618485fd2f631706cd53cd6b13)), closes [#219](https://github.com/misospace/courier/issues/219)
* **deps:** tidy go.sum and gate the static job on tidy drift ([#308](https://github.com/misospace/courier/issues/308)) ([a1d1e7c](https://github.com/misospace/courier/commit/a1d1e7cd82cf55f6d2f5dfb45d97de9ff9d6c18e))


### Documentation

* **harness:** settle context budgets and step-boundary compaction ([#243](https://github.com/misospace/courier/issues/243)) ([#299](https://github.com/misospace/courier/issues/299)) ([764205f](https://github.com/misospace/courier/commit/764205f8e189f3763c2a35413fbbe59aa055d81a))
* **harness:** settle the acceptance-criterion evidence contract ([#249](https://github.com/misospace/courier/issues/249)) ([#283](https://github.com/misospace/courier/issues/283)) ([2d1ff01](https://github.com/misospace/courier/commit/2d1ff01642b532c2f96c07e1d9b5acb1ff501dc2))
* match the failure-evidence runbook to shipped behavior ([#300](https://github.com/misospace/courier/issues/300)) ([bc81b02](https://github.com/misospace/courier/commit/bc81b027096d8f649af8911cbaf7fa8060d0030f))
* settle terminal retention and closed-unmerged lifecycle design ([#251](https://github.com/misospace/courier/issues/251)) ([#278](https://github.com/misospace/courier/issues/278)) ([ab00098](https://github.com/misospace/courier/commit/ab000982692c7b6388733b456c1ed26bfc936bcd))
* specify connector diagnostics without core coupling ([#291](https://github.com/misospace/courier/issues/291)) ([#302](https://github.com/misospace/courier/issues/302)) ([77faa33](https://github.com/misospace/courier/commit/77faa332f323fb57366d5cdac749524b4a15dd98))
* specify portable dashboard and alert views ([#292](https://github.com/misospace/courier/issues/292)) ([#301](https://github.com/misospace/courier/issues/301)) ([d6ad3c8](https://github.com/misospace/courier/commit/d6ad3c8e94e13d5d3ae0941bddd233d5efdad4d8))
* specify source-agnostic telemetry and health semantics ([#290](https://github.com/misospace/courier/issues/290)) ([#303](https://github.com/misospace/courier/issues/303)) ([3804878](https://github.com/misospace/courier/commit/38048782cd5d371588d942621ae00e33f8b09216))
* state the git &gt;= 2.41 minimum required by harness artifact validation ([#235](https://github.com/misospace/courier/issues/235)) ([#313](https://github.com/misospace/courier/issues/313)) ([93bbf4b](https://github.com/misospace/courier/commit/93bbf4bb486bf4b1292d36c9c0175b9e9570e718))

## [0.1.8](https://github.com/misospace/courier/compare/v0.1.7...v0.1.8) (2026-10-12)


### Features

* **controller:** retain terminal runtime diagnostics after pod cleanup ([#272](https://github.com/misospace/courier/issues/272)) ([#275](https://github.com/misospace/courier/issues/275)) ([59f9d42](https://github.com/misospace/courier/commit/59f9d4227023616e26d3746c9ebc043c5e0d43d4))
* **executor:** capture failure evidence at gated moments ([#257](https://github.com/misospace/courier/issues/257)) ([2e379ae](https://github.com/misospace/courier/commit/2e379ae7aa2cbb95eb0c50241271870427fa7326)), closes [#198](https://github.com/misospace/courier/issues/198)
* **helm:** update chart common (5.2.1 → 5.3.0) ([#266](https://github.com/misospace/courier/issues/266)) ([1c6a83c](https://github.com/misospace/courier/commit/1c6a83c31cc9a4a8cf8e6cd43c20439c8b7c3ba2))
* **operator:** evidence intake listener, run-owned persistence, and RBAC ([#259](https://github.com/misospace/courier/issues/259)) ([7a16947](https://github.com/misospace/courier/commit/7a169477521bc293b739406e9ba4a2e0a3622867))


### Bug Fixes

* **ci:** include harness validation in images gate ([#214](https://github.com/misospace/courier/issues/214)) ([#279](https://github.com/misospace/courier/issues/279)) ([6662ec8](https://github.com/misospace/courier/commit/6662ec8c74ed284bab2db2097553e55432d8665a))
* **controller:** hand off red resolve-issue PR checks ([#246](https://github.com/misospace/courier/issues/246)) ([#258](https://github.com/misospace/courier/issues/258)) ([737e8ca](https://github.com/misospace/courier/commit/737e8ca9fd7f1d608f236f308b1ef88e592e5287))
* **controller:** stop misclassifying terminal outcomes ([#267](https://github.com/misospace/courier/issues/267)) ([#271](https://github.com/misospace/courier/issues/271)) ([a3254aa](https://github.com/misospace/courier/commit/a3254aa58f5aa0ecb54a8b8a62516c260464ecbf))
* **deps:** update module github.com/prometheus/common (v0.72.0 → v0.72.1) ([#268](https://github.com/misospace/courier/issues/268)) ([6db4cd8](https://github.com/misospace/courier/commit/6db4cd8247da240530fc5d52f1fbf3712202e8f6))
* **dispatch:** skip consumed PR-fix generations during discovery ([#273](https://github.com/misospace/courier/issues/273)) ([dee8755](https://github.com/misospace/courier/commit/dee8755c146ea7580e430af542bdea68a24ce9a5))
* **operator:** fence worker and broker on control-pod loss detection ([#265](https://github.com/misospace/courier/issues/265)) ([ba7d0c6](https://github.com/misospace/courier/commit/ba7d0c62d861299891562283fda675dbb6ece685))
* **pr-authoring:** require authoritative source-issue linkage before handoff ([#262](https://github.com/misospace/courier/issues/262)) ([f1b263f](https://github.com/misospace/courier/commit/f1b263f498133e36886044dd66ede71061d3eb23))

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

# Changelog

## [0.3.0](https://github.com/prosody-events/gabion/compare/v0.2.0...v0.3.0) (2026-05-28)


### Features

* accurate Retry-After / X-RateLimit-Reset / X-RateLimit-Remaining ([#19](https://github.com/prosody-events/gabion/issues/19)) ([60f3ca3](https://github.com/prosody-events/gabion/commit/60f3ca3aed8b2029c6ecfc18ca026d3418cc2e20))
* emit X-RateLimit-* headers on allowed responses ([#22](https://github.com/prosody-events/gabion/issues/22)) ([101af34](https://github.com/prosody-events/gabion/commit/101af34e3d826701a4516932e8e943ae2f14e97a))


### Bug fixes

* **sim:** one visualizer step is one gossip round ([#27](https://github.com/prosody-events/gabion/issues/27)) ([4587e10](https://github.com/prosody-events/gabion/commit/4587e106bded354aa702e427c56ee5f8b2bc074d))


### Build / CI

* **docker:** back buildx cache with GHCR registry, not the Actions cache ([#23](https://github.com/prosody-events/gabion/issues/23)) ([518244a](https://github.com/prosody-events/gabion/commit/518244a66d8c1644e6fd9da166fe68f141d65b85))
* **docker:** re-test openresty PR build; share the canonical cook cache ([#25](https://github.com/prosody-events/gabion/issues/25)) ([a915cd8](https://github.com/prosody-events/gabion/commit/a915cd8495b173ba81ce20944b42b965ee724606))
* prebuilt nextest, miri sysroot cache, wasm dedup, Playwright cache, release-gated attestation ([#24](https://github.com/prosody-events/gabion/issues/24)) ([551607e](https://github.com/prosody-events/gabion/commit/551607e585932871309a2065583e8703c5bb2d0b))
* share docker's warm cook cache with the nginx + k8s smokes ([#26](https://github.com/prosody-events/gabion/issues/26)) ([0dedddc](https://github.com/prosody-events/gabion/commit/0dedddc2a8d0ed302e155d8be8011707ea74e995))

## [0.2.0](https://github.com/prosody-events/gabion/compare/v0.1.0...v0.2.0) (2026-05-27)


### Features

* **ci:** add GitHub Actions, GHCR publishing, Pages deploy, and release-please ([#1](https://github.com/prosody-events/gabion/issues/1)) ([bc2674d](https://github.com/prosody-events/gabion/commit/bc2674dcef9b0d41eec68bf6f213cc3371889a51))
* **gabion-wasm:** convergence dashboard (uPlot fan + disagreement + headline) ([e6b09b2](https://github.com/prosody-events/gabion/commit/e6b09b287d7d818d1e6d05f23444273697edb1db))
* **gabion-wasm:** gossip visualizer wasm bridge + web skeleton ([e00c267](https://github.com/prosody-events/gabion/commit/e00c267acac02d1b6a62a173a2fa592b46a15886))
* **gabion-wasm:** PixiJS stage with cell arcs, light-beams, convergence pulse ([ff9a8e8](https://github.com/prosody-events/gabion/commit/ff9a8e8b19d5eb9fb1a9fcd60fe5cbe2cec02536))
* **gossip-bench:** simulator-driven evaluation harness with paper-grounded metrics ([a88a6a8](https://github.com/prosody-events/gabion/commit/a88a6a860a57ddcf3cc36a2da2f2509431ebbc52))
* **gossip:** threshold-triggered anti-entropy + operator-tunable knobs ([0833ef9](https://github.com/prosody-events/gabion/commit/0833ef91603aa49b070611b40f56aac6fb52ea70))
* **headers:** align rate-limit headers with Envoy / GitHub conventions ([f676977](https://github.com/prosody-events/gabion/commit/f676977e78e924637a4fc08799814aef347f5b89))
* implement nginx shared rate limits ([0a4f2b7](https://github.com/prosody-events/gabion/commit/0a4f2b7916542a1d5988b71bfa8df04af875c6ed))
* lower gossip defaults to fanout 3 / 500 ms tick (project-wide) ([e255970](https://github.com/prosody-events/gabion/commit/e2559708f382f042b7cbc19a490be1e2ca0eee1c))
* **nginx,server:** DSL audit — parser fixes, validation, fixed-window default ([1d64cb0](https://github.com/prosody-events/gabion/commit/1d64cb022ae8f977d2c736dcd0da2a05f9e7c3a9))
* **nginx:** first-class ASN / UA / IP-range limits + DSL cleanup ([3540e15](https://github.com/prosody-events/gabion/commit/3540e15c4c0c87aafde13fd74c5cfc2dd19376c2))
* **nginx:** rewrite adapter on gossip + rules + SHM, audited unsafe + miri ([3f2267c](https://github.com/prosody-events/gabion/commit/3f2267c475e292ad76ac023859447e1cb09eb440))
* **nginx:** route module logging through a tracing subscriber ([fd5c1c3](https://github.com/prosody-events/gabion/commit/fd5c1c3b19149d1c4ccc633521a0a52ada388f53))
* production defaults module, distributed k8s test, gossip alignment ([c3f4b5e](https://github.com/prosody-events/gabion/commit/c3f4b5e83e8666a95b05556d735e84c3535290cb))
* **rules:** restore explicit `window=` specifier; scale limit from rate ([b16d214](https://github.com/prosody-events/gabion/commit/b16d2146f3f74c5645500074d4d1b964ed36ef30))
* **server:** explicit env-var bindings; verify rate-limit headers in k8s test ([d645ec3](https://github.com/prosody-events/gabion/commit/d645ec3b3def79cfcda7f79b8e5a22ca13190ece))
* **viz:** click a node to inject a burst (Phase 6a) ([8b7d651](https://github.com/prosody-events/gabion/commit/8b7d6516b9ae4a4fe43d79e1c74f91ae10c89635))
* **viz:** scenario presets (Phase 6b-ii) ([ba1fc46](https://github.com/prosody-events/gabion/commit/ba1fc465ec9cbc6aa744a341d388e51c08058d00))
* **viz:** three-pane chassis + accessible send control (Phase 6b-i) ([dcf7943](https://github.com/prosody-events/gabion/commit/dcf794399427b191cacc474bf392820eaa80da6c))
* **wasm-viz:** click-to-select + node inspector in the right rail ([45ce050](https://github.com/prosody-events/gabion/commit/45ce050ab9b153874ef83480d3de4dc05292f944))
* **wasm-viz:** live node join/leave UX + GSAP animation (Phase 6c) ([6ea09f3](https://github.com/prosody-events/gabion/commit/6ea09f36c0e1444ce50fd076a1abeef3a701003e))
* **wasm-viz:** polish gossip packets — soft beams, no disc strobe ([39ddd13](https://github.com/prosody-events/gabion/commit/39ddd133bd71ea890c3822be7fc536ca05651dc4))
* **wasm-viz:** polish the light stage (pulse, dot-mode, reject band) ([83b3d17](https://github.com/prosody-events/gabion/commit/83b3d17a32726e0edede1d57459ceceb8685c2f6))
* **wasm-viz:** polish the live add/remove cluster controls ([283ff75](https://github.com/prosody-events/gabion/commit/283ff756e4f20193e40f1154c9e26d9773c6487f))
* **wasm-viz:** rebuild-knob sliders (fanout, error budget, packet loss) ([67590cf](https://github.com/prosody-events/gabion/commit/67590cf8c2740d6a97584b90b313989c56d2b72c))
* **wasm-viz:** redesigned node-detail panel (cadence, I/O, storage, peers, identity) ([6b86b26](https://github.com/prosody-events/gabion/commit/6b86b26bcbe776d97336a46419f3aa4919d71d92))
* **wasm-viz:** rule + gossip-interval controls in the rail ([9053e2f](https://github.com/prosody-events/gabion/commit/9053e2f3b58240ab3694697faca1eac835cda1e8))
* **wasm-viz:** show time-to-expiry on the bucket strip ([85861ab](https://github.com/prosody-events/gabion/commit/85861ab88d682331b1da482f334d19048733dc24))
* **wasm-viz:** stable node identity + live join/leave engine (Phase 6c-i) ([fa57486](https://github.com/prosody-events/gabion/commit/fa57486b2b65a46da94f90367916af3358dc11c7))
* **wasm-viz:** stable-id identity through the frontend (Phase 6c-iii) ([612387f](https://github.com/prosody-events/gabion/commit/612387fd80d476d7471121579541f23cf3037bd4))
* **wasm-viz:** stage rebuild-knob edits behind an explicit Rebuild ([f7f14e0](https://github.com/prosody-events/gabion/commit/f7f14e0ee598a0c5600cab52ffec7f947645e922))
* **wasm-viz:** steel-blue comet beams — bright head, fading wake, grey drops ([e07e9b7](https://github.com/prosody-events/gabion/commit/e07e9b7c2168c4c56a1699587acb867fca3f7c74))
* **wasm-viz:** Strata small-multiples in the node inspector ([4b32e19](https://github.com/prosody-events/gabion/commit/4b32e1916c77db8b72ff4f2b6a9286abfd3f6ce8))
* **wasm-viz:** surface adaptive gossip decisions (effective fanout, error budget) ([4abbab4](https://github.com/prosody-events/gabion/commit/4abbab4d2be167ba961dabe87b17caaaddbc7e02))
* **wasm-viz:** sustained-overload preset + Aggregate-vs-Limit chart ([a9be391](https://github.com/prosody-events/gabion/commit/a9be39145114a037d9b9b16cdc1f3f1137d52983))
* **wasm-viz:** thinner, more subtle gossip beams ([07222b4](https://github.com/prosody-events/gabion/commit/07222b46d9dafff76e1aadd3393705ab3cca23ad))
* **wasm-viz:** unified light theme for the stage ([e79ef26](https://github.com/prosody-events/gabion/commit/e79ef26ad6181955252a4ec71be70c608c4afa05))
* **wasm-viz:** viz-friendly default rule; pin narrative preset limits ([581a326](https://github.com/prosody-events/gabion/commit/581a3269fe8a988e3fbdcbbecdf3cbf116a0a06c))
* **wasm-viz:** wasm add_node/remove_node + engine-driven heal (Phase 6c-ii) ([976d033](https://github.com/prosody-events/gabion/commit/976d0333554429056240d244f3377eba06e89d8d))
* wire new gossip module into gabion-server ([d0b3a14](https://github.com/prosody-events/gabion/commit/d0b3a14f1f92cfe2b22d9db1a18d9c37200ce9c4))


### Bug fixes

* **ci:** enable Pages site auto-enablement in deploy-pages ([eaf299a](https://github.com/prosody-events/gabion/commit/eaf299ada38455565c258f379b13e543c10f3cd1))
* **ci:** install rustup + prebuilt sccache on apt-based docker bases ([e8e6830](https://github.com/prosody-events/gabion/commit/e8e683052b48f0570fb9a1c930980acd15577d66))
* **ci:** pin wasm-pack v0.15.0 and stop publishing the loader image ([#17](https://github.com/prosody-events/gabion/issues/17)) ([350d5a9](https://github.com/prosody-events/gabion/commit/350d5a96050cb76050aa88ef40e111f485685b35))
* **ci:** switch release-please to simple+TOML jsonpath for workspace inheritance ([b7e619e](https://github.com/prosody-events/gabion/commit/b7e619eb0d0afb19ebcd5f8ae807b964aa4135dd))
* **crdt:** pin configured rules so they survive cell expiry ([55cd3bf](https://github.com/prosody-events/gabion/commit/55cd3bfc05e0caaf73cf74124bf72b19aaaa1869))
* **crdt:** repair lane must ignore the peer frontier so anti-entropy converges ([bfef432](https://github.com/prosody-events/gabion/commit/bfef432731db9cf03d07b6d3ce806d37344c0dc3))
* **discovery:** default to pod's own namespace, bail on fatal RBAC errors ([8c84a21](https://github.com/prosody-events/gabion/commit/8c84a21384cbf31072c0d65e682838a3afc375b5))
* **gossip:** size fanout by the ln(n)+c coverage threshold, not dirty-set bit length ([f64c7b7](https://github.com/prosody-events/gabion/commit/f64c7b76d34c6e63a512af56d46cf254fc5e5f6b))
* **k8s:** rewrite mixed nginx+gabion gossip test for new schemas ([bca4edf](https://github.com/prosody-events/gabion/commit/bca4edf465907346a54308506e2db974b8f89a94))
* **lease:** anchor expires_millis relative to SHM init, not unix epoch ([d9157a3](https://github.com/prosody-events/gabion/commit/d9157a39cc21bd1d1080005264aa40fb3a1fb2ce))
* **wasm-viz:** bucket-window strip scrolls as a fixed-width conveyor ([d09e3b7](https://github.com/prosody-events/gabion/commit/d09e3b7d64d98db86f72c4c1ea4c3bdd6bfb4ee8))
* **wasm-viz:** keep charts continuous across node join/leave ([96313f0](https://github.com/prosody-events/gabion/commit/96313f05ae9795ed1f5f1a412e3e38673e2c5a33))
* **wasm-viz:** perpetual gossip via continuous load + windowed oracle; add Sandbox preset ([4070296](https://github.com/prosody-events/gabion/commit/40702963115b99e2082c463655fe24eefd4e3cf6))
* **wasm-viz:** read applied (not staged) knobs in strip/limit displays ([c0433f3](https://github.com/prosody-events/gabion/commit/c0433f3c3f93e7b4c00394ebdc3e957c2365d622))
* **wasm-viz:** size the gossip send pool to one tick's output ([2bfefe8](https://github.com/prosody-events/gabion/commit/2bfefe857952b28593646793af8f1641bbe51f51))


### Performance

* **admission:** single-pass hot path, allow-by-default on internal limits ([4a257a4](https://github.com/prosody-events/gabion/commit/4a257a4ba6a030ee86d6c3f23cfe28f7e8155df4))


### Refactoring

* Box&lt;str&gt; sweep, per-rule exempt counter, audit-gap tests ([ed04354](https://github.com/prosody-events/gabion/commit/ed04354b0b134befc6eee52d2d36b3abfc8bcb2e))
* **crdt:** RuleDescriptor window/epoch helpers; expire_at uses them ([9731b31](https://github.com/prosody-events/gabion/commit/9731b3174e50445dec0e4bcc6a7b611408178e72))
* promote inline modules to files ([60b915c](https://github.com/prosody-events/gabion/commit/60b915c12f6b3114af8132ba80780471d5689994))
* **wasm-viz:** centralize + shrink the browser sim's per-node sizing ([5de42cc](https://github.com/prosody-events/gabion/commit/5de42ccc31853050047c468f22c19f864c0ddbd5))
* **wasm-viz:** drive Strata window from CRDT-reported epochs ([c015973](https://github.com/prosody-events/gabion/commit/c01597386dbb1764731d34e61899e33915062113))
* **wasm-viz:** plumb full AdminSnapshot to the node snapshot ([3470445](https://github.com/prosody-events/gabion/commit/347044525241752509ccfb2c1e9ab2244660953e))
* **wasm-viz:** source sim defaults from Rust, drop the TS hand-mirror ([40c3e3c](https://github.com/prosody-events/gabion/commit/40c3e3c8f9b70bdd9139dc6babcbb3398bf0f307))


### Tests

* checkpoint gossip rate limiting coverage ([4401baa](https://github.com/prosody-events/gabion/commit/4401baa5df4b521086be242957bbef170320c218))
* cover remaining invariant gaps ([db05d69](https://github.com/prosody-events/gabion/commit/db05d6907f2231466432c5b4966a93b233266be2))
* **gossip:** close 6 coverage gaps with property tests ([ea7a8ca](https://github.com/prosody-events/gabion/commit/ea7a8ca15a3ecd8097be76542c2a54699463e836))
* move all inline test modules to sibling files ([0a583d9](https://github.com/prosody-events/gabion/commit/0a583d99591a1a8823125552cb487e6cae6b8cb9))
* move test modules into dedicated files ([610d1eb](https://github.com/prosody-events/gabion/commit/610d1eb0778b4d4e8af7c7fb39f95257725d8c5c))
* **nginx,server:** close coverage gaps in identity, SHM header, admin, gRPC ([ca0eb0c](https://github.com/prosody-events/gabion/commit/ca0eb0c648f833f1cfc8d941704150f83f51b572))
* verify gossip remote merge laws ([a5e3d1c](https://github.com/prosody-events/gabion/commit/a5e3d1c4eb178d4112f3bba6d937c452ef400be0))
* **wasm-viz:** cover node-click + Reset in isolation/heal mode ([2d928af](https://github.com/prosody-events/gabion/commit/2d928afabaffa7df8ab8b66fa1edd72e46f7ce62))


### Build / CI

* **deps:** bump all open dependabot targets in one PR ([#18](https://github.com/prosody-events/gabion/issues/18)) ([93ccdb1](https://github.com/prosody-events/gabion/commit/93ccdb1840397cc32e366fedb4112f2e52ba57b6))
* **wasm-viz:** size-optimize the release wasm (-24% raw, -15% gzip) ([c9b89d7](https://github.com/prosody-events/gabion/commit/c9b89d7377588babaf2c52e733d72725b8afe44a))


### Documentation

* add walkthrough of the crdt module ([8fa256f](https://github.com/prosody-events/gabion/commit/8fa256f293e84f47777472178e9a8eeaa16a2f89))
* **gossip:** correct the fanout rationale (KMG, not Verma & Ooi) and repurpose the coverage bench ([a667168](https://github.com/prosody-events/gabion/commit/a667168af8d1b0490763ec7d60c85c3797feeb83))
* **nginx:** simplify the /api/upload composition example ([963fdac](https://github.com/prosody-events/gabion/commit/963fdaca2ba0b2a3c979bc2bd9db24e69a318260))
* pass over every README and CRDT.md ([f75ff0f](https://github.com/prosody-events/gabion/commit/f75ff0fb3c58d498a5e02fe8278b6969f37ece72))
* **readme:** drop Svelte references (implementation detail) ([32754e7](https://github.com/prosody-events/gabion/commit/32754e7ab089b7fdb5076dc1a22ac6acb8b58ebc))
* **readme:** feature simulator, flag gabiond experimental, name K8s discovery ([#16](https://github.com/prosody-events/gabion/issues/16)) ([76eb6dc](https://github.com/prosody-events/gabion/commit/76eb6dce9c9e766d821b5ccb9a3e9f4f0f652964))
* restructure README, split nginx adapter guide, add MIT license ([c731cc0](https://github.com/prosody-events/gabion/commit/c731cc0041e51f4b5e24c0d7d0b434bf90ea59d7))
* rewrite staccato fragments into flowing prose ([300120a](https://github.com/prosody-events/gabion/commit/300120a2ad905fc6403de7f586d8835b36b9734a))

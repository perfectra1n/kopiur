# Changelog

## [0.1.12](https://github.com/home-operations/kopiur/compare/0.1.11...0.1.12) (2026-06-08)


### Features

* **helm:** oops, secretProjection should be required opt-in ([9e2c849](https://github.com/home-operations/kopiur/commit/9e2c849bdbd9015cf1e5d8cb5b1552c3592293ce))

## [0.1.11](https://github.com/home-operations/kopiur/compare/0.1.10...0.1.11) (2026-06-08)


### Features

* **certs:** allow for self-signed certs instead of cert-manager as an option ([bb9cc5d](https://github.com/home-operations/kopiur/commit/bb9cc5d25e401b083bc95a9c04fbba5597b3d463))
* **certs:** implement tests and rbac for self-managed certs ([b91638a](https://github.com/home-operations/kopiur/commit/b91638a6b691d0a1d792baaee1efd40b5b0b34cc))
* **chart:** helm-docs README + values schema, release-time digest pinning ([#63](https://github.com/home-operations/kopiur/issues/63)) ([0cc4d3c](https://github.com/home-operations/kopiur/commit/0cc4d3c0aaf0602e3de6c6e07a2178f7cdaa7088))
* **dashboards:** support grafana operator dashboard thingy ([eb3b394](https://github.com/home-operations/kopiur/commit/eb3b394360198f3110c7b4e0f67cd4f761e80fd8))
* **docs:** add more useful docs ([93921db](https://github.com/home-operations/kopiur/commit/93921dbe158beb18072935f420487bc8855e6f73))
* **e2e:** preload the nfs image ([811fe3f](https://github.com/home-operations/kopiur/commit/811fe3f1565a359a3f3e92f65a36dfe25ed29ef6))
* **e2e:** update e2e for even more tests ([6a07512](https://github.com/home-operations/kopiur/commit/6a07512101fef353ac6e27f3e9135a71e7c618b1))
* **nfs:** I love e2e tests finding issues ([3488f52](https://github.com/home-operations/kopiur/commit/3488f52f8b66e16834254aaf5c98a18a50b6e8a4))
* **nfs:** support inline nfs to support onedr0p lol ([7c92884](https://github.com/home-operations/kopiur/commit/7c92884c4ef0a7bf200384623ff36703464c29e0))
* **secrets:** default-on secrets projection ([9896667](https://github.com/home-operations/kopiur/commit/9896667dd93268b851c6d5e6705943fceb5b9db7))
* **secrets:** implement secrets projection by default ([1921526](https://github.com/home-operations/kopiur/commit/1921526d1662cb76249559da7cb84ce2e1e9778c))
* **secrets:** jk secret projection is default opt-in ([cf69f61](https://github.com/home-operations/kopiur/commit/cf69f6113e2d16df19c7aa01d720a20ae8df1447))
* **secrets:** move projection into more granular CRDs ([e30f24f](https://github.com/home-operations/kopiur/commit/e30f24f5380a4d68d5394d817f521b5ec59fdbb3))
* **tests:** use a different nfs container for e2e testing ([e4b2816](https://github.com/home-operations/kopiur/commit/e4b28168ca8407617a9849982b17f8cd00491b26))


### Bug Fixes

* **prettier:** please stop messing with the CRDs oxfmt ([6c06aa9](https://github.com/home-operations/kopiur/commit/6c06aa9f2761674d9604c7d0f4973ee78a56066f))

## [0.1.10](https://github.com/home-operations/kopiur/compare/0.1.9...0.1.10) (2026-06-07)


### Features

* **backend:** also update support for various backends ([99d0942](https://github.com/home-operations/kopiur/commit/99d0942ec41cc97aaa5f46cf6a72073d251354f7))
* **docs:** add a slew of backend docs ([6bb1388](https://github.com/home-operations/kopiur/commit/6bb1388fb68672cb9daef014a5556d0c79691050))
* **docs:** create docs for the various backends ([49b19b3](https://github.com/home-operations/kopiur/commit/49b19b391e9236849935edd25b4073184a2a92d9))
* **docs:** migrate documentation site from mdBook to MkDocs Material ([527508c](https://github.com/home-operations/kopiur/commit/527508c9856d37a5353b35ae7e847c04c25f1e00))
* **docs:** promote rustdoc to a top-level header tab ([3c6d3c8](https://github.com/home-operations/kopiur/commit/3c6d3c809e4e4e2f7396b325301b1bdd763d9d89))
* **docs:** surface rustdoc in the MkDocs header ([ecc98a6](https://github.com/home-operations/kopiur/commit/ecc98a6dfb24695c57761a4e6d77362e43ee9344))
* **e2e:** make sure that values are consistent in e2e tests ([f5fe886](https://github.com/home-operations/kopiur/commit/f5fe8867e408ef119016757cc11a4174a14e9101))
* **tests:** implement more thorough e2e testing ([4e0d266](https://github.com/home-operations/kopiur/commit/4e0d26616d554a9a1b8f986d475299a2786b4e0b))
* **tests:** update broken unit test ([60d34be](https://github.com/home-operations/kopiur/commit/60d34beeb5364dd6e7a88c47ca0adc96ead00257))

## [0.1.9](https://github.com/home-operations/kopiur/compare/0.1.8...0.1.9) (2026-06-06)


### Features

* **dev:** update claude documentation skill ([43da6c8](https://github.com/home-operations/kopiur/commit/43da6c882e0ee9a03d153d1648224a8d46b944ad))
* **docs:** add more useful user-facing documentation ([b296ae0](https://github.com/home-operations/kopiur/commit/b296ae07a3e3c12ab33173ade98619a4bfd093a1))
* **e2e:** also add features to e2e tests ([8be3884](https://github.com/home-operations/kopiur/commit/8be388404d57e174dd27014b8d7de300b0d97cc0))
* **sa:** support stronger typing and testing for SA that goes cross namespace ([ff45c39](https://github.com/home-operations/kopiur/commit/ff45c3901cbe73f15c963be14b34bc3ccae03546))
* **tests:** continue to find wild issues through e2e testing, and resolve them ([77c3032](https://github.com/home-operations/kopiur/commit/77c30322fd3c9ad99964a1bf4184a4e42b7fa51c))


### Miscellaneous Chores

* we no longer have auto merge org wide ([23f3d39](https://github.com/home-operations/kopiur/commit/23f3d39768d2e9fe364db1af4369d30eb7c6a2d4))

## [0.1.8](https://github.com/home-operations/kopiur/compare/0.1.7...0.1.8) (2026-06-06)


### Features

* **docs:** add some useful docs ([6abd4f5](https://github.com/home-operations/kopiur/commit/6abd4f5421742d6f02228f7f269119d4ac5905f4))
* **docs:** implement docs for the movers ([b69f445](https://github.com/home-operations/kopiur/commit/b69f445173a88edc2017dc7cf6dfc65f433f0535))
* **docs:** take that mise ([ed2d9e2](https://github.com/home-operations/kopiur/commit/ed2d9e273ddb7f0f81c97285aa03a09140900a77))
* **maintenance:** make maintenance...actually do something ([e58232d](https://github.com/home-operations/kopiur/commit/e58232dacc1bcf056af82363fb767ab5aee6c4bc))
* **mover:** actually use secrets in movers and get rbac for it ([c8ab1de](https://github.com/home-operations/kopiur/commit/c8ab1deb9aa0a0b56ef1771d2a28571b99f9505d))
* **movers:** implement privileged movers ([334a9a5](https://github.com/home-operations/kopiur/commit/334a9a5919952e241883059f4ca375b813bac45a))
* **tests:** make sure to implement tests for updated rbac ([bb2a118](https://github.com/home-operations/kopiur/commit/bb2a118850eabc7b9d0ee9c3429ab897433a613c))

## [0.1.7](https://github.com/home-operations/kopiur/compare/0.1.6...0.1.7) (2026-06-06)


### Features

* **controller:** make sure not to spam the kube api every 0.33s ([9c4ca18](https://github.com/home-operations/kopiur/commit/9c4ca1817da9a34ade0530f77b67b0d768e3176f))
* **test:** create test to make sure we don't spam kube api ([2556258](https://github.com/home-operations/kopiur/commit/25562581ae099daba8b74d09c691ca6bfa71eab0))


### Miscellaneous Chores

* update rlspls config ([f9f4c70](https://github.com/home-operations/kopiur/commit/f9f4c707f1c43cd9deca761e058ebedfa3b931e1))

## [0.1.6](https://github.com/home-operations/kopiur/compare/0.1.5...0.1.6) (2026-06-06)


### Bug Fixes

* **deps:** update rust crate chrono (0.4.44 → 0.4.45) ([#46](https://github.com/home-operations/kopiur/issues/46)) ([30effc4](https://github.com/home-operations/kopiur/commit/30effc4a8eadab58fe26b4c41f9adbae6f9630a9))

## [0.1.5](https://github.com/home-operations/kopiur/compare/0.1.4...0.1.5) (2026-06-05)


### Features

* **errors:** provide more useful errors ([469947a](https://github.com/home-operations/kopiur/commit/469947aa00b460b145b050b3ef2cd15e74f2cf93))
* **tests:** also update tests so this writeable dir issue doesn't come back ([a6196f9](https://github.com/home-operations/kopiur/commit/a6196f912dce3cee82a9218f8dcdaa7ed4b7fa9c))


### Bug Fixes

* **controller:** make sure to mount writable paths for kopia ([f9fd3d5](https://github.com/home-operations/kopiur/commit/f9fd3d56d0543907ce07530fcf9ef2c4b1612ae1))

## [0.1.4](https://github.com/home-operations/kopiur/compare/0.1.3...0.1.4) (2026-06-05)


### Features

* **docs:** continue implementing rustdocs in crates ([b60b86a](https://github.com/home-operations/kopiur/commit/b60b86a7813510c9d73c8602b3499f480438c390))
* **docs:** make mdbook happy ([6d7f3d2](https://github.com/home-operations/kopiur/commit/6d7f3d2916470a2e241ca16633ff7a30eb9f88f6))
* **docs:** publish mdBook + rustdoc site to GitHub Pages ([74d7518](https://github.com/home-operations/kopiur/commit/74d7518a2dd6e6d382337c3abda6674dcbf3c85f))
* **docs:** serve docs site from kopiur.home-operations.com ([51d9ace](https://github.com/home-operations/kopiur/commit/51d9ace809125c42cb76aeecab4478a7cd0ac99a))
* **errors:** implement more error capturing for ease of use ([7fb10a1](https://github.com/home-operations/kopiur/commit/7fb10a1516662a0e9debe38b0d7e1267005de218))


### Bug Fixes

* **e2e:** resolve e2e errors for non-terminating pods ([cc83f67](https://github.com/home-operations/kopiur/commit/cc83f67b047c0bf851eccfb9e8d4475a5afeeafd))
* **mise:** try to resolve merge conflicts, again ([2b17028](https://github.com/home-operations/kopiur/commit/2b170282e1969dab4275b0608f34401bc41bdb22))

## [0.1.3](https://github.com/home-operations/kopiur/compare/0.1.2...0.1.3) (2026-06-04)


### Features

* **import:** allow Repository CRDs to be bootstrapped and imported ([b95d719](https://github.com/home-operations/kopiur/commit/b95d719b0741e4feb4d79de58dad1273d0cdb59f))
* **logs:** add some useful stdout logging to each container ([e44a9ca](https://github.com/home-operations/kopiur/commit/e44a9caf98aed5918bae0ef1a631a8b7ff93dfe3))
* **maintenance:** enable maintenance by default, but obviously allow overrides ([#48](https://github.com/home-operations/kopiur/issues/48)) ([c193929](https://github.com/home-operations/kopiur/commit/c193929a55f33bc06168febd51984a02921ebdba))

## [0.1.2](https://github.com/home-operations/kopiur/compare/0.1.1...0.1.2) (2026-06-04)


### Features

* **controller:** also add warning events if maintenace isn't configured ([5ba636b](https://github.com/home-operations/kopiur/commit/5ba636bcec78527bb267bc6aef1f293a6375ff5c))
* **rbac:** gonna need increased rbac perms for kubernetes event api push ([2f9f0a5](https://github.com/home-operations/kopiur/commit/2f9f0a5a47e992c270adf7d86221e583278a0735))


### Bug Fixes

* **mise:** pin rust to 1.95.0; correct renovate mise packageNames ([#36](https://github.com/home-operations/kopiur/issues/36)) ([5956f32](https://github.com/home-operations/kopiur/commit/5956f3249fe34ffbf35ce3d7eb98d4c69117b263))
* **schedule:** make sure to support `runOnCreate` ([0ebf046](https://github.com/home-operations/kopiur/commit/0ebf0463b3119599484b9e95dc4a2df03ac408d6))


### Miscellaneous Chores

* add 'cargo-llvm-cov' and 'cargo-deny' to package rules ([10db21b](https://github.com/home-operations/kopiur/commit/10db21b6c99be7c73b1014a29de1da1f37ffeedb))
* **mise:** lock file maintenance tool ([#38](https://github.com/home-operations/kopiur/issues/38)) ([31d117e](https://github.com/home-operations/kopiur/commit/31d117efb23b22b8efbd6931f9ddf03c703d7828))

## [0.1.1](https://github.com/home-operations/kopiur/compare/0.1.0...0.1.1) (2026-06-04)


### Features

* **charts:** bump up values for mem so e2e tests don't choke ([5fdace6](https://github.com/home-operations/kopiur/commit/5fdace6cf993de9a0a09a73494a3999f49974a20))
* **ci:** resolve broken release ci ([acd5fe7](https://github.com/home-operations/kopiur/commit/acd5fe7f8f32988ca787bfd3b065fff1db241402))
* **dev:** change CRD domain ([5eb5b28](https://github.com/home-operations/kopiur/commit/5eb5b28ea6684052dde721cae70866570058f2d5))
* **dev:** just a slight rename ([0a0d7b8](https://github.com/home-operations/kopiur/commit/0a0d7b8774f9cf9e46d412e2fdf4d0208eef1268))
* **dev:** update adr ([a0e37ed](https://github.com/home-operations/kopiur/commit/a0e37edb43d1c7d37cac73b36c067071ce98a69b))
* **dev:** update image prefix ([a41a676](https://github.com/home-operations/kopiur/commit/a41a676ab580ecd36035598cbb7fe4886f1cf468))
* **dev:** yay AGPL ([882445e](https://github.com/home-operations/kopiur/commit/882445e1d23f04a9755ca603ae448e036cbeed59))
* **everything:** also implement working e2e ([8a33b1c](https://github.com/home-operations/kopiur/commit/8a33b1c9fcbc50060e21601023188e2a9a1bbeb7))
* **everything:** implement the basics of the repo ([635ed2c](https://github.com/home-operations/kopiur/commit/635ed2cad321a9b4c9297a6145bc5b0394b982f5))
* **metrics:** also add docs for metrics addition ([1ac6f5e](https://github.com/home-operations/kopiur/commit/1ac6f5e24e71f6ddf642ed4a6109ebf9c9c28a8e))


### Bug Fixes

* **ci:** resolve issue with license in cargo-deny ([079002f](https://github.com/home-operations/kopiur/commit/079002fe51892e77d61bbcfe824b40008fdd8a40))
* **dev:** well I guess I got burnt on that merge conflict ([e2f3818](https://github.com/home-operations/kopiur/commit/e2f381871ceca323895230389b2fd6f9da280613))
* use the right trixie image ([c0f0ec5](https://github.com/home-operations/kopiur/commit/c0f0ec5abef853a04a0757aff8db0da6804f273f))


### Documentation

* **adr:** add kopia operator ADRs and kopiur Rust ADR ([caaa8a0](https://github.com/home-operations/kopiur/commit/caaa8a0ce647f8ca134bb5c69147dd201e0230bf))


### Miscellaneous Chores

* add mise and dotfiles ([#31](https://github.com/home-operations/kopiur/issues/31)) ([6ff63a8](https://github.com/home-operations/kopiur/commit/6ff63a8ce480627aa4ca6b4ccbff8eeeef1b2761))
* bring workflows up to the DAF ([#32](https://github.com/home-operations/kopiur/issues/32)) ([64666c3](https://github.com/home-operations/kopiur/commit/64666c3a8c53e9dcb496b73590c642f3145cfbb1))
* **deps:** lock file maintenance ([#30](https://github.com/home-operations/kopiur/issues/30)) ([940cc92](https://github.com/home-operations/kopiur/commit/940cc92a68188828783c1b50c8258143eb5506ef))
* update Dockerfiles to trixy ([f55e1ff](https://github.com/home-operations/kopiur/commit/f55e1ff89a1d1a7ed21fb8644a21e4aec7ad83fd))

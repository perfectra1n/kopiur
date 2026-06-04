# Changelog

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

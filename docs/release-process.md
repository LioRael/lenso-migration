# Release process

Plugins own their migration SQL. This workspace publishes independent Rust
libraries; it does not migrate application databases during release.

After reviewing and merging the implementation PR and verifying main CI, publish
from a clean checkout of that exact main commit, in dependency order:

1. `lenso-migration`.
2. `lenso-migration-d1`.
3. `lenso-postgres-kit`.

Use `cargo publish --locked -p <package>` through the Lenso Cargo wrapper where
available. Cargo obtains registry credentials through the operator's configured
credential provider; never add credentials to the repository or logs. Verify
registry visibility of each exact version before publishing its dependents.
Existing versions are immutable. Tag each verified version as `<package>@<version>`
and create its GitHub release from the reviewed main commit.

Consumer lockfile adoption and database migration remain separate operations.
A future Trusted Publishing workflow must be configured for each package before
claiming automated releases; this repository currently uses explicit operator
publication.

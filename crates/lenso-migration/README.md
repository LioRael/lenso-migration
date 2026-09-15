# lenso-migration

Backend-independent immutable migration definitions and history validation for
Plugin-owned storage. The crate has no database driver, async runtime, transport,
SQL parser, or execution side effects.

```rust
use lenso_migration::{AppliedMigration, Migration, MigrationPlan};

const MIGRATIONS: &[Migration] = &[
    Migration::new(1, "create-items", "CREATE TABLE items (id integer PRIMARY KEY)"),
];
let plan = MigrationPlan::new(MIGRATIONS)?;
let history: Vec<AppliedMigration> = vec![];
let status = plan.status(&history)?;
assert_eq!(status.current_version, 0);
assert_eq!(status.pending.len(), 1);
# Ok::<(), Box<dyn std::error::Error>>(())
```

Versions start at 1 and are contiguous. Names start with a lowercase ASCII letter
and contain lowercase letters, digits, `_`, or `-` (at most 128 bytes). SQL must
be nonempty. `sql_migrations!` embeds SQL from paths relative to the consuming
crate's `Cargo.toml`; files are never read at runtime.

Checksums are lowercase SHA-256 of the big-endian eight-byte version, a zero
byte, the exact UTF-8 name, a zero byte, and the exact SQL bytes. This preserves
the original `lenso-postgres-kit` ledger encoding; no trimming, SQL parsing, or
normalization affects the hash.

Adapters supply every ledger row sorted by version. History must be an exact
prefix of the authored plan. Missing, duplicate, reordered, renamed, or changed
entries fail closed. A database version beyond the plan returns `SchemaAhead`
before checking drift. An empty history has version 0 and all migrations pending.

The owning Plugin supplies separate SQL plans and histories for PostgreSQL and
D1 (for example `migrations/postgres/` and `migrations/d1/`). A common version
number does not make backend SQL or persisted histories interchangeable.
Backend adapters own schema/ledger existence, ownership checks, locking, version
storage limits, transaction or batch guarantees, and operator authorization.
The core never runs setup, upgrades, or automatic runtime migration.

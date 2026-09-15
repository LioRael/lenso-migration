//! Immutable migration definitions and fail-closed history validation.
//!
//! Plugins own their SQL and keep a separate plan and ledger for each backend.
//! This crate performs no I/O and knows nothing about schemas, roles, transport,
//! SQL dialects, execution, transactions, or runtime preparation.

use std::fmt;

use sha2::{Digest, Sha256};
use thiserror::Error;

/// One immutable, ordered migration owned by a Plugin.
#[derive(Clone, Copy)]
pub struct Migration {
    version: u64,
    name: &'static str,
    sql: &'static str,
}

impl Migration {
    /// Defines one migration. Validation happens when constructing a [`MigrationPlan`].
    pub const fn new(version: u64, name: &'static str, sql: &'static str) -> Self {
        Self { version, name, sql }
    }

    /// Returns the monotonic migration version.
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Returns the stable migration name.
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Returns the exact SQL body for the backend adapter to execute.
    pub const fn sql(&self) -> &'static str {
        self.sql
    }

    /// SHA-256 of the version (big-endian u64), NUL, name, NUL, and exact SQL bytes.
    ///
    /// This encoding preserves historical `lenso-postgres-kit` ledger checksums.
    pub fn checksum(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(self.version.to_be_bytes());
        digest.update([0]);
        digest.update(self.name.as_bytes());
        digest.update([0]);
        digest.update(self.sql.as_bytes());
        hex::encode(digest.finalize())
    }
}

impl fmt::Debug for Migration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Migration")
            .field("version", &self.version)
            .field("name", &self.name)
            .field("checksum", &self.checksum())
            .finish_non_exhaustive()
    }
}

/// A validated, immutable sequence of migrations for one backend's history.
#[derive(Clone, Copy, Debug)]
pub struct MigrationPlan {
    migrations: &'static [Migration],
}

impl MigrationPlan {
    /// Validates versions (starting at one without gaps), names, and SQL bodies.
    pub fn new(migrations: &'static [Migration]) -> Result<Self, PlanError> {
        validate_migrations(migrations)?;
        Ok(Self { migrations })
    }

    pub const fn migrations(&self) -> &'static [Migration] {
        self.migrations
    }

    pub fn current_version(&self) -> u64 {
        self.migrations.last().map_or(0, Migration::version)
    }

    /// Checks an ascending ledger against the exact authored prefix.
    ///
    /// Backend adapters must return every ledger row in version order. Missing,
    /// duplicate, reordered, renamed, or changed entries fail closed. A newer
    /// database takes precedence over drift, preserving the original PG policy.
    pub fn status(&self, applied: &[AppliedMigration]) -> Result<MigrationStatus, HistoryError> {
        if let Some(actual) = applied.iter().map(|migration| migration.version).max()
            && actual > self.current_version()
        {
            return Err(HistoryError::SchemaAhead {
                actual,
                expected: self.current_version(),
            });
        }
        for (index, actual) in applied.iter().enumerate() {
            let Some(expected) = self.migrations.get(index) else {
                return Err(HistoryError::SchemaAhead {
                    actual: actual.version,
                    expected: self.current_version(),
                });
            };
            if actual.version != expected.version()
                || actual.name != expected.name()
                || actual.checksum != expected.checksum()
            {
                return Err(HistoryError::HistoryDiverged {
                    version: actual.version,
                });
            }
        }
        Ok(MigrationStatus {
            current_version: applied.last().map_or(0, |migration| migration.version),
            target_version: self.current_version(),
            applied_count: applied.len(),
            pending: &self.migrations[applied.len()..],
        })
    }

    /// Returns pending SQL only after validating the complete applied history.
    pub fn pending(
        &self,
        applied: &[AppliedMigration],
    ) -> Result<&'static [Migration], HistoryError> {
        Ok(self.status(applied)?.pending)
    }
}

/// One persisted ledger entry supplied by a backend adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppliedMigration {
    pub version: u64,
    pub name: String,
    pub checksum: String,
}

/// A validated view of the applied prefix and remaining migrations.
#[derive(Clone, Copy, Debug)]
pub struct MigrationStatus {
    pub current_version: u64,
    pub target_version: u64,
    pub applied_count: usize,
    pub pending: &'static [Migration],
}

impl MigrationStatus {
    pub const fn is_current(&self) -> bool {
        self.pending.is_empty()
    }
}

/// An authored plan is invalid.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum PlanError {
    #[error("migration plan must contain at least one migration")]
    EmptyMigrations,
    #[error("migration `{name}` has version {actual}; expected {expected}")]
    NonContiguousVersion {
        name: &'static str,
        expected: u64,
        actual: u64,
    },
    #[error("migration version {version} has invalid name `{name}`")]
    InvalidMigrationName { version: u64, name: &'static str },
    #[error("migration version {version} has empty SQL")]
    EmptyMigrationSql { version: u64 },
}

/// The persisted history does not match the linked backend plan.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum HistoryError {
    #[error("migration history diverged at version {version}")]
    HistoryDiverged { version: u64 },
    #[error("database is at version {actual}, newer than supported version {expected}")]
    SchemaAhead { actual: u64, expected: u64 },
}

fn validate_migrations(migrations: &[Migration]) -> Result<(), PlanError> {
    if migrations.is_empty() {
        return Err(PlanError::EmptyMigrations);
    }

    for (index, migration) in migrations.iter().enumerate() {
        let expected = u64::try_from(index).expect("migration index fits u64") + 1;
        if migration.version != expected {
            return Err(PlanError::NonContiguousVersion {
                name: migration.name,
                expected,
                actual: migration.version,
            });
        }
        let mut bytes = migration.name.bytes();
        let valid_start = bytes.next().is_some_and(|byte| byte.is_ascii_lowercase());
        let valid_rest = bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        });
        if migration.name.len() > 128 || !valid_start || !valid_rest {
            return Err(PlanError::InvalidMigrationName {
                version: migration.version,
                name: migration.name,
            });
        }
        if migration.sql.trim().is_empty() {
            return Err(PlanError::EmptyMigrationSql {
                version: migration.version,
            });
        }
    }
    Ok(())
}

/// Declares immutable migrations whose SQL bodies live in dedicated files.
///
/// Paths are resolved from the owning crate's `CARGO_MANIFEST_DIR`. Keeping the
/// ordered version and stable name explicit makes review and checksum drift
/// behavior unchanged.
///
/// ```ignore
/// use lenso_migration::{Migration, sql_migrations};
///
/// const MIGRATIONS: &[Migration] = sql_migrations![
///     (1, "create-orders", "migrations/postgres/001_create_orders.sql"),
///     (2, "add-order-status", "migrations/postgres/002_add_order_status.sql"),
/// ];
/// ```
#[macro_export]
macro_rules! sql_migrations {
    (
        $(
            ($version:literal, $name:literal, $path:literal $(,)?)
        ),+ $(,)?
    ) => {
        &[
            $(
                $crate::Migration::new(
                    $version,
                    $name,
                    include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $path)),
                ),
            )+
        ]
    };
}

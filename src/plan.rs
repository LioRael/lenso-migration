use std::{fmt, sync::Arc};

use lenso_migration::MigrationPlan;
use thiserror::Error;

use crate::Migration;

/// An immutable description of one Plugin-owned `PostgreSQL` schema.
#[derive(Clone)]
pub struct SchemaPlan {
    schema: Arc<str>,
    plan: MigrationPlan,
}

impl SchemaPlan {
    /// Validates and creates a schema plan.
    pub fn new(
        schema: impl Into<Arc<str>>,
        migrations: &'static [Migration],
    ) -> Result<Self, PlanError> {
        let schema = schema.into();
        validate_schema_name(&schema)?;
        let plan = MigrationPlan::new(migrations).map_err(PlanError::from)?;
        for migration in plan.migrations() {
            if i64::try_from(migration.version()).is_err() {
                return Err(PlanError::MigrationVersionTooLarge {
                    version: migration.version(),
                });
            }
        }
        Ok(Self { schema, plan })
    }

    /// Returns the `PostgreSQL` schema name owned by the Plugin.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns the current version declared by the Plugin.
    pub fn current_version(&self) -> u64 {
        self.plan.current_version()
    }

    pub(crate) const fn migrations(&self) -> &'static [Migration] {
        self.plan.migrations()
    }
}

impl SchemaPlan {
    pub(crate) fn status(
        &self,
        applied: &[lenso_migration::AppliedMigration],
    ) -> Result<lenso_migration::MigrationStatus, lenso_migration::HistoryError> {
        self.plan.status(applied)
    }
}

impl fmt::Debug for SchemaPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchemaPlan")
            .field("schema", &self.schema)
            .field("current_version", &self.current_version())
            .field("migration_count", &self.plan.migrations().len())
            .finish()
    }
}

/// A schema plan is invalid and cannot be used for setup or preparation.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum PlanError {
    #[error("invalid owned schema name `{schema}`")]
    InvalidSchemaName { schema: Arc<str> },
    #[error("schema plan must contain at least one migration")]
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
    #[error("migration version {version} exceeds PostgreSQL bigint range")]
    MigrationVersionTooLarge { version: u64 },
}

fn validate_schema_name(schema: &str) -> Result<(), PlanError> {
    let valid_length = !schema.is_empty() && schema.len() <= 63;
    let mut bytes = schema.bytes();
    let valid_start = bytes.next().is_some_and(|byte| byte.is_ascii_lowercase());
    let valid_rest =
        bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
    let reserved =
        schema == "public" || schema == "information_schema" || schema.starts_with("pg_");
    if valid_length && valid_start && valid_rest && !reserved {
        Ok(())
    } else {
        Err(PlanError::InvalidSchemaName {
            schema: Arc::from(schema),
        })
    }
}

impl From<lenso_migration::PlanError> for PlanError {
    fn from(error: lenso_migration::PlanError) -> Self {
        match error {
            lenso_migration::PlanError::EmptyMigrations => Self::EmptyMigrations,
            lenso_migration::PlanError::NonContiguousVersion {
                name,
                expected,
                actual,
            } => Self::NonContiguousVersion {
                name,
                expected,
                actual,
            },
            lenso_migration::PlanError::InvalidMigrationName { version, name } => {
                Self::InvalidMigrationName { version, name }
            }
            lenso_migration::PlanError::EmptyMigrationSql { version } => {
                Self::EmptyMigrationSql { version }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &[Migration] = &[
        Migration::new(
            1,
            "create-items",
            "CREATE TABLE items (id bigint PRIMARY KEY)",
        ),
        Migration::new(2, "add-label", "ALTER TABLE items ADD COLUMN label text"),
    ];

    #[test]
    fn accepts_a_contiguous_owned_plan() {
        let plan = SchemaPlan::new("orders_module", VALID).unwrap();
        assert_eq!(plan.schema(), "orders_module");
        assert_eq!(plan.current_version(), 2);
        assert!(!VALID[0].checksum().is_empty());
    }

    #[test]
    fn rejects_shared_or_unsafe_schema_names() {
        for name in [
            "",
            "public",
            "pg_catalog",
            "Orders",
            "orders-module",
            "1orders",
        ] {
            assert!(matches!(
                SchemaPlan::new(name, VALID),
                Err(PlanError::InvalidSchemaName { .. })
            ));
        }
    }

    #[test]
    fn rejects_non_contiguous_migrations() {
        const GAP: &[Migration] = &[
            Migration::new(1, "create-items", "SELECT 1"),
            Migration::new(3, "skip-two", "SELECT 3"),
        ];
        assert!(matches!(
            SchemaPlan::new("orders", GAP),
            Err(PlanError::NonContiguousVersion {
                expected: 2,
                actual: 3,
                ..
            })
        ));
    }

    #[test]
    fn checksum_binds_version_name_and_sql() {
        let original = Migration::new(1, "create-items", "SELECT 1");
        let renamed = Migration::new(1, "create-records", "SELECT 1");
        let changed = Migration::new(1, "create-items", "SELECT 2");
        assert_ne!(original.checksum(), renamed.checksum());
        assert_ne!(original.checksum(), changed.checksum());
    }
}

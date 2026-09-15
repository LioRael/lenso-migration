use lenso_migration::{AppliedMigration, HistoryError, Migration, MigrationPlan, PlanError};

const MIGRATIONS: &[Migration] = &[
    Migration::new(1, "create-items", "SELECT 1"),
    Migration::new(2, "add-label", "SELECT 2"),
    Migration::new(3, "add-index", "SELECT 3"),
];

fn entry(migration: &Migration) -> AppliedMigration {
    AppliedMigration {
        version: migration.version(),
        name: migration.name().to_owned(),
        checksum: migration.checksum(),
    }
}

#[test]
fn legacy_checksum_vector_binds_exact_version_name_and_sql_bytes() {
    let original = MIGRATIONS[0];
    assert_eq!(
        original.checksum(),
        "2a32ee9d92b0e6990fc989b541d1a192afd0448a490c48af8fe703816b68d66d"
    );
    for changed in [
        Migration::new(2, "create-items", "SELECT 1"),
        Migration::new(1, "create-records", "SELECT 1"),
        Migration::new(1, "create-items", "SELECT 1\n"),
    ] {
        assert_ne!(original.checksum(), changed.checksum());
    }
}

#[test]
fn status_exposes_only_the_unapplied_suffix() {
    let plan = MigrationPlan::new(MIGRATIONS).unwrap();
    for applied_count in 0..=MIGRATIONS.len() {
        let history: Vec<_> = MIGRATIONS[..applied_count].iter().map(entry).collect();
        let status = plan.status(&history).unwrap();
        assert_eq!(
            status.current_version,
            u64::try_from(applied_count).unwrap()
        );
        assert_eq!(status.target_version, 3);
        assert_eq!(status.applied_count, applied_count);
        assert_eq!(status.is_current(), applied_count == 3);
        assert_eq!(
            status
                .pending
                .iter()
                .map(Migration::version)
                .collect::<Vec<_>>(),
            MIGRATIONS[applied_count..]
                .iter()
                .map(Migration::version)
                .collect::<Vec<_>>()
        );
        assert_eq!(plan.pending(&history).unwrap().len(), 3 - applied_count);
    }
}

#[test]
fn gaps_duplicates_and_reordered_history_fail_closed() {
    let plan = MigrationPlan::new(MIGRATIONS).unwrap();
    for (indices, version) in [
        (vec![1], 2),
        (vec![0, 2], 3),
        (vec![0, 0], 1),
        (vec![1, 0], 2),
    ] {
        let history: Vec<_> = indices
            .into_iter()
            .map(|index| entry(&MIGRATIONS[index]))
            .collect();
        assert_eq!(
            plan.status(&history).unwrap_err(),
            HistoryError::HistoryDiverged { version }
        );
        assert!(plan.pending(&history).is_err());
    }
}

#[test]
fn renamed_or_changed_applied_migration_fails_closed() {
    let plan = MigrationPlan::new(MIGRATIONS).unwrap();
    for changed in [
        Migration::new(1, "renamed", "SELECT 1"),
        Migration::new(1, "create-items", "SELECT 9"),
    ] {
        assert_eq!(
            plan.status(&[entry(&changed)]).unwrap_err(),
            HistoryError::HistoryDiverged { version: 1 }
        );
    }
    let mut renamed = entry(&MIGRATIONS[0]);
    renamed.name = "changed-with-original-checksum".to_owned();
    assert!(plan.status(&[renamed]).is_err());
}

#[test]
fn newer_database_takes_precedence_over_drift() {
    let plan = MigrationPlan::new(MIGRATIONS).unwrap();
    let history = [
        entry(&Migration::new(1, "drift", "SELECT 9")),
        entry(&Migration::new(4, "future", "SELECT 4")),
    ];
    assert_eq!(
        plan.status(&history).unwrap_err(),
        HistoryError::SchemaAhead {
            actual: 4,
            expected: 3
        }
    );
}

#[test]
fn invalid_authored_plans_are_rejected_before_history_is_used() {
    const INVALID_VERSIONS: &[(&[Migration], u64, u64)] = &[
        (&[Migration::new(0, "zero", "SELECT 1")], 1, 0),
        (&[Migration::new(2, "gap", "SELECT 1")], 1, 2),
        (
            &[
                Migration::new(1, "first", "SELECT 1"),
                Migration::new(1, "duplicate", "SELECT 2"),
            ],
            2,
            1,
        ),
    ];
    const INVALID_NAMES: &[&[Migration]] = &[
        &[Migration::new(1, "", "SELECT 1")],
        &[Migration::new(1, "Upper", "SELECT 1")],
        &[Migration::new(1, "bad name", "SELECT 1")],
    ];
    const EMPTY: &[Migration] = &[Migration::new(1, "empty", " \n\t")];

    assert_eq!(
        MigrationPlan::new(&[]).unwrap_err(),
        PlanError::EmptyMigrations
    );
    for &(migrations, expected, actual) in INVALID_VERSIONS {
        assert!(
            matches!(MigrationPlan::new(migrations), Err(PlanError::NonContiguousVersion { expected: e, actual: a, .. }) if e == expected && a == actual)
        );
    }
    for migrations in INVALID_NAMES {
        assert!(matches!(
            MigrationPlan::new(migrations),
            Err(PlanError::InvalidMigrationName { .. })
        ));
    }
    assert!(matches!(
        MigrationPlan::new(EMPTY),
        Err(PlanError::EmptyMigrationSql { version: 1 })
    ));
}

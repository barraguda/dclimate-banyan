// --- Tests ---
use crate::query::ValueFilter;
use crate::store::ColumnarDatastreamManager;
use crate::types::{ColumnDefinition, ColumnType, DataDefinition, Record, Value};

use super::*;
use anyhow::{anyhow, Result};
use banyan::store::MemStore;
use banyan::{Config, Secrets};
use banyan_utils::tags::Sha256Digest;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::Path;
use tempfile::tempdir; // For testing persistence
use tracing::info;
use tracing_subscriber::{fmt, EnvFilter}; // For logging in tests // Ensure Path is imported // For logging in tests

// Helper to initialize logging for tests
fn setup_logging() {
    let _ = fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("columnar=trace".parse().unwrap()),
        ) // Set default level trace for our crate
        .with_test_writer() // Write logs to per-test buffer
        .try_init(); // Ignore error if already initialized
}

// Helper to create MemStore easily in tests
fn create_test_mem_store() -> MemStore<Sha256Digest> {
    MemStore::new(usize::MAX, Sha256Digest::digest)
}

// Helper to create a manager with standard test schema
fn create_test_manager(
    store: MemStore<Sha256Digest>,
    path: Option<&Path>,
) -> Result<ColumnarDatastreamManager<MemStore<Sha256Digest>>> {
    let config = Config::debug_fast(); // Use fast config for tests
    let secrets = Secrets::default();
    let data_def = DataDefinition::new(vec![
        ColumnDefinition {
            name: "timestamp".to_string(),
            column_type: ColumnType::Timestamp,
        },
        ColumnDefinition {
            name: "device_id".to_string(),
            column_type: ColumnType::String,
        },
        ColumnDefinition {
            name: "temperature".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "humidity".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "status".to_string(),
            column_type: ColumnType::Enum(vec!["OK".into(), "WARN".into(), "ERROR".into()]),
        },
    ]);
    // Use load_or_initialize to handle potential existing state file
    ColumnarDatastreamManager::load_or_initialize(store, config, secrets, path, Some(data_def))
}

// Generates test records with predictable patterns and some nulls
fn create_test_records(start_offset: u64, count: usize) -> Vec<Record> {
    let mut records = Vec::new();
    for i in 0..count {
        let current_offset = start_offset + i as u64;
        let mut record = Record::new();
        record.insert(
            "timestamp".to_string(),
            // Timestamp in microseconds, increasing linearly
            Some(Value::Timestamp(current_offset as i64 * 1_000_000)),
        );
        record.insert(
            "device_id".to_string(),
            // Cycles through 10 device IDs
            Some(Value::String(format!("dev_{}", current_offset % 10))),
        );
        record.insert(
            "temperature".to_string(),
            // Base 20.0, adds cyclic offset part + linear index part
            Some(Value::Float(
                20.0 + (current_offset % 10) as f64 + (i as f64 * 0.1),
            )),
        );
        // Introduce NULLs for humidity every 5 records (i=0, 5, 10, ...)
        if i % 5 != 0 {
            record.insert(
                "humidity".to_string(),
                // Base 50.0, subtracts cyclic offset part + linear index part
                Some(Value::Float(
                    50.0 - (current_offset % 5) as f64 - (i as f64 * 0.05),
                )),
            );
        } else {
            // Explicitly insert None for NULL
            record.insert("humidity".to_string(), None);
        }
        record.insert(
            "status".to_string(),
            // Cycles through Enum indices 0, 1, 2
            Some(Value::Enum((current_offset % 3) as u32)),
        );

        records.push(record);
    }
    records
}

#[test]
fn test_extend_and_query_basic() -> Result<()> {
    // TODO: add null tests now that queries return that too.
    setup_logging();
    let store = create_test_mem_store();
    let mut manager = create_test_manager(store, None)?; // No persistence path

    let records1 = create_test_records(0, 50);
    let records2 = create_test_records(50, 50);

    info!("Extending with first batch (0-49)...");
    manager.extend(&records1)?;
    assert_eq!(manager.total_rows, 50);
    info!("First batch extended. total_rows: {}", manager.total_rows);
    for (name, cid) in &manager.column_cids {
        info!("  Column '{}' CID: {:?}", name, cid);
        assert!(
            cid.is_some(),
            "CID should be Some after first extend for column {}",
            name
        );
    }

    info!("Extending with second batch (50-99)...");
    manager.extend(&records2)?;
    assert_eq!(manager.total_rows, 100);
    info!("Second batch extended. total_rows: {}", manager.total_rows);
    for (name, cid) in &manager.column_cids {
        info!("  Column '{}' CID: {:?}", name, cid);
        assert!(
            cid.is_some(),
            "CID should remain Some after second extend for column {}",
            name
        );
    }

    info!("Querying all data for timestamp and temperature...");
    let results_iter = manager.query(
        vec!["timestamp".to_string(), "temperature".to_string()],
        (Bound::Unbounded, Bound::Unbounded), // Full offset range
        vec![],                               // No value filters
        (Bound::Unbounded, Bound::Unbounded), // Full time range
    )?;

    let results: Vec<BTreeMap<String, Option<Value>>> = results_iter.collect::<Result<_>>()?;
    info!("Query returned {} results.", results.len());

    assert_eq!(results.len(), 100, "Should return all 100 rows");

    // Check first record (offset 0, index i=0 in first batch)
    assert_eq!(
        results[0].get("timestamp"),
        Some(&Some(Value::Timestamp(0 * 1_000_000))), // offset 0
        "First record timestamp mismatch"
    );
    assert_eq!(
        results[0].get("temperature"),
        // 20.0 + (0 % 10) + (0 * 0.1) = 20.0
        Some(&Some(Value::Float(20.0))),
        "First record temperature mismatch"
    );

    // Check last record (offset 99, index i=49 in second batch)
    assert_eq!(
        results[99].get("timestamp"),
        Some(&Some(Value::Timestamp(99 * 1_000_000))), // offset 99
        "Last record timestamp mismatch"
    );
    assert_eq!(
        results[99].get("temperature"),
        // 20.0 + (99 % 10) + (49 * 0.1) = 20.0 + 9.0 + 4.9 = 33.9
        Some(&Some(Value::Float(33.9))), // Correct calculation: 20 + (99%10=9) + (i=49)*0.1 = 20+9+4.9
        "Last record temperature mismatch"
    );

    Ok(())
}

#[test]
fn test_query_with_offset_range() -> Result<()> {
    setup_logging();
    let store = create_test_mem_store();
    let mut manager = create_test_manager(store, None)?;
    let records = create_test_records(0, 100);
    manager.extend(&records)?;
    assert_eq!(manager.total_rows, 100);

    // Query offset range [10, 20) -> should yield 10 rows (offsets 10 through 19)
    info!("Querying offset range [10, 20)...");
    let results_iter = manager.query(
        vec!["timestamp".to_string()],
        (Bound::Included(10), Bound::Excluded(20)),
        vec![],
        (Bound::Unbounded, Bound::Unbounded),
    )?;
    let results: Vec<BTreeMap<String, Option<Value>>> = results_iter.collect::<Result<_>>()?;
    info!("Query [10, 20) returned {} results.", results.len());
    assert_eq!(results.len(), 10, "Offset range [10, 20) failed");
    assert_eq!(
        results[0].get("timestamp"),
        Some(&Some(Value::Timestamp(10 * 1_000_000))), // First element is offset 10
        "Offset range [10, 20) first element mismatch"
    );
    assert_eq!(
        results[9].get("timestamp"),
        Some(&Some(Value::Timestamp(19 * 1_000_000))), // Last element is offset 19
        "Offset range [10, 20) last element mismatch"
    );

    // Query offset range [95, ...) -> should yield 5 rows (offsets 95 through 99)
    info!("Querying offset range [95, Unbounded)...");
    let results_iter = manager.query(
        vec!["timestamp".to_string()],
        (Bound::Included(95), Bound::Unbounded), // Unbounded end should go up to total_rows (100)
        vec![],
        (Bound::Unbounded, Bound::Unbounded),
    )?;
    let results: Vec<BTreeMap<String, Option<Value>>> = results_iter.collect::<Result<_>>()?;
    info!("Query [95, ...) returned {} results.", results.len());
    assert_eq!(results.len(), 5, "Offset range [95, ...) failed"); // 100 - 95 = 5 rows
    assert_eq!(
        results[0].get("timestamp"),
        Some(&Some(Value::Timestamp(95 * 1_000_000))),
        "Offset range [95, ...) first element mismatch"
    );
    assert_eq!(
        results[4].get("timestamp"),
        Some(&Some(Value::Timestamp(99 * 1_000_000))),
        "Offset range [95, ...) last element mismatch"
    );

    Ok(())
}

#[test]
fn test_query_with_time_range() -> Result<()> {
    setup_logging();
    let store = create_test_mem_store();
    let mut manager = create_test_manager(store, None)?;
    let records = create_test_records(0, 100);
    manager.extend(&records)?;
    assert_eq!(manager.total_rows, 100);

    // Query time range corresponding to offsets [30, 40)
    // Timestamp for offset N is N * 1_000_000 micros
    let start_time = 30 * 1_000_000; // Included
    let end_time = 40 * 1_000_000; // Excluded (so last included is 39 * 1M)
    info!("Querying time range [{}, {})...", start_time, end_time);
    let results_iter = manager.query(
        vec!["timestamp".to_string()],
        (Bound::Unbounded, Bound::Unbounded), // No offset filter
        vec![],
        (Bound::Included(start_time), Bound::Excluded(end_time)), // Time filter
    )?;
    let results: Vec<BTreeMap<String, Option<Value>>> = results_iter.collect::<Result<_>>()?;
    info!("Time range query returned {} results.", results.len());

    // Should include rows with timestamps 30M, 31M, ..., 39M (10 rows)
    assert_eq!(results.len(), 10, "Time range query count mismatch");
    assert_eq!(
        results[0].get("timestamp"),
        Some(&Some(Value::Timestamp(start_time))), // First timestamp is 30M
        "Time range first element mismatch"
    );
    assert_eq!(
        results[9].get("timestamp"),
        Some(&Some(Value::Timestamp(39 * 1_000_000))), // Last timestamp is 39M
        "Time range last element mismatch"
    );

    Ok(())
}

#[test]
fn test_query_with_value_filter() -> Result<()> {
    setup_logging();
    let store = create_test_mem_store();
    let mut manager = create_test_manager(store, None)?;
    let records = create_test_records(0, 100);
    manager.extend(&records)?;
    assert_eq!(manager.total_rows, 100);

    // Filter: temperature > 28.0
    let filter = ValueFilter {
        column_name: "temperature".to_string(),
        operator: query::Comparison::GreaterThan,
        value: Value::Float(28.0),
    };
    info!("Querying with filter: temperature > 28.0");

    let results_iter = manager.query(
        vec!["timestamp".to_string(), "temperature".to_string()], // Request timestamp and filtered column
        (Bound::Unbounded, Bound::Unbounded),
        vec![filter], // Apply the filter
        (Bound::Unbounded, Bound::Unbounded),
    )?;

    let results: Vec<BTreeMap<String, Option<Value>>> = results_iter.collect::<Result<_>>()?;
    info!("Value filter query returned {} results.", results.len());

    // Manually calculate expected count and verify results
    let mut expected_count = 0;
    let mut checked_count = 0;
    let original_records_for_check = create_test_records(0, 100); // Regenerate for checking

    for i in 0..100 {
        let offset = i as u64;
        // Calculate temperature using the same logic as create_test_records
        // i corresponds to the index within the *single* batch generation here
        let temp = 20.0 + (offset % 10) as f64 + (i as f64 * 0.1);
        if temp > 28.0 {
            expected_count += 1;
            // Find the corresponding result row (results are ordered by offset)
            let result_row = results
                .get(checked_count)
                .ok_or_else(|| anyhow!("Result count mismatch during check"))?;
            // Check timestamp matches offset i
            assert_eq!(
                result_row.get("timestamp"),
                Some(&Some(Value::Timestamp(offset as i64 * 1_000_000)))
            );
            // Check temperature matches calculation and is > 28.0
            match result_row.get("temperature") {
                Some(&Some(Value::Float(t))) => {
                    // Use approximate comparison for floats
                    assert!(
                        (t - temp).abs() < f64::EPSILON,
                        "Temperature mismatch for offset {}",
                        offset
                    );
                    assert!(t > 28.0, "Filter failed for offset {}", offset);
                }
                _ => panic!(
                    "Temperature column missing or wrong type in result for offset {}",
                    offset
                ),
            }
            checked_count += 1;
        }
    }
    assert_eq!(results.len(), expected_count, "Final result count mismatch");
    assert_eq!(checked_count, expected_count, "Checked count mismatch");

    Ok(())
}

#[test]
fn test_query_with_null_handling() -> Result<()> {
    setup_logging();
    let store = create_test_mem_store();
    let mut manager = create_test_manager(store, None)?;
    // Create 10 records. humidity is None (NULL) at index i=0 and i=5 (offsets 0, 5).
    let records = create_test_records(0, 10);
    manager.extend(&records)?;
    assert_eq!(manager.total_rows, 10);

    info!("Querying timestamp and humidity (includes NULLs)...");
    let results_iter = manager.query(
        vec!["timestamp".to_string(), "humidity".to_string()],
        (Bound::Unbounded, Bound::Unbounded),
        vec![], // No filters initially
        (Bound::Unbounded, Bound::Unbounded),
    )?;

    let results: Vec<BTreeMap<String, Option<Value>>> = results_iter.collect::<Result<_>>()?;
    info!("Query returned {} results.", results.len());
    assert_eq!(
        results.len(),
        10,
        "Should return all 10 rows even with NULLs"
    );

    // Check presence/absence of humidity based on creation logic
    // Now we expect Some(None) for NULL values
    assert_eq!(
        results[0].get("humidity"),
        Some(&None),
        "Humidity should be NULL (Some(None)) at offset 0"
    ); // i=0
    assert!(
        matches!(results[1].get("humidity"), Some(&Some(Value::Float(_)))),
        "Humidity should be non-null (Some(Some(Value::Float))) at offset 1"
    ); // i=1
    assert!(
        matches!(results[2].get("humidity"), Some(&Some(Value::Float(_)))),
        "Humidity should be non-null (Some(Some(Value::Float))) at offset 2"
    ); // i=2
    assert!(
        matches!(results[3].get("humidity"), Some(&Some(Value::Float(_)))),
        "Humidity should be non-null (Some(Some(Value::Float))) at offset 3"
    ); // i=3
    assert!(
        matches!(results[4].get("humidity"), Some(&Some(Value::Float(_)))),
        "Humidity should be non-null (Some(Some(Value::Float))) at offset 4"
    ); // i=4
    assert_eq!(
        results[5].get("humidity"),
        Some(&None),
        "Humidity should be NULL (Some(None)) at offset 5"
    ); // i=5
    assert!(
        matches!(results[6].get("humidity"), Some(&Some(Value::Float(_)))),
        "Humidity should be non-null (Some(Some(Value::Float))) at offset 6"
    ); // i=6

    // Now query with a filter that applies to humidity
    let filter = ValueFilter {
        column_name: "humidity".to_string(),
        operator: query::Comparison::LessThan, // Example filter
        value: Value::Float(100.0),            // Should match all *non-null* humidity values
    };
    info!("Querying timestamp with filter: humidity < 100.0");
    let results_iter_filtered = manager.query(
        vec!["timestamp".to_string()], // Only request timestamp
        (Bound::Unbounded, Bound::Unbounded),
        vec![filter], // Apply the humidity filter
        (Bound::Unbounded, Bound::Unbounded),
    )?;
    let results_filtered: Vec<BTreeMap<String, Option<Value>>> =
        results_iter_filtered.collect::<Result<_>>()?;

    info!(
        "Filtered query returned {} results.",
        results_filtered.len()
    );
    // Expected: 10 total rows - 2 rows where humidity is NULL = 8 rows
    assert_eq!(
        results_filtered.len(),
        8,
        "Filter should exclude rows where humidity is NULL"
    );

    // Check that the timestamps correspond to the non-null humidity rows
    let expected_timestamps: Vec<i64> = vec![1, 2, 3, 4, 6, 7, 8, 9]
        .into_iter()
        .map(|offset| offset * 1_000_000)
        .collect();
    let actual_timestamps: Vec<i64> = results_filtered
        .iter()
        .map(|row| match row.get("timestamp") {
            Some(&Some(Value::Timestamp(ts))) => ts,
            _ => panic!("Timestamp missing in filtered result"),
        })
        .collect();
    assert_eq!(
        actual_timestamps, expected_timestamps,
        "Filtered timestamps do not match expected non-null rows"
    );

    Ok(())
}

#[test]
fn test_persistence_save_load() -> Result<()> {
    setup_logging();
    let temp_dir = tempdir()?;
    let state_path = temp_dir.path().join("manager_state.json");
    info!("Using temp state file: {:?}", state_path);

    let store1 = create_test_mem_store();
    let data_def_for_check = DataDefinition::new(vec![
        /* ... same as in create_test_manager ... */
        ColumnDefinition {
            name: "timestamp".to_string(),
            column_type: ColumnType::Timestamp,
        },
        ColumnDefinition {
            name: "device_id".to_string(),
            column_type: ColumnType::String,
        },
        ColumnDefinition {
            name: "temperature".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "humidity".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "status".to_string(),
            column_type: ColumnType::Enum(vec!["OK".into(), "WARN".into(), "ERROR".into()]),
        },
    ]); // Define it once for comparison later
    let mut manager1 = create_test_manager(store1.clone(), Some(&state_path))?;

    let records = create_test_records(0, 25);
    info!("Extending manager1...");
    manager1.extend(&records)?;
    assert_eq!(manager1.total_rows, 25);

    info!("Saving manager1 state...");
    manager1.save_state()?;
    assert!(state_path.exists(), "State file was not created");

    // Store state for comparison before dropping manager1
    let manager1_cids = manager1.column_cids.clone();
    let manager1_rows = manager1.total_rows;
    // let manager1_data_def = manager1.data_definition.clone(); // Clone Arc if needed, or use data_def_for_check
    drop(manager1);

    info!("Creating manager2 from saved state...");
    let mut manager2 = ColumnarDatastreamManager::load_or_initialize(
        store1.clone(),       // Use the same store instance
        Config::debug_fast(), // Provide config/secrets again for loading
        Secrets::default(),
        Some(&state_path),
        None, // Don't provide default def, force load
    )?;

    info!("Checking loaded state...");
    assert_eq!(
        manager2.total_rows, manager1_rows,
        "Loaded total_rows mismatch"
    );
    assert_eq!(
        manager2.column_cids, manager1_cids,
        "Loaded column_cids mismatch"
    );
    // Compare the Arc<DataDefinition> inner values
    assert_eq!(
        *manager2.data_definition, data_def_for_check,
        "Loaded data_definition mismatch"
    );

    info!("Querying loaded manager2...");
    let results_iter = manager2.query(
        vec!["timestamp".to_string()],
        (Bound::Included(20), Bound::Unbounded), // Query range [20, 25)
        vec![],
        (Bound::Unbounded, Bound::Unbounded),
    )?;
    let results: Vec<BTreeMap<String, Option<Value>>> = results_iter.collect::<Result<_>>()?;
    info!(
        "Query on loaded manager returned {} results.",
        results.len()
    );
    assert_eq!(results.len(), 5, "Query after load failed"); // 25 - 20 = 5
    assert_eq!(
        results[0].get("timestamp"),
        Some(&Some(Value::Timestamp(20 * 1_000_000)))
    );
    assert_eq!(
        results[4].get("timestamp"),
        Some(&Some(Value::Timestamp(24 * 1_000_000)))
    );

    info!("Extending loaded manager2...");
    let records2 = create_test_records(25, 15); // Add 15 more records
    manager2.extend(&records2)?;
    assert_eq!(
        manager2.total_rows, 40,
        "Total rows after extending loaded manager mismatch"
    );

    info!("Querying extended manager2...");
    let results_iter = manager2.query(
        vec!["timestamp".to_string()],
        (Bound::Unbounded, Bound::Unbounded), // Full range
        vec![],
        (Bound::Unbounded, Bound::Unbounded),
    )?;
    let results: Vec<BTreeMap<String, Option<Value>>> = results_iter.collect::<Result<_>>()?;
    assert_eq!(
        results.len(),
        40,
        "Query after extending loaded manager failed"
    );
    assert_eq!(
        results[39].get("timestamp"),
        Some(&Some(Value::Timestamp(39 * 1_000_000)))
    );

    info!("Saving extended manager2 state...");
    manager2.save_state()?; // Save again after extending

    Ok(())
}

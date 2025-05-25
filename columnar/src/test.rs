// dclimate-banyan/circus/src/test.rs
#[cfg(test)]
mod tests {
    use super::super::*; // Import everything from the parent module (src/lib.rs scope)
    use crate::{
        compression::{
            compress_f64_column, compress_i64_column, compress_string_column,
            decompress_f64_column, decompress_i64_column, decompress_string_column,
        },
        error::BanyanRowSeqError,
        types::{
            CborRoaringBitmap, ColumnChunkData, ColumnDefinition, ColumnType, DataDefinition,
            Record, UserFilter, UserFilterOp, Value,
        }, // Add necessary types
        BanyanRowSeqStore,
        DatastreamRowSeq,
    };
    use anyhow::{anyhow, Result};
    use banyan::{
        store::{BranchCache, MemStore as BanyanMemStore},
        Config, Forest, Secrets, StreamBuilder, Transaction, Tree, TreeTypes,
    };
    use banyan_utils::tags::Sha256Digest;
    use libipld::Cid;
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use roaring::RoaringBitmap;
    use std::{collections::BTreeMap, ops::Bound, sync::Arc, time::Duration};
    use tempfile::tempdir;
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;

    // --- Constants ---
    const TEST_STORE_SIZE: usize = 10 * 1024 * 1024; // 10 MB for tests
    const TIMESTAMP_COL: &str = "timestamp";
    const INT_COL: &str = "int_val";
    const FLOAT_COL: &str = "float_val";
    const STR_COL: &str = "str_val";
    const ENUM_COL: &str = "enum_val";
    const ENUM_VARIANTS: &[&str] = &["Alpha", "Beta", "Gamma"];

    // --- Helper: Setup Tracing ---
    fn setup_tracing() {
        let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::TRACE) // Adjust level as needed (TRACE, DEBUG, INFO)
            .with_test_writer() // Write to test output
            .finish();
        // Use try_init to avoid panic if already initialized
        let _ = tracing::subscriber::set_global_default(subscriber);
    }

    // --- Helper: Create Memory Store ---
    type TestMemStore = BanyanMemStore<Sha256Digest>;

    fn memory_store(max_size: usize) -> TestMemStore {
        TestMemStore::new(max_size, Sha256Digest::digest)
    }

    // --- Helper: Create Test Schema ---
    fn create_test_schema() -> DataDefinition {
        DataDefinition::new(vec![
            ColumnDefinition {
                name: TIMESTAMP_COL.to_string(),
                column_type: ColumnType::Timestamp,
            },
            ColumnDefinition {
                name: INT_COL.to_string(),
                column_type: ColumnType::Integer,
            },
            ColumnDefinition {
                name: FLOAT_COL.to_string(),
                column_type: ColumnType::Float,
            },
            ColumnDefinition {
                name: STR_COL.to_string(),
                column_type: ColumnType::String,
            },
            ColumnDefinition {
                name: ENUM_COL.to_string(),
                column_type: ColumnType::Enum(
                    ENUM_VARIANTS.iter().map(|s| s.to_string()).collect(),
                ),
            },
        ])
    }

    // --- Helper: Generate Test Records ---
    /// Generates `num_records` with somewhat predictable but varied data.
    /// Timestamps are monotonically increasing.
    /// Includes optional None values based on `null_probability`.
    fn generate_records(
        num_records: usize,
        start_ts_micros: i64,
        ts_increment_micros: i64,
        null_probability: f64, // 0.0 to 1.0
        seed: u64,
    ) -> Vec<Record> {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut records = Vec::with_capacity(num_records);
        let mut current_ts = start_ts_micros;

        for i in 0..num_records {
            let mut record = Record::new();

            // Timestamp (always present and increasing)
            record.insert(
                TIMESTAMP_COL.to_string(),
                Some(Value::Timestamp(current_ts)),
            );

            // Integer: Some simple pattern + randomness, optional null
            if rng.gen::<f64>() >= null_probability {
                let val = (i as i64 * 10) + rng.gen_range(-5..5);
                record.insert(INT_COL.to_string(), Some(Value::Integer(val)));
            } else {
                record.insert(INT_COL.to_string(), None);
            }

            // Float: Similar pattern, optional null
            if rng.gen::<f64>() >= null_probability {
                let val = (i as f64 * 1.5) + rng.gen_range(-0.5..0.5);
                // Ensure clean float representation for comparison if needed
                // let val_str = format!("{:.3}", val);
                // let val_clean = val_str.parse::<f64>().unwrap_or(val);
                record.insert(FLOAT_COL.to_string(), Some(Value::Float(val)));
            } else {
                record.insert(FLOAT_COL.to_string(), None);
            }

            // String: Based on index, optional null
            if rng.gen::<f64>() >= null_probability {
                record.insert(
                    STR_COL.to_string(),
                    Some(Value::String(format!("record_{}", i))),
                );
            } else {
                record.insert(STR_COL.to_string(), None);
            }

            // Enum: Cycle through variants, optional null
            if rng.gen::<f64>() >= null_probability {
                let enum_idx = (i % ENUM_VARIANTS.len()) as u32;
                record.insert(ENUM_COL.to_string(), Some(Value::Enum(enum_idx)));
            } else {
                record.insert(ENUM_COL.to_string(), None);
            }

            records.push(record);
            current_ts += ts_increment_micros;
        }
        records
    }

    // --- Helper: Assert Records Match ---
    /// Compares two slices of Records, handling Option<Value> and float nuances.
    fn assert_records_match(actual: &[Record], expected: &[Record]) {
        assert_eq!(
            actual.len(),
            expected.len(),
            "Number of records does not match"
        );
        for (i, (act_rec, exp_rec)) in actual.iter().zip(expected.iter()).enumerate() {
            assert_eq!(
                act_rec.len(),
                exp_rec.len(),
                "Record {} has different number of columns",
                i
            );
            for (col_name, exp_val_opt) in exp_rec {
                let act_val_opt = act_rec.get(col_name).unwrap_or_else(|| {
                    panic!("Column '{}' missing in actual record {}", col_name, i)
                });

                match (act_val_opt, exp_val_opt) {
                    (Some(Value::Float(a)), Some(Value::Float(e))) => {
                        // Use direct comparison for floats assuming no complex calculations
                        // If needed, use approx crate: assert_relative_eq!(*a, *e, epsilon = 1e-9);
                        assert_eq!(
                            a, e,
                            "Float mismatch in record {}, column '{}': actual={}, expected={}",
                            i, col_name, a, e
                        );
                    }
                    (a, e) => {
                        assert_eq!(
                            a, e,
                            "Value mismatch in record {}, column '{}': actual={:?}, expected={:?}",
                            i, col_name, a, e
                        );
                    }
                }
            }
        }
    }

    // --- Helper: Create Test Datastream ---
    fn create_test_datastream(
        schema: DataDefinition,
        persistence_path: Option<&std::path::Path>,
    ) -> Result<DatastreamRowSeq<TestMemStore>> {
        let store = memory_store(TEST_STORE_SIZE);
        let config = Config::default(); // Use default config for tests
        let secrets = Secrets::default(); // Default secrets (no encryption)
        DatastreamRowSeq::load_or_initialize(store, config, secrets, persistence_path, schema)
    }

    // ==========================================================================
    // Compression/Decompression Tests
    // ==========================================================================

    #[test]
    fn test_compress_decompress_i64_roundtrip() -> Result<()> {
        setup_tracing();
        let inputs: Vec<Vec<Option<i64>>> = vec![
            vec![],
            vec![None],
            vec![Some(10)],
            vec![None, None, None],
            vec![Some(1), Some(2), Some(3)],
            vec![Some(100), None, Some(-50), None, Some(999)],
            vec![Some(i64::MIN), Some(0), Some(i64::MAX)],
            (0..100)
                .map(|i| {
                    if i % 3 == 0 {
                        None
                    } else {
                        Some(i * 100 - 5000)
                    }
                })
                .collect(),
        ];

        for original_data in inputs {
            let (bitmap, compressed) = compress_i64_column(&original_data)?;
            let chunk_data = ColumnChunkData {
                present: CborRoaringBitmap::from_bitmap(bitmap),
                data: compressed,
            };
            let decompressed_data = decompress_i64_column(&chunk_data)?;
            assert_eq!(
                original_data, decompressed_data,
                "Mismatch for i64 roundtrip. Original: {:?}, Decompressed: {:?}",
                original_data, decompressed_data
            );
        }
        Ok(())
    }

    #[test]
    fn test_compress_decompress_f64_roundtrip() -> Result<()> {
        setup_tracing();
        let inputs: Vec<Vec<Option<f64>>> = vec![
            vec![],
            vec![None],
            vec![Some(10.5)],
            vec![None, None, None],
            vec![Some(1.1), Some(2.2), Some(3.3)],
            vec![Some(100.123), None, Some(-50.456), None, Some(999.0)],
            vec![
                Some(f64::MIN),
                Some(0.0),
                Some(f64::MAX),
                Some(f64::NAN),
                Some(f64::INFINITY),
            ],
            (0..100)
                .map(|i| {
                    if i % 4 == 0 {
                        None
                    } else {
                        Some(i as f64 * 1.1 - 50.0)
                    }
                })
                .collect(),
        ];

        for original_data in inputs {
            let (bitmap, compressed) = compress_f64_column(&original_data)?;
            let chunk_data = ColumnChunkData {
                present: CborRoaringBitmap::from_bitmap(bitmap),
                data: compressed,
            };
            let decompressed_data = decompress_f64_column(&chunk_data)?;
            // Special handling for NaN comparison
            assert_eq!(original_data.len(), decompressed_data.len());
            for (orig_opt, decomp_opt) in original_data.iter().zip(decompressed_data.iter()) {
                match (orig_opt, decomp_opt) {
                    (Some(orig_val), Some(decomp_val)) => {
                        if orig_val.is_nan() {
                            assert!(
                                decomp_val.is_nan(),
                                "NaN mismatch. Original: {:?}, Decompressed: {:?}",
                                original_data,
                                decompressed_data
                            );
                        } else {
                            assert_eq!(
                                orig_val, decomp_val,
                                "Mismatch for f64 roundtrip. Original: {:?}, Decompressed: {:?}",
                                original_data, decompressed_data
                            );
                        }
                    }
                    (None, None) => { /* Matches */ }
                    _ => panic!(
                        "Mismatch in Option presence. Original: {:?}, Decompressed: {:?}",
                        original_data, decompressed_data
                    ),
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_compress_decompress_string_roundtrip() -> Result<()> {
        setup_tracing();
        let inputs: Vec<Vec<Option<String>>> = vec![
            vec![],
            vec![None],
            vec![Some("hello".to_string())],
            vec![None, None, None],
            vec![
                Some("a".to_string()),
                Some("b".to_string()),
                Some("c".to_string()),
            ],
            vec![
                Some("long string here".to_string()),
                None,
                Some("another one".to_string()),
                None,
                Some("".to_string()),
            ],
            (0..100)
                .map(|i| {
                    if i % 5 == 0 {
                        None
                    } else {
                        Some(format!("string_{}", i))
                    }
                })
                .collect(),
        ];

        for original_data in inputs {
            let (bitmap, compressed) = compress_string_column(&original_data)?;
            let chunk_data = ColumnChunkData {
                present: CborRoaringBitmap::from_bitmap(bitmap),
                data: compressed,
            };
            let decompressed_data = decompress_string_column(&chunk_data)?;
            assert_eq!(
                original_data, decompressed_data,
                "Mismatch for String roundtrip. Original: {:?}, Decompressed: {:?}",
                original_data, decompressed_data
            );
        }
        Ok(())
    }

    // Basic test for enum (u32) - relies on i64/pco test coverage mostly
    #[test]
    fn test_compress_decompress_enum_roundtrip() -> Result<()> {
        setup_tracing();
        let inputs: Vec<Vec<Option<u32>>> = vec![
            vec![],
            vec![None],
            vec![Some(1)],
            vec![Some(0), Some(1), Some(2), Some(0)],
            vec![Some(10), None, Some(0), None, Some(u32::MAX)],
        ];

        for original_data in inputs {
            // Reuse i64 compression/decompression logic via helper functions if needed
            // For now, call the specific enum functions directly
            let (bitmap, compressed) = compression::compress_enum_column(&original_data)?;
            let chunk_data = ColumnChunkData {
                present: CborRoaringBitmap::from_bitmap(bitmap),
                data: compressed,
            };
            let decompressed_data = compression::decompress_enum_column(&chunk_data)?;
            assert_eq!(
                original_data, decompressed_data,
                "Mismatch for Enum(u32) roundtrip. Original: {:?}, Decompressed: {:?}",
                original_data, decompressed_data
            );
        }
        Ok(())
    }

    #[test]
    fn test_decompress_bitmap_value_mismatch() -> Result<()> {
        setup_tracing();
        // Test i64 mismatch
        let original_i64: Vec<Option<i64>> = vec![Some(1), Some(2)];
        let (bitmap_ok, compressed_ok) = compress_i64_column(&original_i64)?;
        assert_eq!(bitmap_ok.len(), 2);

        // Create bad bitmap (too many indices)
        let mut bad_bitmap = bitmap_ok.clone();
        bad_bitmap.insert(10); // Add an extra index not corresponding to a value
        let bad_chunk_i64 = ColumnChunkData {
            present: CborRoaringBitmap::from_bitmap(bad_bitmap),
            data: compressed_ok.clone(),
        };
        let result_i64 = decompress_i64_column(&bad_chunk_i64);
        assert!(
            matches!(
                result_i64,
                Err(BanyanRowSeqError::BitmapValueMismatch { .. })
            ),
            "Expected BitmapValueMismatch error for i64"
        );

        // Create bad bitmap (too few indices - although pco might handle this differently)
        // Let's test mismatch where bitmap is correct but data is truncated (pco error)
        let bad_data_chunk_i64 = ColumnChunkData {
            present: CborRoaringBitmap::from_bitmap(bitmap_ok),
            data: compressed_ok[..compressed_ok.len() / 2].to_vec(), // Truncated data
        };
        let result_i64_trunc = decompress_i64_column(&bad_data_chunk_i64);
        assert!(
            matches!(
                result_i64_trunc,
                Err(BanyanRowSeqError::DecompressionError { .. })
            ),
            "Expected DecompressionError for truncated pco data"
        );

        // Test String mismatch
        let original_str: Vec<Option<String>> = vec![Some("a".to_string()), Some("b".to_string())];
        let (bitmap_str_ok, compressed_str_ok) = compress_string_column(&original_str)?;
        assert_eq!(bitmap_str_ok.len(), 2);

        let mut bad_bitmap_str = bitmap_str_ok.clone();
        bad_bitmap_str.insert(5);
        let bad_chunk_str = ColumnChunkData {
            present: CborRoaringBitmap::from_bitmap(bad_bitmap_str),
            data: compressed_str_ok.clone(),
        };
        let result_str = decompress_string_column(&bad_chunk_str);
        assert!(
            matches!(
                result_str,
                Err(BanyanRowSeqError::BitmapValueMismatch { .. })
            ),
            "Expected BitmapValueMismatch error for String"
        );

        Ok(())
    }

    #[test]
    fn test_reconstruct_sparse_out_of_bounds() -> Result<()> {
        setup_tracing();
        let dense_values = vec![10, 20];
        let mut present_bitmap = RoaringBitmap::new();
        present_bitmap.insert(0);
        present_bitmap.insert(5); // Index 5 is out of bounds for expected len (2)

        let result =
            compression::reconstruct_sparse(dense_values.clone(), &present_bitmap, "test_col");

        assert!(
            matches!(result, Err(BanyanRowSeqError::DecompressionError(msg)) if msg.contains("Bitmap index 5 out of bounds")),
            "Expected Out of Bounds DecompressionError"
        );

        // Test where bitmap count matches dense len, but max index implies larger sparse vec
        let mut present_bitmap_ok_count = RoaringBitmap::new();
        present_bitmap_ok_count.insert(0);
        present_bitmap_ok_count.insert(2); // Max index is 2, implies len 3, but dense_len is 2
        let result_ok_count =
            compression::reconstruct_sparse(dense_values, &present_bitmap_ok_count, "test_col_ok");
        assert!(
            result_ok_count.is_ok(),
            "Reconstruction should succeed when max index > dense_len but count matches"
        );
        assert_eq!(result_ok_count.unwrap(), vec![Some(10), None, Some(20)]);

        Ok(())
    }

    // ==========================================================================
    // RowSeqData Compression/Decompression Tests
    // ==========================================================================

    #[test]
    fn test_compress_row_seq_data_and_decompress_columns() -> Result<()> {
        setup_tracing();
        let schema = create_test_schema();
        let records = generate_records(50, 1_000_000, 10_000, 0.1, 42);

        // --- Compress ---
        let row_seq_data = compression::compress_row_seq_data(&records, &schema)?;

        assert_eq!(row_seq_data.num_rows, 50);
        assert!(!row_seq_data.timestamps_micros.is_empty());
        assert!(row_seq_data.columns.contains_key(INT_COL));
        assert!(row_seq_data.columns.contains_key(FLOAT_COL));
        assert!(row_seq_data.columns.contains_key(STR_COL));
        assert!(row_seq_data.columns.contains_key(ENUM_COL));

        // --- Decompress and Verify Each Column ---
        let col_names = [TIMESTAMP_COL, INT_COL, FLOAT_COL, STR_COL, ENUM_COL];
        for col_name in col_names {
            let decompressed_col_values =
                compression::decompress_column_from_chunk(&row_seq_data, col_name, &schema)?;

            assert_eq!(
                decompressed_col_values.len(),
                records.len(),
                "Decompressed column '{}' has wrong length",
                col_name
            );

            // Extract original values for comparison
            let original_col_values: Vec<Option<Value>> = records
                .iter()
                .map(|rec| rec.get(col_name).cloned().flatten())
                .collect();

            // Compare (handles float NAN etc. if necessary)
            assert_eq!(original_col_values.len(), decompressed_col_values.len());
            for (i, (orig_opt, decomp_opt)) in original_col_values
                .iter()
                .zip(decompressed_col_values.iter())
                .enumerate()
            {
                match (orig_opt, decomp_opt) {
                    (Some(Value::Float(orig_val)), Some(Value::Float(decomp_val))) => {
                        if orig_val.is_nan() {
                            assert!(
                                decomp_val.is_nan(),
                                "NaN mismatch in column '{}', row {}",
                                col_name,
                                i
                            );
                        } else {
                            assert_eq!(
                                orig_val, decomp_val,
                                "Mismatch in column '{}', row {}: expected {:?}, got {:?}",
                                col_name, i, orig_opt, decomp_opt
                            );
                        }
                    }
                    _ => assert_eq!(
                        orig_opt, decomp_opt,
                        "Mismatch in column '{}', row {}: expected {:?}, got {:?}",
                        col_name, i, orig_opt, decomp_opt
                    ),
                }
            }
        }

        // Test decompressing a column not present in the chunk (should return all None)
        let missing_col_def = ColumnDefinition {
            name: "missing_col".to_string(),
            column_type: ColumnType::Integer,
        };
        let mut schema_with_missing = schema.clone();
        schema_with_missing.columns.push(missing_col_def);
        schema_with_missing.rebuild_index();

        let decompressed_missing = compression::decompress_column_from_chunk(
            &row_seq_data,
            "missing_col",
            &schema_with_missing,
        )?;
        assert_eq!(decompressed_missing.len(), records.len());
        assert!(
            decompressed_missing.iter().all(|v| v.is_none()),
            "Decompressed missing column should be all None"
        );

        Ok(())
    }

    #[test]
    fn test_compress_row_seq_data_empty() -> Result<()> {
        setup_tracing();
        let schema = create_test_schema();
        let records: Vec<Record> = vec![];

        let row_seq_data = compression::compress_row_seq_data(&records, &schema)?;

        assert_eq!(row_seq_data.num_rows, 0);
        assert!(row_seq_data.timestamps_micros.is_empty());
        assert!(row_seq_data.columns.is_empty());

        Ok(())
    }

    #[test]
    fn test_compress_row_seq_data_type_error() -> Result<()> {
        setup_tracing();
        let schema = create_test_schema();
        let mut bad_record = Record::new();
        bad_record.insert(TIMESTAMP_COL.to_string(), Some(Value::Timestamp(1000)));
        bad_record.insert(INT_COL.to_string(), Some(Value::Float(1.23))); // Wrong type

        let records = vec![bad_record];
        let result = compression::compress_row_seq_data(&records, &schema);

        assert!(
            matches!(result, Err(BanyanRowSeqError::TypeError { column, .. }) if column == INT_COL),
            "Expected TypeError for incorrect column type"
        );

        Ok(())
    }

    #[test]
    fn test_compress_row_seq_data_missing_timestamp() -> Result<()> {
        setup_tracing();
        let schema = create_test_schema();
        let mut bad_record = Record::new();
        // Missing timestamp
        bad_record.insert(INT_COL.to_string(), Some(Value::Integer(1)));

        let records = vec![bad_record];
        let result = compression::compress_row_seq_data(&records, &schema);

        // Should fail because timestamp is mandatory and expected based on schema
        assert!(
            matches!(result, Err(BanyanRowSeqError::TypeError { column, .. }) if column == TIMESTAMP_COL),
            "Expected TypeError for missing mandatory timestamp"
        );

        Ok(())
    }

    #[test]
    fn test_compress_row_seq_data_null_timestamp_not_allowed() -> Result<()> {
        // Our current compression logic implicitly assumes timestamps are non-null
        // because it doesn't store a presence bitmap for them. Let's test this assumption.
        setup_tracing();
        let schema = create_test_schema();
        let mut record_with_null_ts = Record::new();
        record_with_null_ts.insert(TIMESTAMP_COL.to_string(), None); // Add None timestamp
        record_with_null_ts.insert(INT_COL.to_string(), Some(Value::Integer(1)));

        let records = vec![record_with_null_ts];
        let result = compression::compress_row_seq_data(&records, &schema);

        // The code currently errors if a null timestamp is encountered.
        assert!(
            matches!(result, Err(BanyanRowSeqError::TypeError { column, .. }) if column == TIMESTAMP_COL),
            "Expected TypeError because null timestamp was encountered"
        );

        // If the design changes to allow nullable timestamps, this test would need to be updated
        // to check for correct handling (e.g., bitmap presence).

        Ok(())
    }

    // ==========================================================================
    // DatastreamRowSeq Tests
    // ==========================================================================

    #[test]
    fn test_datastream_new_and_load_or_initialize_new() -> Result<()> {
        setup_tracing();
        let schema = create_test_schema();
        let schema_clone = schema.clone();
        let dir = tempdir()?;
        let persistence_path = dir.path().join("datastream_state.json");

        // Test `new`
        let store1 = memory_store(TEST_STORE_SIZE);
        let ds_new = DatastreamRowSeq::new(
            store1,
            schema.clone(),
            Config::default(),
            Secrets::default(),
            Some(&persistence_path),
        );
        assert!(ds_new.root_cid().is_none());
        assert_eq!(ds_new.total_rows(), 0);
        assert_eq!(*ds_new.schema(), schema); // Compare Arc content
        assert!(ds_new.persistence_path.is_some());

        // Test `load_or_initialize` when file doesn't exist
        let store2 = memory_store(TEST_STORE_SIZE);
        let ds_load_new = DatastreamRowSeq::load_or_initialize(
            store2,
            Config::default(),
            Secrets::default(),
            Some(&persistence_path),
            schema_clone, // Provide schema for initialization
        )?;
        assert!(ds_load_new.root_cid().is_none());
        assert_eq!(ds_load_new.total_rows(), 0);
        assert_eq!(*ds_load_new.schema(), schema);
        assert!(ds_load_new.persistence_path.is_some());
        assert!(
            !persistence_path.exists(),
            "Persistence file should not be created on init"
        ); // load_or_initialize doesn't save automatically

        Ok(())
    }

    #[test]
    fn test_datastream_extend_simple() -> Result<()> {
        setup_tracing();
        let schema = create_test_schema();
        let mut ds = create_test_datastream(schema.clone(), None)?;

        assert_eq!(ds.total_rows(), 0);
        assert!(ds.root_cid().is_none());

        // Extend once
        let records1 = generate_records(10, 0, 1000, 0.0, 1);
        ds.extend(&records1)?;
        assert_eq!(ds.total_rows(), 10);
        assert!(ds.root_cid().is_some());
        let root1 = ds.root_cid();

        // Extend again
        let records2 = generate_records(5, 10 * 1000, 1000, 0.1, 2);
        ds.extend(&records2)?;
        assert_eq!(ds.total_rows(), 15);
        assert!(ds.root_cid().is_some());
        let root2 = ds.root_cid();

        // Root should change after extend
        assert_ne!(root1, root2);

        // Extend with empty slice (should be no-op)
        ds.extend(&[])?;
        assert_eq!(ds.total_rows(), 15);
        assert_eq!(
            ds.root_cid(),
            root2,
            "Root CID should not change after empty extend"
        );

        Ok(())
    }

    #[test]
    fn test_datastream_save_and_load_state() -> Result<()> {
        setup_tracing();
        let schema = create_test_schema();
        let dir = tempdir()?;
        let persistence_path = dir.path().join("saved_state.json");

        // 1. Create, extend, and save
        let mut ds_save = create_test_datastream(schema.clone(), Some(&persistence_path))?;
        let records = generate_records(25, 0, 100, 0.0, 3);
        ds_save.extend(&records)?;
        let original_root = ds_save.root_cid();
        let original_rows = ds_save.total_rows();
        assert_eq!(original_rows, 25);
        assert!(original_root.is_some());

        ds_save.save_state()?;
        assert!(persistence_path.exists());

        // Check content briefly (optional)
        let saved_content = std::fs::read_to_string(&persistence_path)?;
        assert!(saved_content.contains(&original_root.unwrap().to_string()));
        assert!(saved_content.contains(&format!("\"total_rows\": {}", original_rows)));
        assert!(saved_content.contains(TIMESTAMP_COL)); // Check schema presence

        // 2. Load from saved state
        let store_load = memory_store(TEST_STORE_SIZE); // Use a *new* store
        let ds_load = DatastreamRowSeq::load_or_initialize(
            store_load,
            Config::default(),
            Secrets::default(),
            Some(&persistence_path),
            schema.clone(), // Provide schema for verification
        )?;

        // 3. Verify loaded state
        assert_eq!(
            ds_load.root_cid(),
            original_root,
            "Loaded root CID mismatch"
        );
        assert_eq!(
            ds_load.total_rows(),
            original_rows,
            "Loaded total_rows mismatch"
        );
        assert_eq!(*ds_load.schema(), schema, "Loaded schema mismatch"); // Compare Arc content
        assert!(ds_load.persistence_path.is_some());

        // 4. Verify we can still query the loaded state (basic check)
        let query_res = ds_load.query(
            vec![TIMESTAMP_COL.to_string()],
            (Bound::Unbounded, Bound::Unbounded),
            vec![],
            (Bound::Unbounded, Bound::Unbounded),
        )?;
        let results: Vec<Record> = query_res.collect::<Result<_, _>>()?; // Collect results
        assert_eq!(results.len(), 25);

        Ok(())
    }

    #[test]
    fn test_datastream_load_state_schema_mismatch() -> Result<()> {
        setup_tracing();
        let schema1 = create_test_schema();
        let dir = tempdir()?;
        let persistence_path = dir.path().join("mismatch_state.json");

        // 1. Save state with schema1
        let mut ds_save = create_test_datastream(schema1.clone(), Some(&persistence_path))?;
        ds_save.extend(&generate_records(5, 0, 100, 0.0, 4))?;
        ds_save.save_state()?;

        // 2. Create schema2 (different)
        let schema2 = DataDefinition::new(vec![
            ColumnDefinition {
                name: TIMESTAMP_COL.to_string(),
                column_type: ColumnType::Timestamp,
            },
            ColumnDefinition {
                name: "new_col".to_string(),
                column_type: ColumnType::Integer,
            }, // Different column
        ]);

        // 3. Attempt to load with schema2
        let store_load = memory_store(TEST_STORE_SIZE);
        let load_result = DatastreamRowSeq::load_or_initialize(
            store_load,
            Config::default(),
            Secrets::default(),
            Some(&persistence_path),
            schema2, // Required schema is different from saved
        );

        // 4. Verify SchemaMismatch error
        assert!(
            matches!(load_result, Err(BanyanRowSeqError::SchemaMismatch)),
            "Expected SchemaMismatch error"
        );

        Ok(())
    }

    // ==========================================================================
    // Query Tests
    // ==========================================================================

    /// Comprehensive setup for query tests. Creates a datastream with multiple chunks.
    fn setup_query_test_datastream(
        num_records_total: usize,
        chunk_size: usize,
    ) -> Result<(DatastreamRowSeq<TestMemStore>, Vec<Record>)> {
        let schema = create_test_schema();
        let mut ds = create_test_datastream(schema.clone(), None)?;
        let mut all_records = Vec::with_capacity(num_records_total);
        let mut current_ts = 1_000_000_000; // Start timestamp (e.g., 1000 seconds epoch)
        let ts_increment = 50_000; // 50 ms between records
        let null_prob = 0.05; // 5% nulls
        let mut seed = 100;

        for chunk_start in (0..num_records_total).step_by(chunk_size) {
            let records_in_chunk = std::cmp::min(chunk_size, num_records_total - chunk_start);
            if records_in_chunk == 0 {
                break;
            }

            let chunk_records =
                generate_records(records_in_chunk, current_ts, ts_increment, null_prob, seed);
            ds.extend(&chunk_records)?;
            all_records.extend(chunk_records.clone()); // Keep track of original records in order

            // Update start timestamp and seed for next chunk
            current_ts += (records_in_chunk as i64) * ts_increment;
            seed += 1;
            // Add a small gap between chunks to make time filtering more distinct
            current_ts += ts_increment * 10;
        }

        assert_eq!(ds.total_rows(), num_records_total as u64);
        assert_eq!(all_records.len(), num_records_total);
        Ok((ds, all_records))
    }

    #[test]
    fn test_query_empty_datastream() -> Result<()> {
        setup_tracing();
        let schema = create_test_schema();
        let ds = create_test_datastream(schema.clone(), None)?;

        let result_iter = ds.query(
            vec![TIMESTAMP_COL.to_string()],
            (Bound::Unbounded, Bound::Unbounded),
            vec![],
            (Bound::Unbounded, Bound::Unbounded),
        );

        // Querying empty stream should ideally return Ok(empty_iterator)
        assert!(
            result_iter.is_err(),
            "Querying empty stream should return InvalidQuery error"
        );
        assert!(
            matches!(result_iter.unwrap_err(), BanyanRowSeqError::InvalidQuery(_)),
            "Expected InvalidQuery error for empty stream"
        );

        // Let's try extending then querying
        let mut ds_not_empty = create_test_datastream(schema, None)?;
        ds_not_empty.extend(&generate_records(1, 0, 1, 0.0, 1))?;
        let result_iter_not_empty = ds_not_empty.query(
            vec![TIMESTAMP_COL.to_string()],
            (Bound::Unbounded, Bound::Unbounded),
            vec![],
            (Bound::Unbounded, Bound::Unbounded),
        );
        assert!(result_iter_not_empty.is_ok());
        assert_eq!(result_iter_not_empty.unwrap().count(), 1);

        Ok(())
    }

    #[test]
    fn test_query_all_data_no_filters() -> Result<()> {
        setup_tracing();
        let num_records = 150;
        let chunk_size = 40;
        let (ds, all_records) = setup_query_test_datastream(num_records, chunk_size)?;

        let result_iter = ds.query(
            ds.schema().columns.iter().map(|c| c.name.clone()).collect(), // All columns
            (Bound::Unbounded, Bound::Unbounded),                         // All offsets
            vec![],                                                       // No filters
            (Bound::Unbounded, Bound::Unbounded),                         // All times
        )?;

        let results: Vec<Record> = result_iter.collect::<Result<_, _>>()?;
        assert_records_match(&results, &all_records);

        Ok(())
    }

    #[test]
    fn test_query_column_selection() -> Result<()> {
        setup_tracing();
        let num_records = 50;
        let chunk_size = 20;
        let (ds, all_records) = setup_query_test_datastream(num_records, chunk_size)?;

        let requested_cols = vec![TIMESTAMP_COL.to_string(), STR_COL.to_string()];

        let result_iter = ds.query(
            requested_cols.clone(),
            (Bound::Unbounded, Bound::Unbounded),
            vec![],
            (Bound::Unbounded, Bound::Unbounded),
        )?;

        let results: Vec<Record> = result_iter.collect::<Result<_, _>>()?;
        assert_eq!(results.len(), num_records);

        // Create expected records with only the selected columns
        let expected_records: Vec<Record> = all_records
            .iter()
            .map(|full_rec| {
                requested_cols
                    .iter()
                    .map(|col| (col.clone(), full_rec.get(col).cloned().flatten()))
                    .collect()
            })
            .collect();

        assert_records_match(&results, &expected_records);

        Ok(())
    }

    #[test]
    fn test_query_offset_filters() -> Result<()> {
        setup_tracing();
        let num_records = 100;
        let chunk_size = 30; // Ensure multiple chunks
        let (ds, all_records) = setup_query_test_datastream(num_records, chunk_size)?;

        let test_cases = vec![
            // Range, Expected Slice Indices (inclusive start, exclusive end)
            ((Bound::Included(10), Bound::Excluded(20)), 10..20), // Simple slice
            ((Bound::Excluded(50), Bound::Included(60)), 51..61), // Excluded start, Included end
            ((Bound::Unbounded, Bound::Excluded(15)), 0..15),     // Start unbounded
            ((Bound::Included(85), Bound::Unbounded), 85..num_records), // End unbounded
            (
                (Bound::Included(0), Bound::Excluded(num_records as u64)),
                0..num_records,
            ), // Full range explicit
            ((Bound::Included(35), Bound::Excluded(35)), 35..35), // Empty range
            ((Bound::Included(100), Bound::Unbounded), 100..num_records), // Start at end
            ((Bound::Unbounded, Bound::Excluded(0)), 0..0),       // End at start
        ];

        for (offset_range, expected_indices) in test_cases {
            println!("Testing offset range: {:?}", offset_range); // Debug print
            let result_iter = ds.query(
                vec![TIMESTAMP_COL.to_string()], // Only need one col to check count/order
                offset_range,
                vec![],
                (Bound::Unbounded, Bound::Unbounded),
            )?;

            let results: Vec<Record> = result_iter.collect::<Result<_, _>>()?;
            let expected_subset = &all_records[expected_indices.clone()];
            let expected_records_subset: Vec<Record> = expected_subset
                .iter()
                .map(|full_rec| {
                    [(
                        TIMESTAMP_COL.to_string(),
                        full_rec.get(TIMESTAMP_COL).cloned().flatten(),
                    )]
                    .iter()
                    .cloned()
                    .collect()
                })
                .collect();

            assert_eq!(
                results.len(),
                expected_indices.len(),
                "Mismatch in count for range {:?}",
                offset_range
            );
            assert_records_match(&results, &expected_records_subset);
        }
        Ok(())
    }

    #[test]
    fn test_query_time_filters() -> Result<()> {
        setup_tracing();
        let num_records = 120;
        let chunk_size = 25;
        let (ds, all_records) = setup_query_test_datastream(num_records, chunk_size)?;

        // Find some specific timestamps from the generated data
        let ts_at_10 = all_records[10].get(TIMESTAMP_COL).unwrap().clone().unwrap();
        let ts_at_50 = all_records[50].get(TIMESTAMP_COL).unwrap().clone().unwrap();
        let ts_at_90 = all_records[90].get(TIMESTAMP_COL).unwrap().clone().unwrap();
        let last_ts = all_records
            .last()
            .unwrap()
            .get(TIMESTAMP_COL)
            .unwrap()
            .clone()
            .unwrap();

        let Value::Timestamp(ts10) = ts_at_10 else {
            panic!("Not a timestamp");
        };
        let Value::Timestamp(ts50) = ts_at_50 else {
            panic!("Not a timestamp");
        };
        let Value::Timestamp(ts90) = ts_at_90 else {
            panic!("Not a timestamp");
        };
        let Value::Timestamp(ts_last) = last_ts else {
            panic!("Not a timestamp");
        };

        let test_cases = vec![
            // Time Range, Expected Indices based on generated data structure
            ((Bound::Included(ts50), Bound::Excluded(ts90)), 50..90), // [ts50, ts90)
            ((Bound::Excluded(ts10), Bound::Included(ts50)), 11..51), // (ts10, ts50]
            ((Bound::Unbounded, Bound::Included(ts10)), 0..11),       // <= ts10
            ((Bound::Excluded(ts90), Bound::Unbounded), 91..num_records), // > ts90
            ((Bound::Included(ts50), Bound::Included(ts50)), 50..51), // == ts50
            (
                (Bound::Included(ts_last + 1), Bound::Unbounded),
                num_records..num_records,
            ), // After last
            (
                (
                    Bound::Unbounded,
                    Bound::Excluded(
                        all_records[0]
                            .get(TIMESTAMP_COL)
                            .unwrap()
                            .clone()
                            .unwrap()
                            .timestamp_micros()
                            .unwrap(),
                    ),
                ),
                0..0,
            ), // Before first
        ];

        for (time_range, expected_indices) in test_cases {
            println!("Testing time range: {:?}", time_range); // Debug print
            let result_iter = ds.query(
                vec![TIMESTAMP_COL.to_string()],
                (Bound::Unbounded, Bound::Unbounded),
                vec![],
                time_range,
            )?;

            let results: Vec<Record> = result_iter.collect::<Result<_, _>>()?;
            let expected_subset = &all_records[expected_indices.clone()];
            let expected_records_subset: Vec<Record> = expected_subset
                .iter()
                .map(|full_rec| {
                    [(
                        TIMESTAMP_COL.to_string(),
                        full_rec.get(TIMESTAMP_COL).cloned().flatten(),
                    )]
                    .iter()
                    .cloned()
                    .collect()
                })
                .collect();

            assert_eq!(
                results.len(),
                expected_indices.len(),
                "Mismatch in count for time range {:?}",
                time_range
            );
            assert_records_match(&results, &expected_records_subset);
        }
        Ok(())
    }

    #[test]
    fn test_query_value_filters() -> Result<()> {
        setup_tracing();
        let num_records = 60;
        let chunk_size = 20;
        // Use low null probability for easier value filtering tests
        let (ds, all_records) = setup_query_test_datastream(num_records, chunk_size)?;

        // --- Integer Filter ---
        let int_filter_val = all_records[25].get(INT_COL).unwrap().clone().unwrap(); // Get int value at index 25
        let int_filter = UserFilter {
            column_name: INT_COL.to_string(),
            operator: UserFilterOp::Equals,
            value: int_filter_val.clone(),
        };
        let expected_int_filter: Vec<Record> = all_records
            .iter()
            .filter(|r| {
                r.get(INT_COL)
                    .map_or(false, |v_opt| v_opt.as_ref() == Some(&int_filter_val))
            })
            .cloned()
            .collect();

        let result_iter_int = ds.query(
            ds.schema().columns.iter().map(|c| c.name.clone()).collect(),
            (Bound::Unbounded, Bound::Unbounded),
            vec![int_filter], // Apply filter
            (Bound::Unbounded, Bound::Unbounded),
        )?;
        let results_int: Vec<Record> = result_iter_int.collect::<Result<_, _>>()?;
        assert!(!results_int.is_empty()); // Should find at least one match
        assert_records_match(&results_int, &expected_int_filter);

        // --- Float Filter (Greater Than) ---
        let float_threshold = all_records[num_records / 2]
            .get(FLOAT_COL)
            .unwrap()
            .clone()
            .unwrap();
        let Value::Float(float_thresh_val) = float_threshold else {
            panic!("Not a float");
        };
        let float_filter = UserFilter {
            column_name: FLOAT_COL.to_string(),
            operator: UserFilterOp::GreaterThan,
            value: Value::Float(float_thresh_val),
        };
        let expected_float_filter: Vec<Record> = all_records
            .iter()
            .filter(|r| {
                r.get(FLOAT_COL).map_or(false, |v_opt| {
                    v_opt.as_ref().map_or(false, |v| {
                        v.float_val().map_or(false, |f| f > float_thresh_val)
                    })
                })
            })
            .cloned()
            .collect();

        let result_iter_float = ds.query(
            ds.schema().columns.iter().map(|c| c.name.clone()).collect(),
            (Bound::Unbounded, Bound::Unbounded),
            vec![float_filter],
            (Bound::Unbounded, Bound::Unbounded),
        )?;
        let results_float: Vec<Record> = result_iter_float.collect::<Result<_, _>>()?;
        assert!(!results_float.is_empty());
        assert_records_match(&results_float, &expected_float_filter);

        // --- String Filter (Not Equals) ---
        let string_filter_val = Value::String("record_10".to_string());
        let string_filter = UserFilter {
            column_name: STR_COL.to_string(),
            operator: UserFilterOp::NotEquals,
            value: string_filter_val.clone(),
        };
        // Expected: all records where str_val is not None AND not equal to "record_10"
        let expected_str_filter: Vec<Record> = all_records
            .iter()
            .filter(|r| {
                r.get(STR_COL).map_or(false, |v_opt| {
                    v_opt.as_ref().map_or(false, |v| v != &string_filter_val)
                })
            })
            .cloned()
            .collect();

        let result_iter_str = ds.query(
            ds.schema().columns.iter().map(|c| c.name.clone()).collect(),
            (Bound::Unbounded, Bound::Unbounded),
            vec![string_filter],
            (Bound::Unbounded, Bound::Unbounded),
        )?;
        let results_str: Vec<Record> = result_iter_str.collect::<Result<_, _>>()?;
        assert!(!results_str.is_empty());
        assert_records_match(&results_str, &expected_str_filter);

        // --- Enum Filter ---
        let enum_filter_val = Value::Enum(1); // "Beta"
        let enum_filter = UserFilter {
            column_name: ENUM_COL.to_string(),
            operator: UserFilterOp::Equals,
            value: enum_filter_val.clone(),
        };
        let expected_enum_filter: Vec<Record> = all_records
            .iter()
            .filter(|r| {
                r.get(ENUM_COL)
                    .map_or(false, |v_opt| v_opt.as_ref() == Some(&enum_filter_val))
            })
            .cloned()
            .collect();

        let result_iter_enum = ds.query(
            ds.schema().columns.iter().map(|c| c.name.clone()).collect(),
            (Bound::Unbounded, Bound::Unbounded),
            vec![enum_filter],
            (Bound::Unbounded, Bound::Unbounded),
        )?;
        let results_enum: Vec<Record> = result_iter_enum.collect::<Result<_, _>>()?;
        assert!(!results_enum.is_empty());
        assert_records_match(&results_enum, &expected_enum_filter);

        // --- Multiple Filters (AND logic) ---
        // Find records where int_val > val_at_10 AND enum_val is "Gamma" (index 2)
        let int_thresh_val = all_records[10].get(INT_COL).unwrap().clone().unwrap();
        let multi_filter1 = UserFilter {
            column_name: INT_COL.to_string(),
            operator: UserFilterOp::GreaterThan,
            value: int_thresh_val.clone(),
        };
        let multi_filter2 = UserFilter {
            column_name: ENUM_COL.to_string(),
            operator: UserFilterOp::Equals,
            value: Value::Enum(2), // Gamma
        };

        let expected_multi_filter: Vec<Record> = all_records
            .iter()
            .filter(|r| {
                let passes_int = r.get(INT_COL).map_or(false, |v_opt| {
                    v_opt.as_ref().map_or(false, |v| v > &int_thresh_val)
                });
                let passes_enum = r.get(ENUM_COL).map_or(false, |v_opt| {
                    v_opt.as_ref().map_or(false, |v| v == &Value::Enum(2))
                });
                passes_int && passes_enum
            })
            .cloned()
            .collect();

        let result_iter_multi = ds.query(
            ds.schema().columns.iter().map(|c| c.name.clone()).collect(),
            (Bound::Unbounded, Bound::Unbounded),
            vec![multi_filter1, multi_filter2], // Apply both filters
            (Bound::Unbounded, Bound::Unbounded),
        )?;
        let results_multi: Vec<Record> = result_iter_multi.collect::<Result<_, _>>()?;
        // This combination might yield empty results depending on data, which is ok
        // assert!(!results_multi.is_empty());
        assert_records_match(&results_multi, &expected_multi_filter);

        // --- Filter resulting in no matches ---
        let no_match_filter = UserFilter {
            column_name: INT_COL.to_string(),
            operator: UserFilterOp::Equals,
            value: Value::Integer(i64::MAX), // Unlikely value
        };
        let result_iter_none = ds.query(
            ds.schema().columns.iter().map(|c| c.name.clone()).collect(),
            (Bound::Unbounded, Bound::Unbounded),
            vec![no_match_filter],
            (Bound::Unbounded, Bound::Unbounded),
        )?;
        let results_none: Vec<Record> = result_iter_none.collect::<Result<_, _>>()?;
        assert!(results_none.is_empty());

        Ok(())
    }

    #[test]
    fn test_query_combined_filters() -> Result<()> {
        setup_tracing();
        let num_records = 200;
        let chunk_size = 50;
        let (ds, all_records) = setup_query_test_datastream(num_records, chunk_size)?;

        // Define ranges and filters
        let offset_range = (Bound::Included(50), Bound::Excluded(150)); // Offsets 50..150
        let time_at_75 = all_records[75]
            .get(TIMESTAMP_COL)
            .unwrap()
            .clone()
            .unwrap()
            .timestamp_micros()
            .unwrap();
        let time_at_125 = all_records[125]
            .get(TIMESTAMP_COL)
            .unwrap()
            .clone()
            .unwrap()
            .timestamp_micros()
            .unwrap();
        let time_range = (Bound::Excluded(time_at_75), Bound::Included(time_at_125)); // Time (ts75, ts125]

        let value_filter = UserFilter {
            // Enum is "Alpha" (index 0)
            column_name: ENUM_COL.to_string(),
            operator: UserFilterOp::Equals,
            value: Value::Enum(0),
        };

        // Manually filter the original records based on all criteria
        let expected_combined_filter: Vec<Record> = all_records
            .iter()
            .enumerate()
            .filter(|(i, r)| {
                // Offset check
                let offset_check = *i >= 50 && *i < 150;
                // Time check
                let ts = r
                    .get(TIMESTAMP_COL)
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .timestamp_micros()
                    .unwrap();
                let time_check = ts > time_at_75 && ts <= time_at_125;
                // Value check
                let value_check = r
                    .get(ENUM_COL)
                    .map_or(false, |v_opt| v_opt.as_ref() == Some(&Value::Enum(0)));

                offset_check && time_check && value_check
            })
            .map(|(_, r)| r.clone()) // Get the matching record
            .collect();

        // Perform the query
        let result_iter_combined = ds.query(
            ds.schema().columns.iter().map(|c| c.name.clone()).collect(),
            offset_range,
            vec![value_filter],
            time_range,
        )?;
        let results_combined: Vec<Record> = result_iter_combined.collect::<Result<_, _>>()?;

        assert_records_match(&results_combined, &expected_combined_filter);

        Ok(())
    }

    #[test]
    fn test_query_invalid_column_in_request_or_filter() -> Result<()> {
        setup_tracing();
        let num_records = 10;
        let chunk_size = 5;
        let (ds, _) = setup_query_test_datastream(num_records, chunk_size)?;

        // 1. Invalid requested column
        let requested_cols_bad = vec!["non_existent_col".to_string()];
        let result_bad_req = ds.query(
            requested_cols_bad,
            (Bound::Unbounded, Bound::Unbounded),
            vec![],
            (Bound::Unbounded, Bound::Unbounded),
        );
        // The error should occur during iterator creation (inside ds.query)
        assert!(
            result_bad_req.is_err(),
            "Expected error for non-existent requested column"
        );
        assert!(
            matches!(result_bad_req.unwrap_err(), BanyanRowSeqError::ColumnNotFound(name) if name == "non_existent_col")
        );

        // 2. Invalid filter column
        let bad_filter = UserFilter {
            column_name: "another_bad_col".to_string(),
            operator: UserFilterOp::Equals,
            value: Value::Integer(1),
        };
        let result_bad_filter = ds.query(
            vec![TIMESTAMP_COL.to_string()], // Valid request
            (Bound::Unbounded, Bound::Unbounded),
            vec![bad_filter], // Invalid filter
            (Bound::Unbounded, Bound::Unbounded),
        );
        assert!(
            result_bad_filter.is_err(),
            "Expected error for non-existent filter column"
        );
        assert!(
            matches!(result_bad_filter.unwrap_err(), BanyanRowSeqError::ColumnNotFound(name) if name == "another_bad_col")
        );

        Ok(())
    }

    // Test iterator error handling more directly (e.g., if Banyan iter returns error)
    // This requires mocking or a way to inject errors, harder to test perfectly here.
    // We rely on the fact that the iterator propagates errors from fill_buffer,
    // which in turn propagates errors from banyan_iter.next() and decompression.
    #[test]
    fn test_iterator_propagates_banyan_error() -> Result<()> {
        setup_tracing();
        let schema = create_test_schema();
        let mut ds = create_test_datastream(schema.clone(), None)?;
        ds.extend(&generate_records(10, 0, 100, 0.0, 1))?; // Add some data

        // Create a dummy Banyan iterator that returns an error
        let error_iter: Box<dyn Iterator<Item = Result<FilteredChunk<_, ()>>> + Send> =
            Box::new(std::iter::once(Err(anyhow!("Simulated Banyan Read Error"))));

        // Manually create the result iterator with the erroring Banyan source
        let result_iterator_res = iterator::RowSeqResultIterator::new(
            ds.schema(),
            error_iter,
            vec![TIMESTAMP_COL.to_string()],
            vec![],
            0,
            10,
            (Bound::Unbounded, Bound::Unbounded),
        );

        assert!(
            result_iterator_res.is_ok(),
            "Iterator creation should succeed"
        );
        let mut bad_iterator = result_iterator_res.unwrap();

        // Calling next() should trigger fill_buffer, which consumes the error
        let first_item = bad_iterator.next();

        assert!(
            first_item.is_some(),
            "Iterator should yield one item (the error)"
        );
        let item_result = first_item.unwrap();
        assert!(item_result.is_err(), "The yielded item should be an error");
        assert!(
            matches!(item_result.unwrap_err(), BanyanRowSeqError::StoreError(_)),
            "Expected StoreError containing the simulated Banyan error"
        );

        // Subsequent calls should yield None
        assert!(
            bad_iterator.next().is_none(),
            "Iterator should be exhausted after error"
        );

        Ok(())
    }
}

// Add `Value` helper methods for cleaner access in tests if needed
impl Value {
    fn timestamp_micros(&self) -> Option<i64> {
        match self {
            Value::Timestamp(ts) => Some(*ts),
            _ => None,
        }
    }
    fn integer_val(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            _ => None,
        }
    }
    fn float_val(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }
    // Add others as needed...
}

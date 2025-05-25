use anyhow::{anyhow, Result};
use banyan::{Config, Secrets};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use columnar::{
    BanyanRowSeqStore, ColumnDefinition, ColumnType, DataDefinition, DatastreamRowSeq, Record,
    UserFilter, UserFilterOp, Value,
};
use csv;
use std::{
    collections::{BTreeMap, HashMap},
    ops::Bound,
    path::Path,
    sync::Arc,
    time::Instant,
};
use tempfile::tempdir;
use tracing::{debug, error, info, info_span, trace, warn, Level}; // Added info_span
use tracing_subscriber::{fmt, EnvFilter};

// Function to initialize tracing
fn setup_logging() {
    let filter = EnvFilter::from_default_env()
        .add_directive("columnar-example=info".parse().unwrap())
        .add_directive("columnar=info".parse().unwrap());

    let _ = fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_level(true)
        .with_span_events(fmt::format::FmtSpan::CLOSE)
        .with_writer(std::io::stderr)
        .try_init();
}

// Data Definition
fn usw_data_definition_hybrid() -> DataDefinition {
    DataDefinition::new(vec![
        ColumnDefinition {
            name: "timestamp".to_string(),      // Logical name
            column_type: ColumnType::Timestamp, // Expects i64 microseconds
        },
        ColumnDefinition {
            name: "ACMH".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "ACSH".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "AWND".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "FMTM".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "GAHT".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "PGTM".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "PRCP".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "PSUN".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "SNOW".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "SNWD".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "TAVG".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "TMAX".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "TMIN".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "TSUN".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WDF1".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WDF2".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WDF5".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WDFG".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WESD".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WSF1".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WSF2".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WSF5".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WSFG".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT01".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT02".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT03".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT04".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT05".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT06".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT07".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT08".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT09".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT10".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT11".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT13".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT14".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT15".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT16".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT17".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT18".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT19".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT21".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WT22".to_string(),
            column_type: ColumnType::Float,
        },
        ColumnDefinition {
            name: "WV03".to_string(),
            column_type: ColumnType::Float,
        },
    ])
}

fn record_from_csv_hybrid(
    dd: &DataDefinition,
    header_map: &HashMap<String, usize>,
    csv_record: &csv::StringRecord,
) -> Result<Record> {
    let mut record: Record = BTreeMap::new();

    for col_def in &dd.columns {
        let logical_col_name = &col_def.name;
        let csv_header_name: &str = match logical_col_name.as_str() {
            "timestamp" => "DATE",
            _ => logical_col_name.as_str(),
        };

        let value_str_opt: Option<&str> = header_map
            .get(csv_header_name)
            .and_then(|&index| csv_record.get(index));

        trace!(column=%logical_col_name, csv_header=%csv_header_name, csv_value=?value_str_opt, "Parsing CSV value");

        let value_opt: Option<Value> = match value_str_opt {
            Some(value_str) if !value_str.is_empty() => {
                match col_def.column_type {
                    ColumnType::Timestamp => {
                        let date = NaiveDate::parse_from_str(value_str, "%Y-%m-%d")?;
                        let dt =
                            NaiveDateTime::new(date, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
                        Some(Value::Timestamp(dt.timestamp_micros())) // Store as micros
                    }
                    ColumnType::Float => match value_str.parse::<f64>() {
                        Ok(f) => Some(Value::Float(f)),
                        Err(e) => {
                            warn!(column=%logical_col_name, value=%value_str, error=%e, "Parse float error, NULL");
                            None
                        }
                    },
                    ColumnType::Integer => match value_str.parse::<i64>() {
                        Ok(i) => Some(Value::Integer(i)),
                        Err(e) => {
                            warn!(column=%logical_col_name, value=%value_str, error=%e, "Parse integer error, NULL");
                            None
                        }
                    },
                    ColumnType::String => Some(Value::String(value_str.to_string())),
                    ColumnType::Enum(_) => {
                        warn!(column=%logical_col_name, "Enum parsing not implemented, NULL");
                        None
                    }
                }
            }
            _ => None, // Missing or empty -> NULL
        };
        record.insert(logical_col_name.clone(), value_opt);
    }
    Ok(record)
}

fn usw_example_hybrid<S>(store: S, persistence_dir: &Path) -> Result<()>
where
    S: BanyanRowSeqStore,
{
    let config = Config::debug_fast();
    let secrets = Secrets::default();
    let data_def = usw_data_definition_hybrid();
    let data_def_arc = Arc::new(data_def); // Use Arc for schema

    let persistence_path = persistence_dir.join("usw_hybrid_state.json");
    info!("Using persistence path: {:?}", persistence_path);

    let mut datastream = DatastreamRowSeq::load_or_initialize(
        store.clone(),
        config,
        secrets,
        Some(&persistence_path),
        (*data_def_arc).clone(),
    )?;

    let mut initial_data_size = 0; // Variable to store initial size

    // --- Load Data only if datastream is empty ---
    if datastream.total_rows() == 0 {
        info!("Datastream state is empty. Loading data from CSV...");
        let csv_file_path = "./USW00003927.csv";
        let mut reader = csv::Reader::from_path(csv_file_path)?;
        let headers = reader.headers()?.clone();
        let header_map: HashMap<String, usize> = headers
            .iter()
            .enumerate()
            .map(|(i, h)| (h.to_string(), i))
            .collect();

        let mut records: Vec<Record> = vec![];
        let mut parsed_count = 0;
        let parse_span = tracing::info_span!("csv_parsing", library = "lib3").entered();
        for (row_idx, result) in reader.records().enumerate() {
            let string_record = result?; // Early return on CSV read error
            match record_from_csv_hybrid(&data_def_arc, &header_map, &string_record) {
                Ok(record) => {
                    records.push(record);
                    parsed_count += 1;
                }
                Err(e) => {
                    warn!(row = row_idx + 1, error = %e, "Failed to parse CSV row");
                }
            }
        }
        drop(parse_span);
        info!("Successfully parsed {} records from CSV.", parsed_count);

        if records.is_empty() {
            return Err(anyhow!("Failed to parse any records from CSV"));
        }
        initial_data_size = records.len(); // Store initial size

        info!(
            "Extending datastream with {} parsed records (batched)...",
            records.len()
        );
        let batch_size = 1000;
        for batch in records.chunks(batch_size) {
            let extend_batch_span =
                info_span!("extend_batch", size = batch.len(), library = "lib3").entered();
            datastream.extend(batch)?;
            drop(extend_batch_span);
        }
        info!(
            "Datastream extended. Total rows: {}",
            datastream.total_rows()
        );
        if let Some(cid) = datastream.root_cid() {
            println!("LIB3_CID: {}", cid);
        }
        datastream.save_state()?;
        info!("Datastream state saved.");
    } else {
        initial_data_size = datastream.total_rows() as usize; // Get size from loaded state
        info!(
            "Datastream loaded with {} existing rows.",
            initial_data_size
        );
        if let Some(cid) = datastream.root_cid() {
            println!("LIB3_CID (Loaded): {}", cid);
        }
    }

    // --- Verification Query
    // info!("Verifying total row count by iterating...");
    // let verification_span = info_span!("verification_query", library = "lib3").entered();
    // let mut verified_count = 0;
    // match datastream.query(
    //     vec!["timestamp".to_string()],        // Only need one column to count
    //     (Bound::Unbounded, Bound::Unbounded), // Full range
    //     vec![],                               // No filters
    //     (Bound::Unbounded, Bound::Unbounded), // No time filter
    // ) {
    //     Ok(iter) => {
    //         for result in iter {
    //             if result.is_ok() {
    //                 verified_count += 1;
    //             } else {
    //                 error!("Error during verification iteration: {:?}", result.err());
    //                 // Decide whether to break or continue
    //             }
    //         }
    //         info!(
    //             "Verification query finished. Counted {} rows.",
    //             verified_count
    //         );
    //     }
    //     Err(e) => {
    //         error!("Failed to execute verification query: {}", e);
    //     }
    // }
    // drop(verification_span);

    // if verified_count == datastream.total_rows() {
    //     info!(
    //         "VERIFICATION PASSED: Iterator count ({}) matches datastream total rows ({}).",
    //         verified_count,
    //         datastream.total_rows()
    //     );
    // } else {
    //     error!(
    //         "VERIFICATION FAILED: Iterator count ({}) does NOT match datastream total rows ({}).",
    //         verified_count,
    //         datastream.total_rows()
    //     );
    //     // Consider returning an error here if strict verification is needed
    //     // return Err(anyhow!("Verification failed"));
    // }
    // println!(
    //     "VERIFICATION_COUNT_VS_TOTAL: {} == {}",
    //     verified_count,
    //     datastream.total_rows()
    // );
    // --- End Verification Query ---

    // --- Filtered Query Setup
    let start_date = NaiveDate::from_ymd_opt(1975, 1, 1).unwrap();
    let start_dt = NaiveDateTime::new(start_date, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
    let end_date = NaiveDate::from_ymd_opt(1980, 1, 1).unwrap();
    let end_dt = NaiveDateTime::new(end_date, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
    let start_micros = start_dt.timestamp_micros();
    let end_micros = end_dt.timestamp_micros();

    info!(
        start_micros,
        end_micros, "Query time range (micros) [start, end)"
    );
    let prcp_filter = UserFilter {
        column_name: "PRCP".to_string(),
        operator: UserFilterOp::GreaterThan,
        value: Value::Float(500.0), // Using 500.0 to match previous results
    };
    let filters = vec![prcp_filter];
    let requested_columns = vec!["timestamp".to_string()];
    let time_range_for_query = (Bound::Included(start_micros), Bound::Excluded(end_micros));
    let offset_range = (Bound::Unbounded, Bound::Unbounded);

    info!(
        ?time_range_for_query,
        ?filters,
        "Performing Filtered Query (Lib3)..."
    );

    // --- Execute Filtered Query
    let query_span = info_span!("filtered_query_execution", library = "lib3").entered();
    let mut filtered_result_count = 0; // Renamed variable
    match datastream.query(
        requested_columns,
        offset_range,
        filters,
        time_range_for_query,
    ) {
        Ok(results_iterator) => {
            println!("--- Lib3 Filtered Query Results (Keys Only) ---");
            for record_result in results_iterator {
                match record_result {
                    Ok(result_map_with_nulls) => {
                        match result_map_with_nulls.get("timestamp") {
                            Some(Some(Value::Timestamp(micros))) => {
                                if let Some(dt) = NaiveDateTime::from_timestamp_micros(*micros) {
                                    println!("RESULT_LIB3: {}", dt.format("%Y-%m-%d"));
                                } else {
                                    println!("RESULT_LIB3: INVALID_TIMESTAMP({})", micros);
                                }
                            }
                            _ => warn!("Filtered result missing or invalid timestamp"),
                        }
                        filtered_result_count += 1;
                        trace!(?result_map_with_nulls, "Matched record");
                    }
                    Err(e) => error!("Error processing filtered query result: {}", e),
                }
            }
            println!("--- Lib3 Filtered Query Complete ---");
            info!(
                "Total matching rows found by Lib3 query: {}",
                filtered_result_count
            );
            println!("LIB3_COUNT: {}", filtered_result_count);
        }
        Err(e) => error!("Failed to execute Lib3 filtered query: {}", e),
    }
    drop(query_span);

    // --- Simulate Frequent Appends (Added) ---
    info!("Simulating 10 frequent single-row appends...");
    let final_rows_to_add = 10;
    let single_append_span = info_span!("single_appends", count = final_rows_to_add).entered();
    for i in 0..final_rows_to_add {
        let current_offset = datastream.total_rows(); // Get current total before append
        let append_date =
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(i as i64);
        let append_dt = NaiveDateTime::new(append_date, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let mut record = Record::new();
        record.insert(
            "timestamp".to_string(),
            Some(Value::Timestamp(append_dt.timestamp_micros())),
        );
        record.insert("PRCP".to_string(), Some(Value::Float(10.0 + i as f64))); // Example data
        record.insert("TMAX".to_string(), Some(Value::Float(250.0 + i as f64))); // Example data

        let append_span = info_span!("single_append", offset = current_offset).entered();
        let start_time = Instant::now();
        // Use extend with a single-element slice
        if let Err(e) = datastream.extend(&[record]) {
            error!(offset = current_offset, error = %e, "Failed single append");
            // Decide how to handle error, maybe break
        }
        let duration = start_time.elapsed();
        drop(append_span);
        debug!(
            offset = current_offset,
            ?duration,
            "Single append completed"
        ); // Debug level for timing
           // Optionally save state after each single append if desired, but likely slow:
           // if let Err(e) = datastream.save_state() { error!("Failed to save state after single append {}: {}", current_offset, e); }
    }
    drop(single_append_span);
    info!(
        "Finished simulating single appends. New total rows: {}",
        datastream.total_rows()
    );

    // Optionally save state after all single appends
    // if datastream.persistence_path.is_some() {
    //     info!("Saving final state after single appends...");
    //     if let Err(e) = datastream.save_state() {
    //         error!("Failed to save final state: {}", e);
    //     }
    // }

    // // --- Final Verification Query (Added) ---
    // info!("Verifying final total row count by iterating...");
    // let final_verification_span =
    //     info_span!("final_verification_query", library = "lib3").entered();
    // let mut final_verified_count = 0;
    // let expected_final_count = initial_data_size as u64 + final_rows_to_add as u64; // Calculate expected count
    // match datastream.query(
    //     vec!["timestamp".to_string()],
    //     (Bound::Unbounded, Bound::Unbounded),
    //     vec![],
    //     (Bound::Unbounded, Bound::Unbounded),
    // ) {
    //     Ok(iter) => {
    //         for result in iter {
    //             if result.is_ok() {
    //                 final_verified_count += 1;
    //             } else {
    //                 error!(
    //                     "Error during final verification iteration: {:?}",
    //                     result.err()
    //                 );
    //             }
    //         }
    //         info!(
    //             "Final verification query finished. Counted {} rows.",
    //             final_verified_count
    //         );
    //     }
    //     Err(e) => {
    //         error!("Failed to execute final verification query: {}", e);
    //     }
    // }
    // drop(final_verification_span);

    // if final_verified_count == expected_final_count
    //     && final_verified_count == datastream.total_rows()
    // {
    //     info!(
    //         "FINAL VERIFICATION PASSED: Iterator count ({}) matches datastream total ({}) and expected total ({}).",
    //         final_verified_count, datastream.total_rows(), expected_final_count
    //     );
    // } else {
    //     error!(
    //         "FINAL VERIFICATION FAILED: Iterator count ({}) / Datastream total ({}) / Expected total ({}). Mismatch detected!",
    //          final_verified_count, datastream.total_rows(), expected_final_count
    //     );
    // }
    // println!(
    //     "FINAL_VERIFICATION_COUNTS: {} == {} == {}",
    //     final_verified_count,
    //     datastream.total_rows(),
    //     expected_final_count
    // );
    // --- End Final Verification ---

    info!("Lib3 Example finished successfully.");
    Ok(())
}

fn main() -> Result<()> {
    setup_logging();
    use banyan::store::BlockWriter;
    use banyan_utils::{ipfs::IpfsStore, tags::Sha256Digest};

    type MemStore = banyan::store::MemStore<Sha256Digest>;

    let temp_dir = tempdir().expect("Failed to create temp directory");
    let persistence_dir = temp_dir.path();
    info!(
        "Using temp directory for persistence: {:?}",
        persistence_dir
    );

    let is_ipfs_available = IpfsStore.put(vec![]).is_ok();

    if is_ipfs_available {
        info!("IPFS available, using IpfsStore.");
        // Ensure this IpfsStore implements the BanyanRowSeqStore trait
        let store = IpfsStore {};
        usw_example_hybrid(store, persistence_dir)?;
        info!("IPFS store example finished.");
    } else {
        warn!("IPFS not available, using MemStore.");
        let store = MemStore::new(usize::MAX, Sha256Digest::digest);
        usw_example_hybrid(store, persistence_dir)?;
        info!("Memory store example finished.");
    };

    info!("Main function finished.");
    Ok(())
}

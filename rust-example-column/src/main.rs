use std::{
    collections::BTreeMap,
    path::PathBuf, // Use PathBuf for better path handling
    env, // To potentially get CWD
};
use anyhow::{anyhow, Result}; // Use anyhow for error handling

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use dclimate_banyan::{
    columnar::ColumnarDatastream, // Correct import path
    ipfs_available,
    memory_store,
    BanyanStore, // The core trait
    ColumnType::{self, Float, Timestamp},
    DataDefinition,
    IpfsStore,
    Query, // Import the user-facing Query struct
    Record,
    Value,
};
use libipld::Cid;

// Define a larger store size if needed, especially for IPFS testing
// const SIZE_64_MB: usize = 1 << 26; // 64 MiB
const SIZE_256_MB: usize = 1 << 28; // 256 MiB

// DataDefinition remains the same
fn usw_data_definition() -> DataDefinition {
    DataDefinition::from_iter(vec![
        ("DATE", Timestamp, true), // Primary key (time), indexed
        ("ACMH", Float, false),
        ("ACSH", Float, false),
        ("AWND", Float, false),
        ("FMTM", Float, false),
        ("GAHT", Float, false),
        ("PGTM", Float, false),
        ("PRCP", Float, true), // Indexed
        ("PSUN", Float, false),
        ("SNOW", Float, false),
        ("SNWD", Float, false),
        ("TAVG", Float, false),
        ("TMAX", Float, true), // Indexed
        // ("ELEVATION", Float, false), // <--- ADD THIS LINE (assuming it's a float)
        ("TMIN", Float, false),
        ("TSUN", Float, false),
        ("WDF1", Float, false),
        ("WDF2", Float, false),
        ("WDF5", Float, false),
        ("WDFG", Float, false),
        ("WESD", Float, false),
        ("WSF1", Float, false),
        ("WSF2", Float, false),
        ("WSF5", Float, false),
        ("WSFG", Float, false),
        ("WT01", Float, false),
        ("WT02", Float, false),
        ("WT03", Float, false),
        ("WT04", Float, false),
        ("WT05", Float, false),
        ("WT06", Float, false),
        ("WT07", Float, false),
        ("WT08", Float, false),
        ("WT09", Float, false),
        ("WT10", Float, false),
        ("WT11", Float, false),
        ("WT13", Float, false),
        ("WT14", Float, false),
        ("WT15", Float, false),
        ("WT16", Float, false),
        ("WT17", Float, false),
        ("WT18", Float, false),
        ("WT19", Float, false),
        ("WT21", Float, false),
        ("WT22", Float, false),
        ("WV03", Float, false),
    ])
}

// Record creation remains the same - looks good

fn record_from_csv<'dd>(
    dd: &'dd DataDefinition,
    csv_row: BTreeMap<String, String>,
) -> Result<Record<'dd>> {
    let mut record = dd.record();
    for (name, value) in csv_row {
        if value.is_empty() {
            // Skip empty fields - this is correct.
            continue;
        }

        // --- Key Change: Use `if let` to only process columns defined in `dd` ---
        if let Some(col_def) = dd.get_by_name(&name) {
            // Column 'name' exists in our DataDefinition, proceed with parsing.
            match col_def.kind {
                ColumnType::Timestamp => {
                    // Date parsing logic (handles YYYY-MM-DD)
                    let date = NaiveDate::parse_from_str(&value, "%Y%m%d") // Keep original format attempt
                        .or_else(|_| NaiveDate::parse_from_str(&value, "%Y-%m-%d")) // Handle format from data
                        .map_err(|e| anyhow!("Failed to parse date '{}' for column '{}': {}", value, name, e))?;
                    let dt = NaiveDateTime::new(date, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
                    record.insert(name.clone(), Value::from(dt));
                }
                ColumnType::Float => {
                    // Float parsing logic
                     let float_val = value
                        .parse::<f64>()
                        .map_err(|e| anyhow!("Failed to parse float '{}' for column '{}': {}", value, name, e))?;
                    record.insert(name.clone(), Value::from(float_val));
                }
                // Add other types from your DataDefinition if necessary (e.g., Integer, String)
                // ColumnType::String => {
                //     record.insert(name.clone(), Value::String(value.clone()));
                // }
                _ => {
                    // This case should ideally not be hit if your DataDefinition only uses handled types.
                    eprintln!("Warning: Unsupported column type {:?} defined for column '{}'. Skipping field.", col_def.kind, name);
                }
            }
        }
        // else: Column 'name' (e.g., "STATION", "LATITUDE") is in the CSV header
        // but *not* in our DataDefinition. We simply ignore it and move to the next field.
        // --- End Key Change ---
    }

    // Final check: Ensure the essential 'DATE' column was successfully parsed and inserted.
    if !record.values.contains_key("DATE") {
         // This could happen if the DATE column was empty or failed parsing for some reason.
         return Err(anyhow!("Record is missing the required 'DATE' column after processing known fields. Check CSV data."));
    }

    Ok(record)
}
// *** FIX 1: Add necessary trait bounds for the store ***
// Banyan's Forest and Transaction often require the store to be Clone, Send, Sync, 'static
// because they might operate concurrently or store references.
fn usw_columnar_example<S>(store: S, csv_path: PathBuf) -> Result<Cid>
where
    S: BanyanStore + Clone + Send + Sync + 'static,
{
    let dd = usw_data_definition();

    // *** FIX 2: Handle CSV Path Robustly ***
    if !csv_path.exists() {
        return Err(anyhow::anyhow!(
            "CSV file not found at path: {:?}. Current directory is: {:?}",
            csv_path,
            env::current_dir().unwrap_or_else(|_| "unknown".into())
        ));
    }
    println!("Attempting to read CSV from: {:?}", csv_path);
    let mut reader = csv::Reader::from_path(csv_path)?;

    // Use owned String headers
    let headers = reader
        .headers()?
        .iter()
        .map(String::from)
        .collect::<Vec<String>>(); // Vec<String>

    println!("Reading CSV...");
    let mut records = vec![];
    // Pre-allocate based on a guess if possible, e.g., file size / avg row size
    // records.reserve(50000); // Example: Reserve space for 50k records

    for (i, result) in reader.records().enumerate() {
        if i % 10000 == 0 && i > 0 {
            println!("... read {} records", i);
        }
        let row = result?;
        // Ensure the number of fields matches headers
        if row.len() != headers.len() {
             eprintln!("Warning: Skipping row {} due to mismatched field count (expected {}, got {})", i + 1, headers.len(), row.len());
             continue;
        }
        // Create owned BTreeMap for record_from_csv
        let record_map: BTreeMap<String, String> = headers
            .iter()
            .cloned() // Clone header strings
            .zip(row.into_iter().map(String::from))
            .collect();
        match record_from_csv(&dd, record_map) {
             Ok(rec) => records.push(rec),
             Err(e) => eprintln!("Warning: Skipping record at row {} due to error: {}", i + 1, e), // Log errors but continue
        }
    }
    println!("Finished reading {} valid records from CSV.", records.len());
    if records.is_empty() {
        return Err(anyhow::anyhow!("No valid records were read from the CSV. Cannot create datastream."));
    }

    // Use ColumnarDatastream
    // Pass the initial store clone here
    let datastream = ColumnarDatastream::new(store.clone(), dd.clone());

    println!("Extending datastream (this might take a while)...");
    let mut datastream = datastream; // Make mutable for extend loop
    let chunk_size = 10000; // Keep chunking, it's good practice
    let total_chunks = (records.len() + chunk_size - 1) / chunk_size;

    // Use an iterator approach for potentially better memory efficiency if records is huge
    // let records_iter = records.into_iter(); // Consumes records vec

    for (i, chunk) in records.chunks(chunk_size).enumerate() {
        println!(
            "Extending chunk {}/{} ({} records)...",
            i + 1,
            total_chunks,
            chunk.len()
        );
        // *** FIX 3: Pass owned data to extend ***
        // `extend` takes `IntoIterator<Item = Record<'dd>>`.
        // `chunk` is a slice `&[Record<'dd>]`. `to_vec()` clones the Records.
        // This is necessary because `extend` might store these records or process
        // them asynchronously, requiring ownership.
        datastream = datastream.extend(chunk.to_vec())?;
    }
    println!("Extension complete.");
    let cid = datastream
        .cid
        .clone()
        .ok_or_else(|| anyhow::anyhow!("Datastream CID is None after extend"))?;
    println!("Datastream Root CID: {}", cid);

    // --- Verification and Querying ---
    println!("Loading datastream for verification...");
    // Pass another store clone here
    let loaded_datastream = ColumnarDatastream::load(&cid, store.clone(), dd.clone());
    println!("Iterating through all stored records for verification...");
    let mut count = 0;
    let mut errors = 0;
    let mut first_record_verified: Option<Record> = None;
    let mut last_record_verified: Option<Record> = None;

    // Use iter_query with no filter for potentially better performance if optimized
    // let verification_iter = loaded_datastream.iter_query(dclimate_banyan::columnar::ColumnarBanyanQuery {
    //     query: None, range_start: None, range_end: None
    // })?;
    let verification_iter = loaded_datastream.iter()?; // Simple iter is fine too

    for (i, record_result) in verification_iter.enumerate() {
        if i % 10000 == 0 && i > 0 {
            println!("... verified {} records ({} errors)", i, errors);
        }
        match record_result {
            Ok(record) => {
                if i == 0 { first_record_verified = Some(record.clone()); }
                last_record_verified = Some(record); // Keep track of the last one
                count += 1;
            },
            Err(e) => {
                eprintln!("Error reading record during verification {}: {}", i, e);
                errors += 1;
            }
        }
    }
    println!(
        "Finished verification. Total records read: {}, Errors: {}",
        count, errors
    );
    if let Some(rec) = first_record_verified {
        println!("First verified record DATE: {:?}", rec.values.get("DATE"));
    }
    if let Some(rec) = last_record_verified {
         println!("Last verified record DATE: {:?}", rec.values.get("DATE"));
    }

    // Compare count with original number of *valid* records read
    assert_eq!(
        count,
        records.len(), // Use the count of successfully parsed records
        "Verification failed: Record count mismatch!"
    );
    if errors > 0 {
         eprintln!("Warning: Verification encountered {} errors.", errors);
    }


    println!("Querying datastream...");
    let ts_col = dd
        .get_by_name("DATE")
        .expect("DATE column definition should exist"); // Use expect if critical
    let start = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(1975, 1, 1).unwrap(),
        NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    );
    let end = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(1976, 1, 1).unwrap(),
        NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    );

    // *** FIX 4: Correct Query Construction using `?` ***
    // Chain `and` calls and apply `?` at the end of logical blocks.
    let ts_query = ts_col.ge(Value::Timestamp(start))?
                 .and(ts_col.lt(Value::Timestamp(end))?); // Query for [1975-01-01, 1976-01-01)

    let prcp_col = dd
        .get_by_name("PRCP")
        .expect("PRCP column definition should exist");
    let prcp_query = prcp_col.gt(Value::Float(0.0))?;

    // Combine the two parts
    let final_query = ts_query.and(prcp_query); // No ? needed here if `and` doesn't return Result

    // Pass another store clone here
    let query_datastream = ColumnarDatastream::load(&cid, store.clone(), dd.clone()); // Use dd.clone()
    println!("Querying for 1975 with PRCP > 0...");

    // Use the final combined query
    let results_iter = query_datastream.query(&final_query)?;
    let mut query_count = 0;
    let mut query_errors = 0;
    let mut first_result: Option<Record> = None;
    let mut last_result: Option<Record> = None;

    for (i, record_result) in results_iter.enumerate() {
        query_count += 1;
        match record_result {
            Ok(record) => {
                 if i == 0 { first_result = Some(record.clone()); }
                 last_result = Some(record.clone()); // Keep track of last result
                if i < 10 {
                    // Print relevant fields clearly
                    println!(
                        "  Result {}: DATE={:?}, PRCP={:?}",
                        i,
                        record.values.get("DATE").map(|v| format!("{:?}", v)), // Format Option<Value>
                        record.values.get("PRCP").map(|v| format!("{:?}", v))
                    );
                } else if i == 10 {
                    println!("  ... (omitting further results)");
                }
            }
            Err(e) => {
                eprintln!("Error reading query result {}: {}", i, e);
                query_errors += 1;
            }
        }
    }
     println!(
        "Query finished. Found {} results ({} errors).",
        query_count, query_errors
    );
     if let Some(rec) = first_result {
        println!("First query result DATE: {:?}", rec.values.get("DATE"));
    }
    if let Some(rec) = last_result {
         println!("Last query result DATE: {:?}", rec.values.get("DATE"));
    }
     if query_errors > 0 {
         eprintln!("Warning: Query encountered {} errors.", query_errors);
    }

    Ok(cid)
}

fn main() -> Result<()> {
    // Basic logging setup (optional, but helpful)
    // env_logger::init(); // Add env_logger = "0.9" to Cargo.toml if you use this

    // *** FIX 5: Determine CSV Path ***
    // Try to find the CSV relative to the Cargo manifest directory
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let mut csv_path = PathBuf::from(manifest_dir);
    // Adjust the relative path from the manifest dir (your Cargo.toml location)
    csv_path.push(".."); // Go up one level from dclimate-banyan dir
    csv_path.push("rust-example"); // Go into rust-example dir (assuming it's a sibling)
    csv_path.push("USW00003927.csv");

    // Alternatively, use a fixed path or command-line argument:
    // let csv_path = PathBuf::from("/path/to/your/USW00003927.csv");
    // let args: Vec<String> = env::args().collect();
    // let csv_path = PathBuf::from(args.get(1).expect("Please provide CSV path as argument"));

    println!("Resolved CSV path: {:?}", csv_path);


    if ipfs_available() {
        println!("IPFS available, using IpfsStore.");
        // IpfsStore doesn't need a size limit typically
        let cid = usw_columnar_example(IpfsStore, csv_path)?; // Pass store by value, IpfsStore is ()
        println!("Final Columnar Datastream Root CID (IPFS): {}", cid);
    } else {
        println!("IPFS not available, using MemStore.");
        let cid = usw_columnar_example(memory_store(SIZE_256_MB), csv_path)?; // Pass store by value
        println!("Final Columnar Datastream Root CID (Memory): {}", cid);
    };

    Ok(())
}
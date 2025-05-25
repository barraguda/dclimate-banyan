//! Data compression and decompression logic for columnar chunks.
//!
//! This module handles converting between user-facing `Record`s and the
//! compressed `RowSeqData` format stored in Banyan leaves. It uses type-specific
//! compression strategies to optimize storage size.
//!
//! **Implementation Notes:** Uses `pco` for numerical types and CBOR+Zstd for Strings.

use crate::{
    error::{BanyanRowSeqError, Result}, // Use local Result alias
    types::{
        CborRoaringBitmap, ColumnChunkData, ColumnType, DataDefinition, Record, RowSeqData, Value,
    },
};
use libipld::{
    cbor::DagCborCodec,
    codec::{Decode, Encode},
    prelude::Codec, // Import Codec trait explicitly
};
// Import pco types
use pco::{ChunkConfig, DEFAULT_COMPRESSION_LEVEL}; // Import config and default level
use roaring::RoaringBitmap;
use std::collections::BTreeMap;
use std::io::Write; // Need Write for pco compress_into
use tracing::{debug, error, trace, warn};

// --- Compression Configuration ---

/// Zstandard compression level (1-21) for non-pco types (like Strings).
const ZSTD_COMPRESSION_LEVEL: i32 = 3;

/// Default configuration for pco compression.
fn pco_config() -> ChunkConfig {
    // Start with default, adjust level as needed
    ChunkConfig::default().with_compression_level(DEFAULT_COMPRESSION_LEVEL)
}

// --- Placeholder Compression: CBOR + Zstd (Only for Strings now) ---

/// Compresses a slice of dense strings using CBOR encoding followed by Zstd.
fn compress_string_cbor_zstd(values: &[String]) -> Result<Vec<u8>> {
    if values.is_empty() {
        return Ok(Vec::new());
    }
    let cbor_bytes = DagCborCodec.encode(&values.to_vec()).map_err(|e| {
        BanyanRowSeqError::CompressionError(format!("String CBOR encode error: {}", e))
    })?;
    let compressed =
        zstd::stream::encode_all(&cbor_bytes[..], ZSTD_COMPRESSION_LEVEL).map_err(|e| {
            BanyanRowSeqError::CompressionError(format!("String Zstd encode error: {}", e))
        })?;
    trace!(
        "String CBOR+Zstd: original_dense_len={}, cbor_len={}, final_len={}",
        values.len(),
        cbor_bytes.len(),
        compressed.len()
    );
    Ok(compressed)
}

/// Decompresses string data compressed with `compress_string_cbor_zstd`.
fn decompress_string_cbor_zstd(data: &[u8]) -> Result<Vec<String>, BanyanRowSeqError> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let cbor_bytes = zstd::stream::decode_all(data).map_err(|e| {
        BanyanRowSeqError::DecompressionError(format!("String Zstd decode error: {}", e))
    })?;
    Decode::decode(DagCborCodec, &mut std::io::Cursor::new(&cbor_bytes)).map_err(|e| {
        BanyanRowSeqError::DecompressionError(format!("String CBOR decode error: {}", e))
    })
}

// --- Reconstruction Helper (Unchanged) ---

/// Reconstructs the sparse `Vec<Option<T>>` from a dense `Vec<T>` and a presence bitmap.
fn reconstruct_sparse<T: Clone>(
    dense_values: Vec<T>,
    present_bitmap: &RoaringBitmap,
    column_name_for_error: &str,
) -> Result<Vec<Option<T>>> {
    // *** Keep the existing reconstruct_sparse function exactly as it was ***
    // It's correct and independent of the underlying dense compression method.
    if present_bitmap.len() != dense_values.len() as u64 {
        return Err(BanyanRowSeqError::BitmapValueMismatch {
            column: column_name_for_error.to_string(),
            bitmap_count: present_bitmap.len(),
            value_count: dense_values.len(),
        });
    }
    let len = present_bitmap.max().map_or(0, |max_idx| max_idx + 1) as usize;
    let mut result = vec![None; len];
    let mut dense_iter = dense_values.into_iter();
    for index in present_bitmap.iter() {
        let idx_usize = index as usize;
        if idx_usize < len {
            result[idx_usize] = dense_iter.next();
        } else {
            error!(
                column = %column_name_for_error,
                bitmap_index = index,
                max_expected_index = len.saturating_sub(1),
                "Bitmap index out of bounds during reconstruction"
            );
            return Err(BanyanRowSeqError::DecompressionError(format!(
                "Corrupted data: Bitmap index {} out of bounds for column '{}' (len {})",
                index, column_name_for_error, len
            )));
        }
    }
    if dense_iter.next().is_some() {
        warn!(column = %column_name_for_error, "More dense values existed than bitmap indices during reconstruction");
    }
    Ok(result)
}

// --- Type-Specific Compression/Decompression Wrappers ---

/// Extracts dense values and builds the presence bitmap.
fn extract_dense_and_bitmap<T: Clone>(values: &[Option<T>]) -> (RoaringBitmap, Vec<T>) {
    let mut present_bitmap = RoaringBitmap::new();
    let mut dense_values: Vec<T> = Vec::with_capacity(values.len()); // Pre-allocate roughly
    for (i, v_opt) in values.iter().enumerate() {
        if let Some(v) = v_opt {
            present_bitmap.insert(i as u32);
            dense_values.push(v.clone()); // Clone only non-null values
        }
    }
    (present_bitmap, dense_values)
}

// Helper to map pco errors
fn map_pco_comp_err(e: pco::errors::PcoError) -> BanyanRowSeqError {
    BanyanRowSeqError::CompressionError(format!("Pco compress error: {}", e))
}
fn map_pco_decomp_err(e: pco::errors::PcoError) -> BanyanRowSeqError {
    BanyanRowSeqError::DecompressionError(format!("Pco decompress error: {}", e))
}

// i64 (Timestamps, Integers) - USE PCO
pub fn compress_i64_column(values: &[Option<i64>]) -> Result<(RoaringBitmap, Vec<u8>)> {
    let (present_bitmap, dense_values) = extract_dense_and_bitmap(values);
    if dense_values.is_empty() {
        return Ok((present_bitmap, Vec::new()));
    }

    // Use pco for compression
    let config = pco_config();
    let compressed_data =
        pco::standalone::simple_compress(&dense_values, &config).map_err(map_pco_comp_err)?;

    trace!(
        strategy = "Pco",
        original_options = values.len(),
        dense_len = dense_values.len(),
        final_len = compressed_data.len(),
        "Compressed i64 column with Pco"
    );
    Ok((present_bitmap, compressed_data))
}

pub fn decompress_i64_column(data: &ColumnChunkData) -> Result<Vec<Option<i64>>> {
    let present_bitmap = data.present.bitmap();
    if data.data.is_empty() {
        // If data is empty, bitmap must also be empty for consistency
        if !present_bitmap.is_empty() {
            return Err(BanyanRowSeqError::BitmapValueMismatch {
                column: "i64/timestamp".to_string(),
                bitmap_count: present_bitmap.len(),
                value_count: 0,
            });
        }
        // Need to return a Vec<Option<T>> of the correct sparse length
        let len = present_bitmap.max().map_or(0, |max_idx| max_idx + 1) as usize;
        return Ok(vec![None; len]);
    }

    // Use pco for decompression
    let dense_values =
        pco::standalone::simple_decompress::<i64>(&data.data).map_err(map_pco_decomp_err)?;

    reconstruct_sparse(dense_values, present_bitmap, "i64/timestamp")
}

// f64 (Floats) - USE PCO
pub fn compress_f64_column(values: &[Option<f64>]) -> Result<(RoaringBitmap, Vec<u8>)> {
    let (present_bitmap, dense_values) = extract_dense_and_bitmap(values);
    if dense_values.is_empty() {
        return Ok((present_bitmap, Vec::new()));
    }

    let config = pco_config();
    let compressed_data =
        pco::standalone::simple_compress(&dense_values, &config).map_err(map_pco_comp_err)?;

    trace!(
        strategy = "Pco",
        original_options = values.len(),
        dense_len = dense_values.len(),
        final_len = compressed_data.len(),
        "Compressed f64 column with Pco"
    );
    Ok((present_bitmap, compressed_data))
}

pub fn decompress_f64_column(data: &ColumnChunkData) -> Result<Vec<Option<f64>>> {
    let present_bitmap = data.present.bitmap();
    if data.data.is_empty() {
        if !present_bitmap.is_empty() {
            return Err(BanyanRowSeqError::BitmapValueMismatch {
                column: "f64/float".to_string(),
                bitmap_count: present_bitmap.len(),
                value_count: 0,
            });
        }
        let len = present_bitmap.max().map_or(0, |max_idx| max_idx + 1) as usize;
        return Ok(vec![None; len]);
    }

    let dense_values =
        pco::standalone::simple_decompress::<f64>(&data.data).map_err(map_pco_decomp_err)?;

    reconstruct_sparse(dense_values, present_bitmap, "f64/float")
}

// String - KEEP CBOR + ZSTD FOR NOW
pub fn compress_string_column(values: &[Option<String>]) -> Result<(RoaringBitmap, Vec<u8>)> {
    let (present_bitmap, dense_values) = extract_dense_and_bitmap(values);
    // Use original CBOR+Zstd for strings
    let compressed_data = compress_string_cbor_zstd(&dense_values)?;
    Ok((present_bitmap, compressed_data))
}

pub fn decompress_string_column(data: &ColumnChunkData) -> Result<Vec<Option<String>>> {
    let present_bitmap = data.present.bitmap();
    // Use original CBOR+Zstd for strings
    let dense_values = decompress_string_cbor_zstd(&data.data)?;
    reconstruct_sparse(dense_values, present_bitmap, "String")
}

// u32 (Enum Indices) - USE PCO
pub fn compress_enum_column(values: &[Option<u32>]) -> Result<(RoaringBitmap, Vec<u8>)> {
    let (present_bitmap, dense_values) = extract_dense_and_bitmap(values);
    if dense_values.is_empty() {
        return Ok((present_bitmap, Vec::new()));
    }

    let config = pco_config();
    let compressed_data =
        pco::standalone::simple_compress(&dense_values, &config).map_err(map_pco_comp_err)?;

    trace!(
        strategy = "Pco",
        original_options = values.len(),
        dense_len = dense_values.len(),
        final_len = compressed_data.len(),
        "Compressed Enum (u32) column with Pco"
    );
    Ok((present_bitmap, compressed_data))
}

pub fn decompress_enum_column(data: &ColumnChunkData) -> Result<Vec<Option<u32>>> {
    let present_bitmap = data.present.bitmap();
    if data.data.is_empty() {
        if !present_bitmap.is_empty() {
            return Err(BanyanRowSeqError::BitmapValueMismatch {
                column: "Enum".to_string(),
                bitmap_count: present_bitmap.len(),
                value_count: 0,
            });
        }
        let len = present_bitmap.max().map_or(0, |max_idx| max_idx + 1) as usize;
        return Ok(vec![None; len]);
    }

    let dense_values =
        pco::standalone::simple_decompress::<u32>(&data.data).map_err(map_pco_decomp_err)?;

    reconstruct_sparse(dense_values, present_bitmap, "Enum")
}

// --- High-Level Compression/Decompression (Unchanged Logic, Calls Updated Functions) ---

/// Compresses a batch of user-facing `Record`s into a `RowSeqData` chunk.
pub fn compress_row_seq_data(records: &[Record], schema: &DataDefinition) -> Result<RowSeqData> {
    let num_rows = records.len() as u32;
    if num_rows == 0 {
        trace!("Compressing empty batch, returning empty RowSeqData");
        return Ok(RowSeqData {
            num_rows: 0,
            timestamps_micros: Vec::new(),
            columns: BTreeMap::new(),
        });
    }

    let mut compressed_columns = BTreeMap::new();
    let mut timestamps: Vec<Option<i64>> = Vec::with_capacity(num_rows as usize);

    // --- Process Timestamp Column First (Mandatory) ---
    let ts_col_name = "timestamp"; // Standard name assumption
                                   // ... (timestamp extraction logic remains the same) ...
    match schema.get_col_def(ts_col_name) {
        Some(ts_col_def) if ts_col_def.column_type == ColumnType::Timestamp => {
            for (i, record) in records.iter().enumerate() {
                match record.get(ts_col_name).cloned().flatten() {
                    Some(Value::Timestamp(micros)) => timestamps.push(Some(micros)),
                    Some(other_val) => {
                        error!(record_index = i, column = ts_col_name, expected = "Timestamp", actual = ?other_val, "Type error in timestamp column");
                        return Err(BanyanRowSeqError::TypeError {
                            column: ts_col_name.into(),
                            expected: "Timestamp".into(),
                            actual: Some(other_val),
                        });
                    }
                    None => {
                        warn!(record_index = i, column = ts_col_name, "Null timestamp encountered, but schema assumes non-null. Treating as error.");
                        return Err(BanyanRowSeqError::TypeError {
                            column: ts_col_name.into(),
                            expected: "Non-null Timestamp".into(),
                            actual: None,
                        });
                    }
                }
            }
        }
        Some(_) => {
            error!("Column 'timestamp' exists but is not of type Timestamp");
            return Err(BanyanRowSeqError::SchemaMismatch);
        }
        None => {
            error!("Mandatory 'timestamp' column not found in schema");
            return Err(BanyanRowSeqError::ColumnNotFound(ts_col_name.into()));
        }
    }

    // Compress timestamps (uses the NEW pco version)
    let (ts_bitmap, ts_compressed_data) = compress_i64_column(&timestamps)?;
    if ts_bitmap.len() != num_rows as u64 {
        warn!(
            column = ts_col_name,
            expected = num_rows,
            actual_bitmap_len = ts_bitmap.len(),
            "Timestamp compression resulted in unexpected bitmap length"
        );
        return Err(BanyanRowSeqError::CompressionError(
            "Timestamp column processing failed (likely null value issue)".to_string(),
        ));
    }
    trace!(
        column = ts_col_name,
        compressed_size = ts_compressed_data.len(),
        "Compressed timestamp column"
    );

    // --- Process Other Columns ---
    for col_def in &schema.columns {
        if col_def.name == ts_col_name {
            continue;
        }
        let col_name = &col_def.name;
        trace!(column = %col_name, column_type = ?col_def.column_type, "Processing column for compression");

        // Extract values and compress based on type (calls NEW or OLD functions)
        let (bitmap, compressed_data) = match col_def.column_type {
            ColumnType::Timestamp => unreachable!(), // Handled above
            ColumnType::Integer => {
                let values: Result<Vec<Option<i64>>, _> = records
                    .iter()
                    .map(|r| match r.get(col_name).cloned().flatten() {
                        Some(Value::Integer(i)) => Ok(Some(i)),
                        Some(other) => Err(BanyanRowSeqError::TypeError {
                            column: col_name.clone(),
                            expected: "Integer".into(),
                            actual: Some(other),
                        }),
                        None => Ok(None),
                    })
                    .collect();
                compress_i64_column(&values?)? // Uses pco
            }
            ColumnType::Float => {
                let values: Result<Vec<Option<f64>>, _> = records
                    .iter()
                    .map(|r| match r.get(col_name).cloned().flatten() {
                        Some(Value::Float(f)) => Ok(Some(f)),
                        Some(other) => Err(BanyanRowSeqError::TypeError {
                            column: col_name.clone(),
                            expected: "Float".into(),
                            actual: Some(other),
                        }),
                        None => Ok(None),
                    })
                    .collect();
                compress_f64_column(&values?)? // Uses pco
            }
            ColumnType::String => {
                let values: Result<Vec<Option<String>>, _> = records
                    .iter()
                    .map(|r| match r.get(col_name).cloned().flatten() {
                        Some(Value::String(s)) => Ok(Some(s)),
                        Some(other) => Err(BanyanRowSeqError::TypeError {
                            column: col_name.clone(),
                            expected: "String".into(),
                            actual: Some(other),
                        }),
                        None => Ok(None),
                    })
                    .collect();
                compress_string_column(&values?)? // Uses CBOR+Zstd
            }
            ColumnType::Enum(ref variants) => {
                let values: Result<Vec<Option<u32>>, _> = records
                    .iter()
                    .map(|r| match r.get(col_name).cloned().flatten() {
                        Some(Value::Enum(idx)) => {
                            if (idx as usize) >= variants.len() {
                                Err(BanyanRowSeqError::TypeError {
                                    column: col_name.clone(),
                                    expected: format!("Valid Enum index < {}", variants.len()),
                                    actual: Some(Value::Enum(idx)),
                                })
                            } else {
                                Ok(Some(idx))
                            }
                        }
                        Some(other) => Err(BanyanRowSeqError::TypeError {
                            column: col_name.clone(),
                            expected: "Enum".into(),
                            actual: Some(other),
                        }),
                        None => Ok(None),
                    })
                    .collect();
                compress_enum_column(&values?)? // Uses pco
            }
        };

        trace!(column = %col_name, bitmap_len = bitmap.len(), compressed_size = compressed_data.len(), "Compressed column");
        compressed_columns.insert(
            col_name.clone(),
            ColumnChunkData {
                present: CborRoaringBitmap::from_bitmap(bitmap),
                data: compressed_data,
            },
        );
    } // End loop over other columns

    debug!(
        num_rows,
        num_columns = compressed_columns.len() + 1,
        "Finished compressing batch into RowSeqData"
    );
    Ok(RowSeqData {
        num_rows,
        timestamps_micros: ts_compressed_data,
        columns: compressed_columns,
    })
}

/// Decompresses a specific column from a `RowSeqData` chunk. (Unchanged Logic)
pub fn decompress_column_from_chunk(
    chunk: &RowSeqData,
    column_name: &str,
    schema: &DataDefinition,
) -> Result<Vec<Option<Value>>, BanyanRowSeqError> {
    trace!(column = %column_name, chunk_rows = chunk.num_rows, "Decompressing column from RowSeqData");

    // Handle timestamp separately
    if column_name == "timestamp" {
        let ts_chunk_data = ColumnChunkData {
            present: CborRoaringBitmap::from_bitmap((0..chunk.num_rows).collect::<RoaringBitmap>()),
            data: chunk.timestamps_micros.clone(),
        };
        // Uses NEW pco decompression
        let decompressed_opts = decompress_i64_column(&ts_chunk_data)?;
        return Ok(decompressed_opts
            .into_iter()
            .map(|opt| opt.map(Value::Timestamp))
            .collect());
    }

    // Handle other columns
    match chunk.columns.get(column_name) {
        Some(column_data) => {
            let col_def = schema
                .get_col_def(column_name)
                .ok_or_else(|| BanyanRowSeqError::ColumnNotFound(column_name.to_string()))?;

            match col_def.column_type {
                ColumnType::Timestamp => Err(BanyanRowSeqError::SchemaMismatch),
                ColumnType::Integer => decompress_i64_column(column_data) // Uses pco
                    .map(|vec| vec.into_iter().map(|opt| opt.map(Value::Integer)).collect()),
                ColumnType::Float => decompress_f64_column(column_data) // Uses pco
                    .map(|vec| vec.into_iter().map(|opt| opt.map(Value::Float)).collect()),
                ColumnType::String => decompress_string_column(column_data) // Uses CBOR+Zstd
                    .map(|vec| vec.into_iter().map(|opt| opt.map(Value::String)).collect()),
                ColumnType::Enum(_) => decompress_enum_column(column_data) // Uses pco
                    .map(|vec| vec.into_iter().map(|opt| opt.map(Value::Enum)).collect()),
            }
        }
        None => {
            trace!(
                "Column {} not found in RowSeqData chunk, returning all None",
                column_name
            );
            Ok(vec![None; chunk.num_rows as usize])
        }
    }
}

//! Data compression and decompression logic for columnar chunks.
//!
//! This module handles converting between user-facing `Record`s and the
//! compressed `RowSeqData` format stored in Banyan leaves. It uses type-specific
//! compression strategies to optimize storage size.
//!
//! **Current Implementation:** Uses a placeholder CBOR + Zstd strategy for simplicity.
//! **TODO:** Implement more advanced, type-specific compression like Delta+Varint, Gorilla, Dictionary encoding.

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
use roaring::RoaringBitmap;
use std::collections::BTreeMap; // Added HashMap
use std::io::{Read, Seek, Write}; // Added IO traits
use tracing::{debug, error, trace, warn}; // Added tracing imports

// --- Compression Configuration ---

/// Zstandard compression level (1-21, higher is slower but more compression). 3 is a good default.
const ZSTD_COMPRESSION_LEVEL: i32 = 3;

// --- Placeholder Compression: CBOR + Zstd ---
// This is applied to the *dense* vector of values (no Option<T>)

/// Compresses a slice of dense values using CBOR encoding followed by Zstd.
///
/// This is a placeholder strategy. More efficient type-specific methods should replace it.
fn compress_dense_cbor_zstd<T: Encode<DagCborCodec> + Clone>(values: &[T]) -> Result<Vec<u8>> {
    if values.is_empty() {
        return Ok(Vec::new());
    }

    // Note: `values.to_vec()` creates a potentially unnecessary copy if `values` is already a Vec.
    // Consider optimizing if performance critical. Encode directly takes a reference.
    let cbor_bytes = DagCborCodec
        .encode(&values.to_vec())
        .map_err(|e| BanyanRowSeqError::CompressionError(format!("CBOR encode error: {}", e)))?;

    let compressed = zstd::stream::encode_all(&cbor_bytes[..], ZSTD_COMPRESSION_LEVEL)
        .map_err(|e| BanyanRowSeqError::CompressionError(format!("Zstd encode error: {}", e)))?;

    trace!(
        "CBOR+Zstd: original_dense_len={}, cbor_len={}, final_len={}",
        values.len(),
        cbor_bytes.len(),
        compressed.len()
    );
    Ok(compressed)
}

/// Decompresses data compressed with `compress_dense_cbor_zstd`.
fn decompress_dense_cbor_zstd<T: Decode<DagCborCodec>>(
    data: &[u8],
) -> Result<Vec<T>, BanyanRowSeqError> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let cbor_bytes = zstd::stream::decode_all(data)
        .map_err(|e| BanyanRowSeqError::DecompressionError(format!("Zstd decode error: {}", e)))?;

    Decode::decode(DagCborCodec, &mut std::io::Cursor::new(&cbor_bytes))
        .map_err(|e| BanyanRowSeqError::DecompressionError(format!("CBOR decode error: {}", e)))
}

// --- Reconstruction Helper ---

/// Reconstructs the sparse `Vec<Option<T>>` from a dense `Vec<T>` and a presence bitmap.
//todo, doublecheck clone and refactor
fn reconstruct_sparse<T: Clone>(
    dense_values: Vec<T>,
    present_bitmap: &RoaringBitmap,
    column_name_for_error: &str,
) -> Result<Vec<Option<T>>> {
    if present_bitmap.len() != dense_values.len() as u64 {
        return Err(BanyanRowSeqError::BitmapValueMismatch {
            column: column_name_for_error.to_string(),
            bitmap_count: present_bitmap.len(),
            value_count: dense_values.len(),
        });
    }

    // Determine the total length of the original sparse vector.
    // If bitmap is empty, length is 0. Otherwise, it's max_index + 1.
    let len = present_bitmap.max().map_or(0, |max_idx| max_idx + 1) as usize;
    let mut result = vec![None; len];
    let mut dense_iter = dense_values.into_iter();

    for index in present_bitmap.iter() {
        let idx_usize = index as usize;
        if idx_usize < len {
            // This should always succeed if the bitmap length matched dense_values length.
            result[idx_usize] = dense_iter.next();
        } else {
            // This indicates a corrupted bitmap or logic error.
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
    // Ensure we consumed all dense values (sanity check)
    if dense_iter.next().is_some() {
        warn!(column = %column_name_for_error, "More dense values existed than bitmap indices during reconstruction");
        // This might indicate an issue, but we proceed based on the bitmap.
    }

    Ok(result)
}

// --- Type-Specific Compression/Decompression Wrappers ---

// i64 (Timestamps, Integers)
pub fn compress_i64_column(values: &[Option<i64>]) -> Result<(RoaringBitmap, Vec<u8>)> {
    let mut present_bitmap = RoaringBitmap::new();
    let mut dense_values: Vec<i64> = Vec::with_capacity(values.len());
    for (i, v_opt) in values.iter().enumerate() {
        if let Some(v) = v_opt {
            present_bitmap.insert(i as u32);
            dense_values.push(*v);
        }
    }
    // TODO: Replace with Delta+Varint+Zstd later
    let compressed_data = compress_dense_cbor_zstd(&dense_values)?;
    Ok((present_bitmap, compressed_data))
}

pub fn decompress_i64_column(data: &ColumnChunkData) -> Result<Vec<Option<i64>>> {
    let present_bitmap = data.present.bitmap();
    // TODO: Replace with Delta+Varint+Zstd later
    let dense_values = decompress_dense_cbor_zstd::<i64>(&data.data)?;
    reconstruct_sparse(dense_values, present_bitmap, "i64/timestamp")
}

// f64 (Floats)
pub fn compress_f64_column(values: &[Option<f64>]) -> Result<(RoaringBitmap, Vec<u8>)> {
    let mut present_bitmap = RoaringBitmap::new();
    let mut dense_values: Vec<f64> = Vec::with_capacity(values.len());
    for (i, v_opt) in values.iter().enumerate() {
        if let Some(v) = v_opt {
            present_bitmap.insert(i as u32);
            dense_values.push(*v);
        }
    }
    // TODO: Replace with Gorilla+Zstd later
    let compressed_data = compress_dense_cbor_zstd(&dense_values)?;
    Ok((present_bitmap, compressed_data))
}

pub fn decompress_f64_column(data: &ColumnChunkData) -> Result<Vec<Option<f64>>> {
    let present_bitmap = data.present.bitmap();
    // TODO: Replace with Gorilla+Zstd later
    let dense_values = decompress_dense_cbor_zstd::<f64>(&data.data)?;
    reconstruct_sparse(dense_values, present_bitmap, "f64/float")
}

// String
pub fn compress_string_column(values: &[Option<String>]) -> Result<(RoaringBitmap, Vec<u8>)> {
    let mut present_bitmap = RoaringBitmap::new();
    let mut dense_values: Vec<String> = Vec::with_capacity(values.len());
    for (i, v_opt) in values.iter().enumerate() {
        if let Some(v) = v_opt {
            present_bitmap.insert(i as u32);
            // Clone the string to store in dense_values
            dense_values.push(v.clone());
        }
    }
    // TODO: Replace with Dict+Varint+Zstd later
    let compressed_data = compress_dense_cbor_zstd(&dense_values)?;
    Ok((present_bitmap, compressed_data))
}

pub fn decompress_string_column(data: &ColumnChunkData) -> Result<Vec<Option<String>>> {
    let present_bitmap = data.present.bitmap();
    // TODO: Replace with Dict+Varint+Zstd later
    let dense_values = decompress_dense_cbor_zstd::<String>(&data.data)?;
    reconstruct_sparse(dense_values, present_bitmap, "String")
}

// u32 (Enum Indices)
pub fn compress_enum_column(values: &[Option<u32>]) -> Result<(RoaringBitmap, Vec<u8>)> {
    let mut present_bitmap = RoaringBitmap::new();
    let mut dense_values: Vec<u32> = Vec::with_capacity(values.len());
    for (i, v_opt) in values.iter().enumerate() {
        if let Some(v) = v_opt {
            present_bitmap.insert(i as u32);
            dense_values.push(*v);
        }
    }
    // TODO: Replace with Varint+Zstd later
    let compressed_data = compress_dense_cbor_zstd(&dense_values)?;
    Ok((present_bitmap, compressed_data))
}

pub fn decompress_enum_column(data: &ColumnChunkData) -> Result<Vec<Option<u32>>> {
    let present_bitmap = data.present.bitmap();
    // TODO: Replace with Varint+Zstd later
    let dense_values = decompress_dense_cbor_zstd::<u32>(&data.data)?;
    reconstruct_sparse(dense_values, present_bitmap, "Enum")
}

// --- High-Level Compression/Decompression ---

/// Compresses a batch of user-facing `Record`s into a `RowSeqData` chunk.
///
/// This function iterates through the provided records and schema, extracts
/// values for each column, compresses them using type-specific methods,
/// and packages them into the `RowSeqData` structure ready for storage
/// in a Banyan tree leaf.
///
/// # Arguments
/// * `records` - A slice of `Record`s to compress.
/// * `schema` - The `DataDefinition` describing the structure of the records.
///
/// # Returns
/// A `Result` containing the compressed `RowSeqData` or a `BanyanRowSeqError`.
///
/// # Errors
/// Returns errors for schema mismatches, type errors in records, or internal
/// compression failures.
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
                        // Current design assumes timestamps are non-null.
                        // If they can be null, this needs adjustment and RowSeqData
                        // needs to store the timestamp presence bitmap.
                        warn!(record_index = i, column = ts_col_name, "Null timestamp encountered, but schema assumes non-null. Treating as error.");
                        return Err(BanyanRowSeqError::TypeError {
                            column: ts_col_name.into(),
                            expected: "Non-null Timestamp".into(),
                            actual: None,
                        });
                        // Alternatively, handle nulls if design changes:
                        // timestamps.push(None);
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

    // Compress timestamps
    // Note: We discard the bitmap here, assuming non-null timestamps for the main field.
    // If timestamps *can* be null, RowSeqData needs modification.
    let (ts_bitmap, ts_compressed_data) = compress_i64_column(&timestamps)?;
    if ts_bitmap.len() != num_rows as u64 {
        warn!(column = ts_col_name, expected = num_rows, actual_bitmap_len = ts_bitmap.len(), "Timestamp compression resulted in unexpected bitmap length (potentially due to null handling issue)");
        // If nulls were allowed and handled above, this check might be different.
        // For now, this indicates an issue with the non-null assumption.
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
        // Skip the already processed timestamp column
        if col_def.name == ts_col_name {
            continue;
        }

        let col_name = &col_def.name;
        trace!(column = %col_name, column_type = ?col_def.column_type, "Processing column for compression");

        // Extract values and compress based on type
        let (bitmap, compressed_data) = match col_def.column_type {
            ColumnType::Timestamp => unreachable!("Timestamp handled separately"),
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
                compress_i64_column(&values?)?
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
                compress_f64_column(&values?)?
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
                compress_string_column(&values?)?
            }
            ColumnType::Enum(ref variants) => {
                // Use variants for potential validation?
                let values: Result<Vec<Option<u32>>, _> = records
                    .iter()
                    .map(|r| match r.get(col_name).cloned().flatten() {
                        Some(Value::Enum(idx)) => {
                            // Optional: Validate idx against variants.len() here?
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
                compress_enum_column(&values?)?
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
    ); // +1 for timestamp
    Ok(RowSeqData {
        num_rows,
        timestamps_micros: ts_compressed_data,
        columns: compressed_columns,
    })
}

/// Decompresses a specific column from a `RowSeqData` chunk back into user-facing values.
///
/// Retrieves the compressed data for the specified column, decompresses it according
/// to its type (derived from the schema), and reconstructs the `Vec<Option<Value>>`
/// including nulls based on the presence bitmap.
///
/// # Arguments
/// * `chunk` - The `RowSeqData` chunk containing the compressed data.
/// * `column_name` - The name of the column to decompress.
/// * `schema` - The `DataDefinition` needed to determine the column's type.
///
/// # Returns
/// A `Result` containing the decompressed `Vec<Option<Value>>` for the column,
/// matching the `chunk.num_rows` length, or a `BanyanRowSeqError`. If the column
/// was not present in the chunk (assumed all null), returns a Vec of `None`s.
///
/// # Errors
/// Returns errors for column not found in schema, decompression failures, or
/// inconsistencies (e.g., bitmap/value count mismatch).
pub fn decompress_column_from_chunk(
    chunk: &RowSeqData,
    column_name: &str,
    schema: &DataDefinition,
) -> Result<Vec<Option<Value>>, BanyanRowSeqError> {
    trace!(column = %column_name, chunk_rows = chunk.num_rows, "Decompressing column from RowSeqData");

    // Handle timestamp separately
    if column_name == "timestamp" {
        // Synthesize ColumnChunkData assuming timestamps were dense (or store their bitmap)
        let ts_chunk_data = ColumnChunkData {
            present: CborRoaringBitmap::from_bitmap(
                (0..chunk.num_rows).collect::<RoaringBitmap>(), // Assumes all present
            ),
            data: chunk.timestamps_micros.clone(),
        };
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
                ColumnType::Timestamp => Err(BanyanRowSeqError::SchemaMismatch), // Handled above
                ColumnType::Integer => decompress_i64_column(column_data)
                    .map(|vec| vec.into_iter().map(|opt| opt.map(Value::Integer)).collect()),
                ColumnType::Float => decompress_f64_column(column_data)
                    .map(|vec| vec.into_iter().map(|opt| opt.map(Value::Float)).collect()),
                ColumnType::String => decompress_string_column(column_data)
                    .map(|vec| vec.into_iter().map(|opt| opt.map(Value::String)).collect()),
                ColumnType::Enum(_) => decompress_enum_column(column_data)
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

// --- START OF COMMENTED OUT ADVANCED COMPRESSION CODE ---
/*
mod advanced_compression {
    use super::*; // Import from outer compression module
    use crate::error::BanyanRowSeqError;
    use crate::types::{CborRoaringBitmap, ColumnChunkData, Value};
    use libipld::cbor::DagCborCodec;
    use libipld::prelude::{Codec, Decode, Encode};
    use roaring::RoaringBitmap;
    use std::collections::HashMap;
    use std::io::{Read, Seek, Write};
    use tracing::trace;
    // Add these to Cargo.toml if uncommenting:
    // use vint;
    // use gorilla;

    // --- Type-Specific Compression/Decompression Helpers ---

    // i64 (Timestamps, Integers): Delta + Signed Varint + Zstd
    pub fn compress_i64_delta_varint_zstd(
        values: &[Option<i64>],
    ) -> Result<(RoaringBitmap, Vec<u8>), BanyanRowSeqError> {
        let mut present_bitmap = RoaringBitmap::new();
        let mut dense_values: Vec<i64> = Vec::with_capacity(values.len());
        for (i, v_opt) in values.iter().enumerate() {
            if let Some(v) = v_opt {
                present_bitmap.insert(i as u32);
                dense_values.push(*v);
            }
        }

        if dense_values.is_empty() {
            return Ok((present_bitmap, Vec::new()));
        }

        let mut delta_encoded: Vec<u8> = Vec::with_capacity(dense_values.len() * 2); // Estimate
        let mut last_value = dense_values[0];
        vint::encode_signed(&mut delta_encoded, last_value).map_err(|e| {
            BanyanRowSeqError::CompressionError(format!("Varint encode error (first): {}", e))
        })?;

        for current_value in dense_values.iter().skip(1) {
            let delta = current_value.wrapping_sub(last_value);
            vint::encode_signed(&mut delta_encoded, delta).map_err(|e| {
                BanyanRowSeqError::CompressionError(format!("Varint encode error (delta): {}", e))
            })?;
            last_value = *current_value;
        }

        let compressed = zstd::stream::encode_all(&delta_encoded[..], super::ZSTD_COMPRESSION_LEVEL)
            .map_err(|e| BanyanRowSeqError::CompressionError(format!("Zstd encode error: {}", e)))?;

        trace!(
            strategy = "Delta+Varint+Zstd",
            original_options = values.len(),
            dense_len = dense_values.len(),
            delta_varint_len = delta_encoded.len(),
            final_len = compressed.len(),
            "Compressed i64 column"
        );
        Ok((present_bitmap, compressed))
    }

    pub fn decompress_i64_delta_varint_zstd(
        data: &ColumnChunkData,
    ) -> Result<Vec<Option<i64>>, BanyanRowSeqError> {
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
            // Need to return a Vec<Option<i64>> of the correct sparse length
            let len = present_bitmap.max().map_or(0, |max_idx| max_idx + 1) as usize;
            return Ok(vec![None; len]);
        }

        let delta_encoded = zstd::stream::decode_all(&data.data[..]).map_err(|e| {
            BanyanRowSeqError::DecompressionError(format!("Zstd decode error: {}", e))
        })?;

        let mut reader = &delta_encoded[..];
        let mut dense_values: Vec<i64> = Vec::with_capacity(present_bitmap.len() as usize);

        // Check if decoded data is unexpectedly empty
        if reader.is_empty() && !present_bitmap.is_empty() {
             return Err(BanyanRowSeqError::DecompressionError(
                 "Got empty decompressed varint data but bitmap indicates values exist".into(),
             ));
        }

        if !reader.is_empty() {
            // Decode the first value
            let mut current_value = vint::decode_signed(&mut reader).map_err(|e| {
                BanyanRowSeqError::DecompressionError(format!("Varint decode error (first): {}", e))
            })?;
            dense_values.push(current_value);

            // Decode subsequent deltas
            while !reader.is_empty() {
                let delta = vint::decode_signed(&mut reader).map_err(|e| {
                    BanyanRowSeqError::DecompressionError(format!(
                        "Varint decode error (delta): {}",
                        e
                    ))
                })?;
                // Apply delta using wrapping arithmetic to match encoding
                current_value = current_value.wrapping_add(delta);
                dense_values.push(current_value);
            }
        }
        // After decoding all, reconstruct the sparse vector
        super::reconstruct_sparse(dense_values, present_bitmap, "i64/timestamp")
    }

    // f64 (Floats): Gorilla + Zstd
    pub fn compress_f64_gorilla_zstd(
        values: &[Option<f64>],
    ) -> Result<(RoaringBitmap, Vec<u8>), BanyanRowSeqError> {
        let mut present_bitmap = RoaringBitmap::new();
        let mut dense_values: Vec<f64> = Vec::with_capacity(values.len());
        for (i, v_opt) in values.iter().enumerate() {
            if let Some(v) = v_opt {
                present_bitmap.insert(i as u32);
                dense_values.push(*v);
            }
        }

        if dense_values.is_empty() {
            return Ok((present_bitmap, Vec::new()));
        }

        // Gorilla compression requires a mutable slice buffer.
        // Estimate size: 8 bytes/value worst case + overhead, but often much less.
        let mut buffer = Vec::with_capacity(dense_values.len() * 8 + 128); // Generous estimate
        let mut encoder = gorilla::Compressor::new(0, 0); // Initial time/value don't matter? Check docs.

        // According to gorilla docs example, use `encode_all` or push values
        for &value in &dense_values {
             encoder.push(0, value.to_bits()); // Using value.to_bits() as u64
        }
        // Write the compressed data to the buffer
        encoder.encode(&mut buffer); // Check error handling if gorilla provides it
        let gorilla_bytes = buffer;


        let compressed = zstd::stream::encode_all(&gorilla_bytes[..], super::ZSTD_COMPRESSION_LEVEL)
            .map_err(|e| BanyanRowSeqError::CompressionError(format!("Zstd encode error: {}", e)))?;

        trace!(
            strategy = "Gorilla+Zstd",
            original_options=values.len(),
            dense_len=dense_values.len(),
            gorilla_len=gorilla_bytes.len(),
            final_len=compressed.len(),
            "Compressed f64 column"
        );
        Ok((present_bitmap, compressed))
    }

     pub fn decompress_f64_gorilla_zstd(
         data: &ColumnChunkData,
     ) -> Result<Vec<Option<f64>>, BanyanRowSeqError> {
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

         let gorilla_bytes = zstd::stream::decode_all(&data.data[..]).map_err(|e| {
             BanyanRowSeqError::DecompressionError(format!("Zstd decode error: {}", e))
         })?;

        // Use gorilla's `StreamingDecoder` or `decode`
        let mut decoder = gorilla::Decompressor::new(&gorilla_bytes);
        let mut dense_values = Vec::with_capacity(present_bitmap.len() as usize);

        // Iterate through decompressed pairs (time, value_bits)
        // We only care about value_bits here
        while let Some(result) = decoder.next() {
             match result {
                 Ok((_time, value_bits)) => dense_values.push(f64::from_bits(value_bits)),
                 Err(e) => {
                      return Err(BanyanRowSeqError::DecompressionError(format!(
                         "Gorilla decode error: {}",
                         e // Assuming gorilla::Error implements Display
                     )))
                 }
             }
        }

         // After decoding all, reconstruct the sparse vector
         super::reconstruct_sparse(dense_values, present_bitmap, "f64/float")
     }

    // String: Dictionary + Varint Indices + Zstd
    fn serialize_dictionary(dict: &[String]) -> Result<Vec<u8>, BanyanRowSeqError> {
        // Simple CBOR list encoding for the dictionary Vec<String>
        DagCborCodec.encode(dict).map_err(|e| {
            BanyanRowSeqError::CompressionError(format!("Dict CBOR encode error: {}", e))
        })
    }

    fn deserialize_dictionary(data: &[u8]) -> Result<Vec<String>, BanyanRowSeqError> {
        let mut cursor = std::io::Cursor::new(data);
        Decode::decode(DagCborCodec, &mut cursor).map_err(|e| {
            BanyanRowSeqError::DecompressionError(format!("Dict CBOR decode error: {}", e))
        })
    }

    pub fn compress_string_dict_varint_zstd(
        values: &[Option<String>],
    ) -> Result<(RoaringBitmap, Vec<u8>), BanyanRowSeqError> {
        let mut present_bitmap = RoaringBitmap::new();
        let mut dense_strings: Vec<&str> = Vec::with_capacity(values.len()); // Use refs initially
        for (i, v_opt) in values.iter().enumerate() {
            if let Some(v) = v_opt {
                present_bitmap.insert(i as u32);
                dense_strings.push(v); // Push ref
            }
        }

        if dense_strings.is_empty() {
            return Ok((present_bitmap, Vec::new()));
        }

        let mut dictionary_map: HashMap<&str, u32> = HashMap::new();
        let mut dictionary_vec: Vec<String> = Vec::new();
        let mut indices: Vec<u32> = Vec::with_capacity(dense_strings.len());

        // Build dictionary and indices
        for s_ref in dense_strings {
            let index = *dictionary_map.entry(s_ref).or_insert_with(|| {
                let new_index = dictionary_vec.len() as u32;
                dictionary_vec.push(s_ref.to_string()); // Store owned string in vec
                new_index
            });
            indices.push(index);
        }

        let dict_bytes = serialize_dictionary(&dictionary_vec)?;

        // Encode indices using unsigned varint
        let mut indices_bytes = Vec::new();
        for index in indices {
            // Use unsigned vint::encode
            vint::encode(&mut indices_bytes, index as u64).map_err(|e| {
                BanyanRowSeqError::CompressionError(format!("Varint encode error (indices): {}", e))
            })?;
        }

        // Combine: 4 bytes for dict length + dict bytes + indices bytes
        let mut combined_uncompressed = Vec::with_capacity(4 + dict_bytes.len() + indices_bytes.len());
        combined_uncompressed.write_all(&(dict_bytes.len() as u32).to_be_bytes()).unwrap(); // Length prefix
        combined_uncompressed.write_all(&dict_bytes).unwrap();
        combined_uncompressed.write_all(&indices_bytes).unwrap();

        // Compress the combined data
        let compressed = zstd::stream::encode_all(&combined_uncompressed[..], super::ZSTD_COMPRESSION_LEVEL)
            .map_err(|e| BanyanRowSeqError::CompressionError(format!("Zstd encode error: {}", e)))?;

        trace!(
            strategy = "Dict+Varint+Zstd",
            original_options=values.len(),
            dense_len=indices.len(),
            dict_size=dictionary_vec.len(),
            dict_bytes=dict_bytes.len(),
            indices_bytes=indices_bytes.len(),
            final_len=compressed.len(),
            "Compressed String column"
        );
        Ok((present_bitmap, compressed))
    }

     pub fn decompress_string_dict_varint_zstd(
         data: &ColumnChunkData,
     ) -> Result<Vec<Option<String>>, BanyanRowSeqError> {
         let present_bitmap = data.present.bitmap();
         if data.data.is_empty() {
              if !present_bitmap.is_empty() {
                 return Err(BanyanRowSeqError::BitmapValueMismatch {
                     column: "String".to_string(),
                     bitmap_count: present_bitmap.len(),
                     value_count: 0,
                 });
             }
             let len = present_bitmap.max().map_or(0, |max_idx| max_idx + 1) as usize;
             return Ok(vec![None; len]);
         }

         let combined_uncompressed = zstd::stream::decode_all(&data.data[..]).map_err(|e| {
             BanyanRowSeqError::DecompressionError(format!("Zstd decode error: {}", e))
         })?;
         let mut reader = &combined_uncompressed[..];

         // Read dictionary length prefix
         if reader.len() < 4 {
             return Err(BanyanRowSeqError::DecompressionError(
                 "String data too short for dictionary length prefix".into(),
             ));
         }
         let dict_len = u32::from_be_bytes(reader[0..4].try_into().unwrap()) as usize;
         reader = &reader[4..]; // Advance reader past the length

         // Read dictionary bytes
         if reader.len() < dict_len {
             return Err(BanyanRowSeqError::DecompressionError(format!(
                 "String data too short for dictionary (expected {} bytes, got {})",
                 dict_len, reader.len()
             )));
         }
         let dictionary = deserialize_dictionary(&reader[0..dict_len])?;
         reader = &reader[dict_len..]; // Advance reader past the dictionary

         // Read varint indices
         let mut dense_values = Vec::with_capacity(present_bitmap.len() as usize);
         while !reader.is_empty() {
             // Use unsigned vint::decode
             let index = vint::decode(&mut reader).map_err(|e| {
                 BanyanRowSeqError::DecompressionError(format!("Varint decode error (indices): {}", e))
             })? as usize; // Cast to usize

             if index < dictionary.len() {
                 // Clone the string from the dictionary
                 dense_values.push(dictionary[index].clone());
             } else {
                 return Err(BanyanRowSeqError::DecompressionError(format!(
                     "Invalid dictionary index {} found (dict size {})",
                     index, dictionary.len()
                 )));
             }
         }

         // After decoding all, reconstruct the sparse vector
         super::reconstruct_sparse(dense_values, present_bitmap, "String")
     }


    // u32 (Enum Indices): Unsigned Varint + Zstd
    pub fn compress_enum_varint_zstd(
        values: &[Option<u32>],
    ) -> Result<(RoaringBitmap, Vec<u8>), BanyanRowSeqError> {
        let mut present_bitmap = RoaringBitmap::new();
        let mut dense_values: Vec<u32> = Vec::with_capacity(values.len());
        for (i, v_opt) in values.iter().enumerate() {
            if let Some(v) = v_opt {
                present_bitmap.insert(i as u32);
                dense_values.push(*v);
            }
        }

        if dense_values.is_empty() {
            return Ok((present_bitmap, Vec::new()));
        }

        // Encode using unsigned varint
        let mut varint_encoded: Vec<u8> = Vec::with_capacity(dense_values.len()); // Rough estimate
        for &value in &dense_values {
            vint::encode(&mut varint_encoded, value as u64).map_err(|e| {
                BanyanRowSeqError::CompressionError(format!("Varint encode error (enum): {}", e))
            })?;
        }

        let compressed = zstd::stream::encode_all(&varint_encoded[..], super::ZSTD_COMPRESSION_LEVEL)
            .map_err(|e| BanyanRowSeqError::CompressionError(format!("Zstd encode error: {}", e)))?;

        trace!(
            strategy = "Varint+Zstd",
            original_options=values.len(),
            dense_len=dense_values.len(),
            varint_len=varint_encoded.len(),
            final_len=compressed.len(),
            "Compressed Enum column"
        );
        Ok((present_bitmap, compressed))
    }

     pub fn decompress_enum_varint_zstd(
         data: &ColumnChunkData,
     ) -> Result<Vec<Option<u32>>, BanyanRowSeqError> {
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

         let varint_encoded = zstd::stream::decode_all(&data.data[..]).map_err(|e| {
             BanyanRowSeqError::DecompressionError(format!("Zstd decode error: {}", e))
         })?;

         let mut reader = &varint_encoded[..];
         let mut dense_values = Vec::with_capacity(present_bitmap.len() as usize);
         while !reader.is_empty() {
            // Use unsigned vint::decode
             let value = vint::decode(&mut reader).map_err(|e| {
                 BanyanRowSeqError::DecompressionError(format!("Varint decode error (enum): {}", e))
             })? as u32; // Cast to u32
             dense_values.push(value);
         }

         // After decoding all, reconstruct the sparse vector
         super::reconstruct_sparse(dense_values, present_bitmap, "Enum")
     }

} // End of advanced_compression mod
 */

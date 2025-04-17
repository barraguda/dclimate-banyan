/// --- Compression Logic ---
/// NOTE: These are still placeholder implementations using basic CBOR + Zstd.
/// Replace with proper columnar compression algorithms (delta, dictionary, RLE, Gorilla, etc.)
/// for performance and efficiency in a real library.
///
use crate::types::{CborRoaringBitmap, ColumnChunk, ColumnType, ColumnarError, Value};

use anyhow::Result;
use libipld::{cbor::DagCborCodec, codec::Decode, prelude::Codec};
use roaring::RoaringBitmap;

use tracing::{error, trace};
use zstd::stream::{copy_encode, decode_all};

// Placeholder: CBOR + Zstd for i64
fn compress_i64_zstd(values: &[i64]) -> Result<Vec<u8>> {
    let cbor_bytes = DagCborCodec.encode(&values.to_vec())?;
    let mut compressed = Vec::new();
    copy_encode(&cbor_bytes[..], &mut compressed, 3)?; // Level 3 compression
    Ok(compressed)
}

fn decompress_i64_zstd(data: &[u8]) -> Result<Vec<i64>> {
    let cbor_bytes = decode_all(data)?;
    let values: Vec<i64> = Decode::decode(DagCborCodec, &mut std::io::Cursor::new(&cbor_bytes))?;
    Ok(values)
}

// Placeholder: CBOR + Zstd for f64
fn compress_f64_zstd(values: &[f64]) -> Result<Vec<u8>> {
    // DagCborCodec needs Vec<f64>, not &[f64] directly
    let cbor_bytes = DagCborCodec.encode(&values.to_vec())?;
    let mut compressed = Vec::new();
    copy_encode(&cbor_bytes[..], &mut compressed, 3)?;
    Ok(compressed)
}

fn decompress_f64_zstd(data: &[u8]) -> Result<Vec<f64>> {
    let cbor_bytes = decode_all(data)?;
    let values: Vec<f64> = Decode::decode(DagCborCodec, &mut std::io::Cursor::new(&cbor_bytes))?;
    Ok(values)
}

// Placeholder: CBOR + Zstd for String
fn compress_string_zstd(values: &[String]) -> Result<Vec<u8>> {
    // DagCborCodec needs Vec<String>, not &[String] directly
    let cbor_bytes = DagCborCodec.encode(&values.to_vec())?;
    let mut compressed = Vec::new();
    copy_encode(&cbor_bytes[..], &mut compressed, 3)?;
    Ok(compressed)
}

fn decompress_string_zstd(data: &[u8]) -> Result<Vec<String>> {
    let cbor_bytes = decode_all(data)?;
    let values: Vec<String> = Decode::decode(DagCborCodec, &mut std::io::Cursor::new(&cbor_bytes))?;
    Ok(values)
}

// Placeholder: CBOR + Zstd for u32 (Enum indices)
fn compress_enum_zstd(values: &[u32]) -> Result<Vec<u8>> {
    // DagCborCodec needs Vec<u32>, not &[u32] directly
    let cbor_bytes = DagCborCodec.encode(&values.to_vec())?;
    let mut compressed = Vec::new();
    copy_encode(&cbor_bytes[..], &mut compressed, 3)?;
    Ok(compressed)
}

fn decompress_enum_zstd(data: &[u8]) -> Result<Vec<u32>> {
    let cbor_bytes = decode_all(data)?;
    let values: Vec<u32> = Decode::decode(DagCborCodec, &mut std::io::Cursor::new(&cbor_bytes))?;
    Ok(values)
}

// Compresses a slice of Option<Value> for a given column type
pub fn compress_column(
    values: &[Option<Value>],
    col_type: &ColumnType,
) -> Result<(CborRoaringBitmap, Vec<u8>)> {
    let mut present_bitmap = RoaringBitmap::new();
    let mut dense_values: Vec<Value> = Vec::with_capacity(values.len());

    for (i, val_opt) in values.iter().enumerate() {
        if let Some(val) = val_opt {
            present_bitmap.insert(i as u32);
            dense_values.push(val.clone()); // Clone only non-null values
        }
    }
    trace!(
        "Compressed column: bitmap len {}, dense values len {}",
        present_bitmap.len(),
        dense_values.len()
    );

    let compressed_data = match col_type {
        ColumnType::Timestamp => {
            let dense_i64s: Vec<i64> = dense_values
                .into_iter()
                .map(|v| match v {
                    Value::Timestamp(ts) => Ok(ts),
                    _ => Err(ColumnarError::TypeError {
                        expected: "Timestamp".to_string(),
                        actual: format!("{:?}", v),
                    }),
                })
                .collect::<Result<_, _>>()?;
            // *** Replace with delta + zstd or similar ***
            compress_i64_zstd(&dense_i64s)?
        }
        ColumnType::Integer => {
            let dense_i64s: Vec<i64> = dense_values
                .into_iter()
                .map(|v| match v {
                    Value::Integer(i) => Ok(i),
                    _ => Err(ColumnarError::TypeError {
                        expected: "Integer".to_string(),
                        actual: format!("{:?}", v),
                    }),
                })
                .collect::<Result<_, _>>()?;
            // *** Replace with delta + zstd or similar ***
            compress_i64_zstd(&dense_i64s)?
        }
        ColumnType::Float => {
            let dense_f64s: Vec<f64> = dense_values
                .into_iter()
                .map(|v| match v {
                    Value::Float(f) => Ok(f),
                    _ => Err(ColumnarError::TypeError {
                        expected: "Float".to_string(),
                        actual: format!("{:?}", v),
                    }),
                })
                .collect::<Result<_, _>>()?;
            // *** Replace with Gorilla + zstd or similar ***
            compress_f64_zstd(&dense_f64s)?
        }
        ColumnType::String => {
            let dense_strings: Vec<String> = dense_values
                .into_iter()
                .map(|v| match v {
                    Value::String(s) => Ok(s),
                    _ => Err(ColumnarError::TypeError {
                        expected: "String".to_string(),
                        actual: format!("{:?}", v),
                    }),
                })
                .collect::<Result<_, _>>()?;
            // *** Replace with dictionary + zstd or similar ***
            compress_string_zstd(&dense_strings)?
        }
        ColumnType::Enum(_) => {
            let dense_enums: Vec<u32> = dense_values
                .into_iter()
                .map(|v| match v {
                    Value::Enum(idx) => Ok(idx),
                    _ => Err(ColumnarError::TypeError {
                        expected: "Enum".to_string(),
                        actual: format!("{:?}", v),
                    }),
                })
                .collect::<Result<_, _>>()?;
            // *** Replace with varint + zstd or similar ***
            compress_enum_zstd(&dense_enums)?
        }
    };

    // Wrap the bitmap here
    Ok((
        CborRoaringBitmap::from_bitmap(present_bitmap),
        compressed_data,
    ))
}

// Decompresses a ColumnChunk back into Vec<Option<Value>>
pub fn decompress_column(chunk: &ColumnChunk) -> Result<Vec<Option<Value>>> {
    trace!("Decompressing column chunk: {:?}", chunk);
    let (present_cbor_bitmap, data, value_constructor): (
        &CborRoaringBitmap,
        &[u8],
        Box<dyn Fn(Value) -> Value>, // Placeholder type constructor
    ) = match chunk {
        ColumnChunk::Timestamp { present, data } => (present, data, Box::new(|v| v)), // Assuming Value::Timestamp
        ColumnChunk::Integer { present, data } => (present, data, Box::new(|v| v)), // Assuming Value::Integer
        ColumnChunk::Float { present, data } => (present, data, Box::new(|v| v)), // Assuming Value::Float
        ColumnChunk::String { present, data } => (present, data, Box::new(|v| v)), // Assuming Value::String
        ColumnChunk::Enum { present, data } => (present, data, Box::new(|v| v)), // Assuming Value::Enum
    };

    let present_bitmap = present_cbor_bitmap.bitmap(); // Get the actual bitmap

    let dense_values: Vec<Value> = match chunk {
        ColumnChunk::Timestamp { .. } => decompress_i64_zstd(data)?
            .into_iter()
            .map(Value::Timestamp)
            .collect(),
        ColumnChunk::Integer { .. } => decompress_i64_zstd(data)?
            .into_iter()
            .map(Value::Integer)
            .collect(),
        ColumnChunk::Float { .. } => decompress_f64_zstd(data)?
            .into_iter()
            .map(Value::Float)
            .collect(),
        ColumnChunk::String { .. } => decompress_string_zstd(data)?
            .into_iter()
            .map(Value::String)
            .collect(),
        ColumnChunk::Enum { .. } => decompress_enum_zstd(data)?
            .into_iter()
            .map(Value::Enum)
            .collect(),
    };

    // Determine the logical size of the chunk (number of rows it represents)
    // This is crucial. The size isn't just the number of present values.
    // It should correspond to the `count` field in the `RichRangeKey` that
    // this chunk was stored with. Since we don't have the key here directly,
    // we rely on the bitmap's perspective. The highest set bit's index + 1
    // gives the minimum length needed to contain all present values.
    // If the bitmap is empty, the chunk logically represented 0 rows *that were present*.
    // A better approach might be to store the original `count` alongside the chunk data,
    // but for now, we use the bitmap max.
    let chunk_len = present_bitmap.max().map_or(0, |max_idx| max_idx + 1) as usize;
    trace!(
        "  Bitmap max: {:?}, inferred chunk len: {}",
        present_bitmap.max(),
        chunk_len
    );

    let mut result = vec![None; chunk_len]; // Initialize with Nones
    let mut dense_iter = dense_values.into_iter();

    // Iterate through the bitmap's set bits (indices of present values)
    for present_index in present_bitmap.iter() {
        if (present_index as usize) < chunk_len {
            // Fetch the next corresponding value from the dense iterator
            if let Some(value) = dense_iter.next() {
                // Apply the correct type constructor (though currently placeholders)
                result[present_index as usize] = Some(value_constructor(value));
            } else {
                // This indicates a mismatch: more bits set in bitmap than values found
                error!("Decompression mismatch: Bitmap indicates value at index {}, but dense iterator exhausted.", present_index);
                return Err(ColumnarError::DecompressionError(format!(
                    "Mismatch between bitmap presence (index {}) and decompressed value count.",
                    present_index
                ))
                .into());
            }
        } else {
            // This should ideally not happen if chunk_len is derived correctly
            error!("Decompression consistency error: Bitmap contains index {} which is >= inferred chunk length {}.", present_index, chunk_len);
            return Err(ColumnarError::DecompressionError(format!(
                "Bitmap index {} out of bounds for inferred chunk length {}",
                present_index, chunk_len
            ))
            .into());
        }
    }

    // After iterating through all present bits, the dense iterator should be empty
    if dense_iter.next().is_some() {
        error!("Decompression mismatch: Dense iterator not exhausted after processing all bitmap indices.");
        return Err(ColumnarError::DecompressionError(
            "Mismatch between bitmap count and decompressed value count (extra values found)"
                .to_string(),
        )
        .into());
    }

    trace!("Decompressed into {} Option<Value>s", result.len());
    Ok(result)
}

pub fn decompress_dense_data(chunk: &ColumnChunk) -> Result<Vec<Value>> {
    trace!("Decompressing dense data for chunk variant");
    match chunk {
        ColumnChunk::Timestamp { data, .. } => {
            trace!(" -> Decompressing as Timestamp (i64)");
            decompress_i64_zstd(data).map(|vals| vals.into_iter().map(Value::Timestamp).collect())
        }
        ColumnChunk::Integer { data, .. } => {
            trace!(" -> Decompressing as Integer (i64)");
            decompress_i64_zstd(data).map(|vals| vals.into_iter().map(Value::Integer).collect())
        }
        ColumnChunk::Float { data, .. } => {
            trace!(" -> Decompressing as Float (f64)");
            decompress_f64_zstd(data).map(|vals| vals.into_iter().map(Value::Float).collect())
        }
        ColumnChunk::String { data, .. } => {
            trace!(" -> Decompressing as String");
            decompress_string_zstd(data).map(|vals| vals.into_iter().map(Value::String).collect())
        }
        ColumnChunk::Enum { data, .. } => {
            trace!(" -> Decompressing as Enum (u32)");
            decompress_enum_zstd(data).map(|vals| vals.into_iter().map(Value::Enum).collect())
        }
    }
}

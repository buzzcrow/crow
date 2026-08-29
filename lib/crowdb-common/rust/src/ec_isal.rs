// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! isa-l FFI bindings for Reed-Solomon GF(2^8) erasure coding.
//!
//! Wraps the isa-l `erasure_code.h` API: `gf_gen_rs_matrix`,
//! `ec_init_tables`, `ec_encode_data`, `gf_invert_matrix`. The safe
//! public functions (`isal_encode`, `isal_decode`) are called by
//! `ec.rs` as the EC backend.

#![allow(unsafe_code)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_lossless,
    clippy::many_single_char_names,
    clippy::needless_range_loop
)]

type UcPtr = *mut u8;

extern "C" {
    fn gf_gen_rs_matrix(a: UcPtr, m: i32, k: i32);
    fn gf_invert_matrix(input: UcPtr, output: UcPtr, n: i32);
    fn ec_init_tables(k: i32, rows: i32, a: UcPtr, gftbls: UcPtr);
    fn ec_encode_data(len: i32, k: i32, rows: i32, gftbls: UcPtr, data: *const UcPtr, coding: *const UcPtr);
}

// ── GF(2^8) arithmetic ──────────────────────────────────────────
// isa-l uses the AES polynomial 0x11d. We build log/exp tables for
// multiplication so the decode-matrix construction (parity row ×
// inverse) can run in pure Rust without another FFI call.

const GF_POLY: u16 = 0x11d;

fn build_gf_tables() -> ([u8; 256], [u8; 256]) {
    let mut exp = [0u8; 256];
    let mut log = [0u8; 256];
    let mut x: u16 = 1;
    for i in 0..255 {
        exp[i] = x as u8;
        log[x as usize] = i as u8;
        x <<= 1;
        if x & 0x100 != 0 {
            x ^= GF_POLY;
        }
    }
    // exp[255..] wraps: exp[i] = exp[i - 255]
    for i in 255..256 {
        exp[i] = exp[i - 255];
    }
    (exp, log)
}

fn gf_mul(a: u8, b: u8, exp: &[u8; 256], log: &[u8; 256]) -> u8 {
    if a == 0 || b == 0 {
        0
    } else {
        let l = (log[a as usize] as u16 + log[b as usize] as u16) % 255;
        exp[l as usize]
    }
}

/// Encode `data_num` data shards into `code_num` parity shards.
///
/// All data shards must be the same length. Returns a vector of
/// `code_num` parity shards.
pub fn isal_encode(data: &mut [&mut [u8]], data_num: usize, code_num: usize) -> Vec<Vec<u8>> {
    let shard_size = data.first().map_or(0, |s| s.len());
    let mut parity: Vec<Vec<u8>> = (0..code_num).map(|_| vec![0u8; shard_size]).collect();

    let k = data_num as i32;
    let rows = code_num as i32;
    let m = k + rows;

    // Build the (m × k) RS generator matrix.
    let mut matrix = vec![0u8; (data_num + code_num) * data_num];
    unsafe {
        gf_gen_rs_matrix(matrix.as_mut_ptr(), m, k);
    }

    // Init encode tables from the parity rows (bottom `rows` rows).
    let mut gftbls = vec![0u8; 32 * data_num * code_num];
    unsafe {
        ec_init_tables(
            k,
            rows,
            matrix.as_mut_ptr().add(data_num * data_num),
            gftbls.as_mut_ptr(),
        );
    }

    let data_ptrs: Vec<UcPtr> = data.iter_mut().map(|s| s.as_mut_ptr()).collect();
    let parity_ptrs: Vec<UcPtr> = parity.iter_mut().map(Vec::as_mut_ptr).collect();

    unsafe {
        ec_encode_data(
            shard_size as i32,
            k,
            rows,
            gftbls.as_mut_ptr(),
            data_ptrs.as_ptr(),
            parity_ptrs.as_ptr(),
        );
    }

    parity
}

/// Reconstruct lost shards.
///
/// `shards` is a vector of `(data_num + code_num)` entries, each
/// `Some(bytes)` if survived or `None` if lost. Up to `code_num` shards
/// can be lost. Returns the full reconstructed shard vector.
pub fn isal_decode(shards: Vec<Option<Vec<u8>>>, data_num: usize, code_num: usize) -> Vec<Vec<u8>> {
    let total = data_num + code_num;
    let shard_size = shards
        .iter()
        .filter_map(|s| s.as_ref())
        .map(Vec::len)
        .max()
        .unwrap_or(0);

    let lost: Vec<usize> = shards
        .iter()
        .enumerate()
        .filter(|(_, s)| s.is_none())
        .map(|(i, _)| i)
        .collect();

    // Normalize: pad surviving shards to shard_size, lost shards zero-filled.
    let mut buffers: Vec<Vec<u8>> = shards
        .into_iter()
        .map(|s| match s {
            Some(b) => {
                let mut padded = b;
                padded.resize(shard_size, 0);
                padded
            }
            None => vec![0u8; shard_size],
        })
        .collect();

    if lost.is_empty() {
        return buffers;
    }

    let (exp, log) = build_gf_tables();
    let k = data_num as i32;

    // Build the (m × k) RS generator matrix.
    let m = (data_num + code_num) as i32;
    let mut encode_matrix = vec![0u8; (data_num + code_num) * data_num];
    unsafe {
        gf_gen_rs_matrix(encode_matrix.as_mut_ptr(), m, k);
    }

    // Build g (k × k) from the rows of encode_matrix corresponding to
    // the first k surviving shards.
    let mut g = vec![0u8; data_num * data_num];
    let mut surviving: Vec<usize> = Vec::with_capacity(data_num);
    for i in 0..total {
        if !lost.contains(&i) {
            surviving.push(i);
            if surviving.len() == data_num {
                break;
            }
        }
    }
    for (row, &src) in surviving.iter().enumerate() {
        for col in 0..data_num {
            g[row * data_num + col] = encode_matrix[src * data_num + col];
        }
    }

    // Invert g → g_inv.
    let mut g_inv = vec![0u8; data_num * data_num];
    unsafe {
        gf_invert_matrix(g.as_mut_ptr(), g_inv.as_mut_ptr(), k);
    }

    // Build decode_matrix (lost_count × k): for each lost shard,
    // the decode row is g_inv[lost_data_row] (for lost data shards) or
    // encode_matrix[parity_row] × g_inv (for lost parity shards).
    let mut decode_matrix = vec![0u8; lost.len() * data_num];
    for (out_row, &lost_idx) in lost.iter().enumerate() {
        if lost_idx < data_num {
            // Lost data shard: decode row = g_inv row lost_idx.
            for col in 0..data_num {
                decode_matrix[out_row * data_num + col] = g_inv[lost_idx * data_num + col];
            }
        } else {
            // Lost parity shard p: decode row = encode_matrix[p] × g_inv.
            let p = lost_idx;
            for col in 0..data_num {
                let mut s = 0u8;
                for j in 0..data_num {
                    s ^= gf_mul(
                        encode_matrix[p * data_num + j],
                        g_inv[j * data_num + col],
                        &exp,
                        &log,
                    );
                }
                decode_matrix[out_row * data_num + col] = s;
            }
        }
    }

    // Init decode tables.
    let rows = lost.len() as i32;
    let mut gftbls = vec![0u8; 32 * data_num * lost.len()];
    unsafe {
        ec_init_tables(k, rows, decode_matrix.as_mut_ptr(), gftbls.as_mut_ptr());
    }

    // Input = the k surviving shards (in the order they appear in `surviving`).
    let input_ptrs: Vec<UcPtr> = surviving.iter().map(|&i| buffers[i].as_mut_ptr()).collect();
    let lost_ptrs: Vec<UcPtr> = lost.iter().map(|&i| buffers[i].as_mut_ptr()).collect();

    unsafe {
        ec_encode_data(
            shard_size as i32,
            k,
            rows,
            gftbls.as_mut_ptr(),
            input_ptrs.as_ptr(),
            lost_ptrs.as_ptr(),
        );
    }

    buffers
}

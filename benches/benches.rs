// Copyright (c) 2013, 2014, 2015, 2016 Michael Woerister
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#![feature(test)]
#![allow(unused_parens)]

extern crate test;
extern crate hamt_rs;
extern crate rand;

use test::Bencher;
use rand::{Rng};
use std::collections::HashMap;

use std::collections::hash_map::DefaultHasher as StdHasher;

use hamt_rs::{ItemStore, ShareStore, CopyStore};
use hamt_rs::HamtMap;

static BENCH_FIND_COUNT: usize = 1000;
static BENCH_INSERT_COUNT: usize = 1000;

fn create_random_std_hashmap(count: usize) -> HashMap<u64, u64> {
    let mut hashmap = HashMap::<u64, u64>::new();
    let mut rng = rand::thread_rng();

    while hashmap.len() < count {
        let value = rng.gen();
        hashmap.insert(value, value);
    }

    return hashmap;
}

fn create_unique_values(count: usize) -> Vec<u64> {
    create_random_std_hashmap(count).keys().map(|x| *x).collect()
}

pub static mut RESULTS: [Option<u64>; 1000000] = [None; 1000000];

fn create_random_hamt<IS: ItemStore<u64, u64>>(empty: HamtMap<u64, u64, IS>, count: usize) -> (HamtMap<u64, u64, IS>, Vec<u64>) {
    let keys = create_unique_values(count);
    let mut map = empty;

    for &x in keys.iter() {
        map = map.plus(x, x);
    }

    return (map, keys);
}

fn bench_hamt_find<IS: ItemStore<u64, u64>>(empty: HamtMap<u64, u64, IS>, count: usize, bh: &mut Bencher) {
    let (map, keys) = create_random_hamt(empty, count);

    bh.iter(|| {
        for i in (0usize .. BENCH_FIND_COUNT) {
            let val = keys[i % count];
            // lets make about half of the lookups fail
            let val = val + (i as u64 & 1);

            unsafe {
                match map.find(&val) {
                    Some(&x) => RESULTS[i] = Some(x),
                    None => RESULTS[i] = None,
                }
            }
        }
    })
}

fn bench_hamt_insert<IS: ItemStore<u64, u64>>(empty: HamtMap<u64, u64, IS>, count: usize, bh: &mut Bencher) {
    let (map, keys) = create_random_hamt(empty, count + BENCH_INSERT_COUNT);

    bh.iter(|| {
        let mut map1 = map.clone();

        for i in (0usize .. BENCH_INSERT_COUNT) {
            let val = keys[count + i];
            map1 = map1.plus(val, val);
        }
    })
}

fn bench_hamt_remove<IS: ItemStore<u64, u64>>(empty: HamtMap<u64, u64, IS>, count: usize, bh: &mut Bencher) {
    let (map, keys) = create_random_hamt(empty, count);

    bh.iter(|| {
        let mut map = map.clone();

        for x in (0 .. count).filter(|x| x % 2 == 0) {
            map = map.minus(&keys[x]);
        }
    })
}

fn bench_std_hashmap_find(count: usize, bh: &mut Bencher) {
    let values = create_unique_values(count);
    let mut map = HashMap::new();

    for &x in values.iter() {
        map.insert(x, x);
    }

    bh.iter(|| {
        for i in (0usize .. BENCH_FIND_COUNT) {
            let val = values[i % count];

            // lets make about half of the lookups fail
            let val = val + (i as u64 & 1);

            unsafe {
                match map.get(&val) {
                    Some(&x) => RESULTS[i] = Some(x),
                    None => RESULTS[i] = None,
                }
            }
        }
    })
}

fn bench_std_hashmap_insert(count: usize, bh: &mut Bencher) {
    let values = create_unique_values(count + BENCH_INSERT_COUNT);
    let mut map = HashMap::new();

    for &x in values.iter() {
        map.insert(x, x);
    }

    bh.iter(|| {
        let mut map1 = map.clone();

        for i in (0usize .. BENCH_INSERT_COUNT) {
            let val = values[count + i];
            map1.insert(val, val);
        }
    })
}

fn bench_std_hashmap_clone(count: usize, bh: &mut Bencher) {
    let values = create_unique_values(count);
    let mut map = HashMap::new();

    for &x in values.iter() {
        map.insert(x, x);
    }

    bh.iter(|| {
        map.clone();
    })
}

fn bench_std_hashmap_remove(count: usize, bh: &mut Bencher) {
    let values = create_unique_values(count);
    let mut map = HashMap::new();

    for &x in values.iter() {
        map.insert(x, x);
    }

    bh.iter(|| {
        let mut map1 = map.clone();

        for x in (0..count).filter(|x| x % 2 == 0) {
            map1.remove(&values[x]);
        }
    })
}




//=-------------------------------------------------------------------------------------------------
// Bench std::HashMap::find()
//=-------------------------------------------------------------------------------------------------

#[bench]
fn bench_std_hashmap_find_10(bh: &mut Bencher) {
    bench_std_hashmap_find(10, bh);
}

#[bench]
fn bench_std_hashmap_find_1000(bh: &mut Bencher) {
    bench_std_hashmap_find(1000, bh);
}

#[bench]
fn bench_std_hashmap_find_100000(bh: &mut Bencher) {
    bench_std_hashmap_find(100000, bh);
}



//=-------------------------------------------------------------------------------------------------
// Bench std::HashMap::insert()
//=-------------------------------------------------------------------------------------------------

#[bench]
fn bench_std_hashmap_insert_10(bh: &mut Bencher) {
    bench_std_hashmap_insert(10, bh);
}

#[bench]
fn bench_std_hashmap_insert_1000(bh: &mut Bencher) {
    bench_std_hashmap_insert(1000, bh);
}

#[bench]
fn bench_std_hashmap_insert_100000(bh: &mut Bencher) {
    bench_std_hashmap_insert(100000, bh);
}

#[bench]
fn std_hashmap_clone_10(bh: &mut Bencher) {
    bench_std_hashmap_clone(10, bh);
}

#[bench]
fn bench_std_hashmap_clone_1000(bh: &mut Bencher) {
    bench_std_hashmap_clone(1000, bh);
}

#[bench]
fn bench_std_hashmap_clone_100000(bh: &mut Bencher) {
    bench_std_hashmap_clone(100000, bh);
}



//=-------------------------------------------------------------------------------------------------
// Bench std::HashMap::remove()
//=-------------------------------------------------------------------------------------------------

#[bench]
fn bench_std_hashmap_remove_10(bh: &mut Bencher) {
    bench_std_hashmap_remove(10, bh);
}

#[bench]
fn bench_std_hashmap_remove_1000(bh: &mut Bencher) {
    bench_std_hashmap_remove(1000, bh);
}

#[bench]
fn bench_std_hashmap_remove_10000(bh: &mut Bencher) {
    bench_std_hashmap_remove(10000, bh);
}


//=-------------------------------------------------------------------------------------------------
// Bench HamtMap<ShareStore>
//=-------------------------------------------------------------------------------------------------

type ShareStoreHamt = HamtMap<u64, u64, ShareStore<u64,u64>, StdHasher>;

#[bench]
fn bench_hamt_insert_share_10(bh: &mut Bencher) {
    bench_hamt_insert(ShareStoreHamt::new(), 10, bh);
}

#[bench]
fn bench_hamt_insert_share_1000(bh: &mut Bencher) {
    bench_hamt_insert(ShareStoreHamt::new(), 1000, bh);
}

#[bench]
fn bench_hamt_insert_share_100000(bh: &mut Bencher) {
    bench_hamt_insert(ShareStoreHamt::new(), 100000, bh);
}

#[bench]
fn bench_hamt_find_share_10(bh: &mut Bencher) {
    bench_hamt_find(ShareStoreHamt::new(), 10, bh);
}

#[bench]
fn bench_hamt_find_share_1000(bh: &mut Bencher) {
    bench_hamt_find(ShareStoreHamt::new(), 1000, bh);
}

#[bench]
fn bench_hamt_find_share_100000(bh: &mut Bencher) {
    bench_hamt_find(ShareStoreHamt::new(), 100000, bh);
}

#[bench]
fn bench_hamt_remove_share_10(bh: &mut Bencher) {
    bench_hamt_remove(ShareStoreHamt::new(), 10, bh);
}

#[bench]
fn bench_hamt_remove_share_1000(bh: &mut Bencher) {
    bench_hamt_remove(ShareStoreHamt::new(), 1000, bh);
}

#[bench]
fn bench_hamt_remove_share_100000(bh: &mut Bencher) {
    bench_hamt_remove(ShareStoreHamt::new(), 100000, bh);
}

#[bench]
fn bench_hamt_iterate_share_10(bh: &mut Bencher) {
    bench_hamt_iterate_share(ShareStoreHamt::new(), 10, bh);
}

#[bench]
fn bench_hamt_iterate_share_1000(bh: &mut Bencher) {
    bench_hamt_iterate_share(ShareStoreHamt::new(), 1000, bh);
}

#[bench]
fn bench_hamt_iterate_share_100000(bh: &mut Bencher) {
    bench_hamt_iterate_share(ShareStoreHamt::new(), 100000, bh);
}

fn bench_hamt_iterate_share(mut map: ShareStoreHamt,
                        size: usize,
                        bh: &mut Bencher) {
    for i in (0u64 .. size as u64) {
        map = map.plus(i, i);
    }

    bh.iter(|| {
        for _ in map.iter() {}
    })
}


//=-------------------------------------------------------------------------------------------------
// Bench HamtMap<CopyStore>
//=-------------------------------------------------------------------------------------------------

type CopyStoreHamt = HamtMap<u64, u64, CopyStore<u64, u64>, StdHasher>;

#[bench]
fn bench_hamt_insert_copy_10(bh: &mut Bencher) {
    bench_hamt_insert(CopyStoreHamt::new(), 10, bh);
}

#[bench]
fn bench_hamt_insert_copy_1000(bh: &mut Bencher) {
    bench_hamt_insert(CopyStoreHamt::new(), 1000, bh);
}

#[bench]
fn bench_hamt_insert_copy_100000(bh: &mut Bencher) {
    bench_hamt_insert(CopyStoreHamt::new(), 100000, bh);
}

#[bench]
fn bench_hamt_find_copy_10(bh: &mut Bencher) {
    bench_hamt_find(CopyStoreHamt::new(), 10, bh);
}

#[bench]
fn bench_hamt_find_copy_1000(bh: &mut Bencher) {
    bench_hamt_find(CopyStoreHamt::new(), 1000, bh);
}

#[bench]
fn bench_hamt_find_copy_100000(bh: &mut Bencher) {
    bench_hamt_find(CopyStoreHamt::new(), 100000, bh);
}

#[bench]
fn bench_hamt_remove_copy_10(bh: &mut Bencher) {
    bench_hamt_remove(CopyStoreHamt::new(), 10, bh);
}

#[bench]
fn bench_hamt_remove_copy_1000(bh: &mut Bencher) {
    bench_hamt_remove(CopyStoreHamt::new(), 1000, bh);
}

#[bench]
fn bench_hamt_remove_copy_100000(bh: &mut Bencher) {
    bench_hamt_remove(CopyStoreHamt::new(), 100000, bh);
}

#[bench]
fn bench_iterate_copy_10(bh: &mut Bencher) {
    bench_hamt_iterate_copy(CopyStoreHamt::new(), 10, bh);
}

#[bench]
fn bench_iterate_copy_1000(bh: &mut Bencher) {
    bench_hamt_iterate_copy(CopyStoreHamt::new(), 1000, bh);
}

#[bench]
fn bench_iterate_copy_100000(bh: &mut Bencher) {
    bench_hamt_iterate_copy(CopyStoreHamt::new(), 100000, bh);
}

fn bench_hamt_iterate_copy(mut map: CopyStoreHamt,
                       size: usize,
                       bh: &mut Bencher) {
    for i in (0u64 .. size as u64) {
        map = map.plus(i, i);
    }

    bh.iter(|| {
        for _ in map.iter() {}
    })
}

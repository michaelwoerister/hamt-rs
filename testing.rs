
use rand::{Rng, StdRng};
use collections::hashmap::HashMap;

use test::Bencher;

use PersistentMap;

macro_rules! assert_find(
    ($map:ident, $key:expr, None) => (
        assert!($map.find(&$key).is_none());
    );
    ($map:ident, $key:expr, $val:expr) => (
        match $map.find(&$key) {
            Some(&value) => {
                assert_eq!(value, $val);
            }
            _ => fail!()
        };
    );
)

static BENCH_FIND_COUNT: uint = 1000;
static BENCH_INSERT_COUNT: uint = 1000;

pub struct Test;

impl<TPersistentMap: PersistentMap<u64, u64>> Test {

    pub fn test_insert(empty: TPersistentMap) {
        let map00 = empty;
        let (map01, new_entry01) = map00.clone().insert(1, 2);
        let (map10, new_entry10) = map00.clone().insert(2, 4);
        let (map11, new_entry11) = map01.clone().insert(2, 4);

        assert_find!(map00, 1, None);
        assert_find!(map00, 2, None);

        assert_find!(map01, 1, 2);
        assert_find!(map01, 2, None);

        assert_find!(map11, 1, 2);
        assert_find!(map11, 2, 4);

        assert_find!(map10, 1, None);
        assert_find!(map10, 2, 4);

        assert_eq!(new_entry01, true);
        assert_eq!(new_entry10, true);
        assert_eq!(new_entry11, true);

        assert_eq!(map00.len(), 0);
        assert_eq!(map01.len(), 1);
        assert_eq!(map10.len(), 1);
        assert_eq!(map11.len(), 2);
    }

    pub fn test_insert_ascending(empty: TPersistentMap) {
        let mut map = empty;

        for x in range(0u64, 1000u64) {
            assert_eq!(map.len(), x as uint);
            map = map.insert(x, x).val0();
            assert_find!(map, x, x);
        }
    }

    pub fn test_insert_descending(empty: TPersistentMap) {
        let mut map = empty;

        for x in range(0u64, 1000u64) {
            let key = 999u64 - x;
            assert_eq!(map.len(), x as uint);
            map = map.insert(key, x).val0();
            assert_find!(map, key, x);
        }
    }

    pub fn test_insert_overwrite(empty: TPersistentMap) {
        let (mapA, new_entryA) = empty.clone().insert(1, 2);
        let (mapB, new_entryB) = mapA.clone().insert(1, 4);
        let (mapC, new_entryC) = mapB.clone().insert(1, 6);

        assert_find!(empty, 1, None);
        assert_find!(mapA, 1, 2);
        assert_find!(mapB, 1, 4);
        assert_find!(mapC, 1, 6);

        assert_eq!(new_entryA, true);
        assert_eq!(new_entryB, false);
        assert_eq!(new_entryC, false);

        assert_eq!(empty.len(), 0);
        assert_eq!(mapA.len(), 1);
        assert_eq!(mapB.len(), 1);
        assert_eq!(mapC.len(), 1);
    }

    pub fn test_remove(empty: TPersistentMap) {
        let (map00, _) = (empty
            .insert(1, 2)).val0()
            .insert(2, 4);

        let (map01, _) = map00.clone().remove(&1);
        let (map10, _) = map00.clone().remove(&2);
        let (map11, _) = map01.clone().remove(&2);

        assert_find!(map00, 1, 2);
        assert_find!(map00, 2, 4);

        assert_find!(map01, 1, None);
        assert_find!(map01, 2, 4);

        assert_find!(map11, 1, None);
        assert_find!(map11, 2, None);

        assert_find!(map10, 1, 2);
        assert_find!(map10, 2, None);

        assert_eq!(map00.len(), 2);
        assert_eq!(map01.len(), 1);
        assert_eq!(map10.len(), 1);
        assert_eq!(map11.len(), 0);
    }

    pub fn random_insert_remove_stress_test(empty: TPersistentMap) {
        let mut reference: HashMap<u64, u64> = HashMap::new();
        let mut rng = StdRng::new().ok().expect("Could not create random number generator");

        let mut map = empty;

        for _ in range(0, 500000) {
            let value: u64 = rng.gen();

            if rng.gen_weighted_bool(2) {
                let ref_size_change = reference.remove(&value);
                let (map1, size_change) = map.remove(&value);
                assert_eq!(ref_size_change, size_change);
                assert_find!(map1, value, None);
                assert_eq!(reference.len(), map1.len());
                assert_eq!(reference.find(&value), map1.find(&value));
                map = map1;
            } else {
                let ref_size_change = reference.insert(value, value);
                let (map1, size_change) = map.insert(value, value);
                assert_eq!(ref_size_change, size_change);
                assert_find!(map1, value, value);
                assert_eq!(reference.len(), map1.len());
                assert_eq!(reference.find(&value), map1.find(&value));
                map = map1;
            }
        }
    }
}

fn create_random_std_hashmap(count: uint) -> HashMap<u64, u64> {
    let mut hashmap = HashMap::<u64, u64>::new();
    let mut rng = StdRng::new().ok().expect("Could not create random number generator");

    while hashmap.len() < count {
        let value = rng.gen();
        hashmap.insert(value, value);
    }

    return hashmap;
}


fn create_unique_values(count: uint) -> Vec<u64> {
    create_random_std_hashmap(count).keys().map(|x| *x).collect()
}

pub static mut results: [Option<u64>, ..1000000] = [None, ..1000000];

impl<TPersistentMap: PersistentMap<u64, u64>> Test {

    fn create_random_map(empty: TPersistentMap, count: uint) -> (TPersistentMap, Vec<u64>) {
        let keys = create_unique_values(count);
        let mut map = empty;

        for &x in keys.iter() {
            map = map.plus(x, x);
        }

        return (map, keys);
    }

    pub fn bench_find(empty: TPersistentMap, count: uint, bh: &mut Bencher) {
        let (map, keys) = Test::create_random_map(empty, count);

        bh.iter(|| {
            for i in range(0u, BENCH_FIND_COUNT) {
                let val = keys.get(i % count);
                // lets make about of half the lookups fail
                let val = val + (i as u64 & 1);

                unsafe {
                    match map.find(&val) {
                        Some(&x) => results[i] = Some(x),
                        None => results[i] = None,
                    }
                }
            }
        })
    }

    pub fn bench_insert(empty: TPersistentMap, count: uint, bh: &mut Bencher) {
        let (map, keys) = Test::create_random_map(empty, count + BENCH_INSERT_COUNT);

        bh.iter(|| {
            let mut map1 = map.clone();

            for i in range(0u, BENCH_INSERT_COUNT) {
                let val = *keys.get(count + i);
                map1 = map1.plus(val, val);
            }
        })
    }

    pub fn bench_remove(empty: TPersistentMap, count: uint, bh: &mut Bencher) {
        let (map, keys) = Test::create_random_map(empty, count);

        bh.iter(|| {
            let mut map = map.clone();

            for x in ::std::iter::range_step(0, count as uint, 2) {
                map = map.minus(keys.get(x));
            }
        })
    }
}

fn bench_find_hashmap<T: MutableMap<u64, u64>>(empty: T, count: uint, bh: &mut Bencher) {
    let values = create_unique_values(count);
    let mut map = empty;

    for &x in values.iter() {
        map.insert(x, x);
    }

    bh.iter(|| {
        for i in range(0u, BENCH_FIND_COUNT) {
            let val = values.get(i % count);

            unsafe {
                match map.find(val) {
                    Some(&x) => results[i] = Some(x),
                    None => results[i] = None,
                }
            }
        }
    })
}

fn bench_insert_hashmap<T: MutableMap<u64, u64> + Clone>(empty: T, count: uint, bh: &mut Bencher) {
    let values = create_unique_values(count + BENCH_INSERT_COUNT);
    let mut map = empty;

    for &x in values.iter() {
        map.insert(x, x);
    }

    bh.iter(|| {
        let mut map1 = map.clone();

        for i in range(0u, BENCH_INSERT_COUNT) {
            let val = *values.get(count + i);
            map1.insert(val, val);
        }
    })
}

fn bench_clone_hashmap<T: MutableMap<u64, u64>+Clone>(empty: T,
                                                        count: uint,
                                                        bh: &mut Bencher) {
    let values = create_unique_values(count);
    let mut map = empty;

    for &x in values.iter() {
        map.insert(x, x);
    }

    bh.iter(|| {
        map.clone();
    })
}

fn bench_remove_hashmap<T: MutableMap<u64, u64>+Clone>(empty: T, count: uint, bh: &mut Bencher) {
    let values = create_unique_values(count);
    let mut map = empty;

    for &x in values.iter() {
        map.insert(x, x);
    }

    bh.iter(|| {
        let mut map1 = map.clone();

        for x in ::std::iter::range_step(0, count, 2) {
            map1.remove(values.get(x));
        }
    })
}




//=-------------------------------------------------------------------------------------------------
// Bench std::HashMap::find()
//=-------------------------------------------------------------------------------------------------

#[bench]
fn std_hashmap_find_10(bh: &mut Bencher) {
    bench_find_hashmap(HashMap::<u64, u64>::new(), 10, bh);
}

#[bench]
fn std_hashmap_find_1000(bh: &mut Bencher) {
    bench_find_hashmap(HashMap::<u64, u64>::new(), 1000, bh);
}

#[bench]
fn std_hashmap_find_100000(bh: &mut Bencher) {
    bench_find_hashmap(HashMap::<u64, u64>::new(), 100000, bh);
}



//=-------------------------------------------------------------------------------------------------
// Bench std::HashMap::insert()
//=-------------------------------------------------------------------------------------------------

#[bench]
fn std_hashmap_insert_10(bh: &mut Bencher) {
    bench_insert_hashmap(HashMap::<u64, u64>::new(),10, bh);
}

#[bench]
fn std_hashmap_insert_1000(bh: &mut Bencher) {
    bench_insert_hashmap(HashMap::<u64, u64>::new(),1000, bh);
}

#[bench]
fn std_hashmap_insert_100000(bh: &mut Bencher) {
    bench_insert_hashmap(HashMap::<u64, u64>::new(),100000, bh);
}

#[bench]
fn std_hashmap_clone_10(bh: &mut Bencher) {
    bench_clone_hashmap(HashMap::<u64, u64>::new(),10, bh);
}

#[bench]
fn std_hashmap_clone_1000(bh: &mut Bencher) {
    bench_clone_hashmap(HashMap::<u64, u64>::new(),1000, bh);
}

#[bench]
fn std_hashmap_clone_100000(bh: &mut Bencher) {
    bench_clone_hashmap(HashMap::<u64, u64>::new(),100000, bh);
}



//=-------------------------------------------------------------------------------------------------
// Bench std::HashMap::remove()
//=-------------------------------------------------------------------------------------------------

#[bench]
fn std_hashmap_remove_10(bh: &mut Bencher) {
    bench_remove_hashmap(HashMap::<u64, u64>::new(),10, bh);
}

#[bench]
fn std_hashmap_remove_1000(bh: &mut Bencher) {
    bench_remove_hashmap(HashMap::<u64, u64>::new(),1000, bh);
}

#[bench]
fn std_hashmap_remove_10000(bh: &mut Bencher) {
    bench_remove_hashmap(HashMap::<u64, u64>::new(),10000, bh);
}

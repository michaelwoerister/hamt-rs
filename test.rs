
use persistent::PersistentMap;
use std::hashmap::HashSet;
use std::rand;
use std::rand::Rng;
use std::iter::range;
use extra::test::BenchHarness;
use std::hashmap::HashMap;
use std::trie::TrieMap;

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

pub struct Test;

impl<TPersistentMap: PersistentMap<uint, uint>> Test {

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

        for x in range(0u, 1000u) {
            assert_eq!(map.len(), x as uint);
            map = map.insert(x, x).first();
            assert_find!(map, x, x);
        }
    }

    pub fn test_insert_descending(empty: TPersistentMap) {
        let mut map = empty;

        for x in range(0u, 1000u) {
            let key = 999u - x;
            assert_eq!(map.len(), x as uint);
            map = map.insert(key, x).first();
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
            .insert(1, 2)).first()
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

    pub fn test_random(empty: TPersistentMap) {
        let mut values: HashSet<uint> = HashSet::new();
        let mut rng = rand::rng();

        for _ in range(0, 2000000) {
            values.insert(rand::Rand::rand(&mut rng));
        }

        let mut map = empty;

        for &x in values.iter() {
            let (map1, _) = map.insert(x, x);
            map = map1;
        }

        for &x in values.iter() {
            assert_find!(map, x, x);
        }

        for (i, x) in values.iter().enumerate() {
            if i % 2 == 0 {
                let (map1, _) = map.remove(x);
                map = map1;
            }
        }

        for (i, &x) in values.iter().enumerate() {
            if i % 2 != 0 {
                assert_find!(map, x, x);
            } else {
                assert_find!(map, x, None);
            }
        }
    }

    pub fn random_insert_remove_stress_test(empty: TPersistentMap) {
        let mut reference: TrieMap<uint> = TrieMap::new();
        let mut rng = rand::rng();

        let mut map = empty;

        for _ in range(0, 10000000) {
            let value = rng.gen();

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

impl<TPersistentMap: PersistentMap<uint, uint>> Test {

    pub fn bench_find(empty: TPersistentMap, count: uint, bh: &mut BenchHarness) {
        let mut rng = rand::rng();
        let values = ::std::vec::from_fn(count, |_| rand::Rand::rand(&mut rng));
        let mut map = empty;

        for &x in values.iter() {
            map = map.insert(x, x).first();
        }

        bh.iter(|| {
            for x in values.rev_iter() {
                let _ = map.find(x);
            }
        })
    }

    pub fn bench_insert(empty: TPersistentMap, count: uint, bh: &mut BenchHarness) {
        let mut rng = rand::rng();
        let values = ::std::vec::from_fn(count, |_| rand::Rand::rand(&mut rng));

        bh.iter(|| {
            let mut map = empty.clone();

            for &x in values.iter() {
                let (map1, _) = map.insert(x, x);
                map = map1;
            }
        })
    }

    pub fn bench_remove(empty: TPersistentMap, count: uint, bh: &mut BenchHarness) {
        let mut rng = rand::rng();
        let values = ::std::vec::from_fn(count, |_| rand::Rand::rand(&mut rng));
        let mut map = empty;

        for &x in values.iter() {
            map = map.insert(x, x).first();
        }

        bh.iter(|| {
            let mut map = map.clone();

            for x in ::std::iter::range_step(0, count as uint, 2) {
                map = map.remove(&values[x]).first();
            }
        })
    }
}

fn bench_find_hashmap<T: MutableMap<uint, uint>>(empty: T, count: uint, bh: &mut BenchHarness) {
    let mut rng = rand::rng();
    let values = ::std::vec::from_fn(count, |_| rand::Rand::rand(&mut rng));
    let mut map = empty;

    for &x in values.iter() {
        map.insert(x, x);
    }

    bh.iter(|| {
        for x in values.rev_iter() {
            let _ = map.find(x);
        }
    })
}

fn bench_insert_hashmap<T: MutableMap<uint, uint>>(empty: T, count: uint, bh: &mut BenchHarness) {
    let mut rng = rand::rng();
    let values = ::std::vec::from_fn(count, |_| rand::Rand::rand(&mut rng));
    let mut map = empty;

    bh.iter(|| {
        for &x in values.iter() {
            map.insert(x, x);
        }
    })
}

fn bench_remove_hashmap<T: MutableMap<uint, uint>>(empty: T, count: uint, bh: &mut BenchHarness) {
    let mut rng = rand::rng();
    let values = ::std::vec::from_fn(count, |_| rand::Rand::rand(&mut rng));
    let mut map = empty;

    for &x in values.iter() {
        map.insert(x, x);
    }

    bh.iter(|| {
        for x in ::std::iter::range_step(0, count, 2) {
            map.remove(&values[x]);
        }
    })
}

//=-------------------------------------------------------------------------------------------------
// Bench std::HashMap::find()
//=-------------------------------------------------------------------------------------------------

#[bench]
fn std_hashmap_find_10(bh: &mut BenchHarness) {
    bench_find_hashmap(HashMap::<uint, uint>::new(), 10, bh);
}

#[bench]
fn std_hashmap_find_100(bh: &mut BenchHarness) {
    bench_find_hashmap(HashMap::<uint, uint>::new(),100, bh);
}

#[bench]
fn std_hashmap_find_1000(bh: &mut BenchHarness) {
    bench_find_hashmap(HashMap::<uint, uint>::new(),1000, bh);
}

#[bench]
fn std_hashmap_find_50000(bh: &mut BenchHarness) {
    bench_find_hashmap(HashMap::<uint, uint>::new(),50000, bh);
}



//=-------------------------------------------------------------------------------------------------
// Bench std::HashMap::insert()
//=-------------------------------------------------------------------------------------------------

#[bench]
fn std_hashmap_insert_10(bh: &mut BenchHarness) {
    bench_insert_hashmap(HashMap::<uint, uint>::new(),10, bh);
}

#[bench]
fn std_hashmap_insert_100(bh: &mut BenchHarness) {
    bench_insert_hashmap(HashMap::<uint, uint>::new(),100, bh);
}

#[bench]
fn std_hashmap_insert_1000(bh: &mut BenchHarness) {
    bench_insert_hashmap(HashMap::<uint, uint>::new(),1000, bh);
}

#[bench]
fn std_hashmap_insert_50000(bh: &mut BenchHarness) {
    bench_insert_hashmap(HashMap::<uint, uint>::new(),50000, bh);
}



//=-------------------------------------------------------------------------------------------------
// Bench std::HashMap::remove()
//=-------------------------------------------------------------------------------------------------

#[bench]
fn std_hashmap_remove_10(bh: &mut BenchHarness) {
    bench_remove_hashmap(HashMap::<uint, uint>::new(),10, bh);
}

#[bench]
fn std_hashmap_remove_100(bh: &mut BenchHarness) {
    bench_remove_hashmap(HashMap::<uint, uint>::new(),100, bh);
}

#[bench]
fn std_hashmap_remove_1000(bh: &mut BenchHarness) {
    bench_remove_hashmap(HashMap::<uint, uint>::new(),1000, bh);
}

#[bench]
fn std_hashmap_remove_50000(bh: &mut BenchHarness) {
    bench_remove_hashmap(HashMap::<uint, uint>::new(),50000, bh);
}


//=-------------------------------------------------------------------------------------------------
// Bench std::TrieMap::find()
//=-------------------------------------------------------------------------------------------------

#[bench]
fn std_triemap_find_10(bh: &mut BenchHarness) {
    bench_find_hashmap(TrieMap::<uint>::new(), 10, bh);
}

#[bench]
fn std_triemap_find_100(bh: &mut BenchHarness) {
    bench_find_hashmap(TrieMap::<uint>::new(),100, bh);
}

#[bench]
fn std_triemap_find_1000(bh: &mut BenchHarness) {
    bench_find_hashmap(TrieMap::<uint>::new(),1000, bh);
}

#[bench]
fn std_triemap_find_50000(bh: &mut BenchHarness) {
    bench_find_hashmap(TrieMap::<uint>::new(),50000, bh);
}



//=-------------------------------------------------------------------------------------------------
// Bench std::TrieMap::insert()
//=-------------------------------------------------------------------------------------------------

#[bench]
fn std_triemap_insert_10(bh: &mut BenchHarness) {
    bench_insert_hashmap(TrieMap::<uint>::new(),10, bh);
}

#[bench]
fn std_triemap_insert_100(bh: &mut BenchHarness) {
    bench_insert_hashmap(TrieMap::<uint>::new(),100, bh);
}

#[bench]
fn std_triemap_insert_1000(bh: &mut BenchHarness) {
    bench_insert_hashmap(TrieMap::<uint>::new(),1000, bh);
}

#[bench]
fn std_triemap_insert_50000(bh: &mut BenchHarness) {
    bench_insert_hashmap(TrieMap::<uint>::new(),50000, bh);
}

//=-------------------------------------------------------------------------------------------------
// Bench std::TrieMap::remove()
//=-------------------------------------------------------------------------------------------------

#[bench]
fn std_triemap_remove_10(bh: &mut BenchHarness) {
    bench_remove_hashmap(TrieMap::<uint>::new(),10, bh);
}

#[bench]
fn std_triemap_remove_100(bh: &mut BenchHarness) {
    bench_remove_hashmap(TrieMap::<uint>::new(),100, bh);
}

#[bench]
fn std_triemap_remove_1000(bh: &mut BenchHarness) {
    bench_remove_hashmap(TrieMap::<uint>::new(),1000, bh);
}

#[bench]
fn std_triemap_remove_50000(bh: &mut BenchHarness) {
    bench_remove_hashmap(TrieMap::<uint>::new(),50000, bh);
}

use persistent::PersistentMap;
use std::hashmap::HashSet;
use std::rand;
use std::iter::range;
use extra::test::BenchHarness;

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
        let mut values: HashSet<u64> = HashSet::new();
        let mut rng = rand::rng();

        for _ in range(0, 20000) {
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
}

impl<TPersistentMap: PersistentMap<u64, u64>> Test {

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
}
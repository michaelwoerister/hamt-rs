#[link(name = "persistent-datastructures",
       package_id = "persistent-datastructures",
       vers = "0.9-pre",
       uuid = "ed131d50-5d95-11e3-949a-0800200c9a66",
       url = "https://github.com/mozilla/rust/tree/master/src/libstd")];

#[comment = "Persistent data structures for Rust"];
#[license = "MIT"];
#[crate_type = "rlib"];
#[crate_type = "dylib"];

#[feature(macro_rules)]; // Used for test cases

#[deny(non_camel_case_types)];

extern mod extra;
pub mod hamt;


mod persistent {

    trait PersistentMap<K: Hash+Eq+Send+Freeze,
                        V: Send+Freeze>:
        Map<K, V> + Clone {
        /// Insert a key-value pair into the map. An existing value for a
        /// key is replaced by the new value. Return true if the key did
        /// not already exist in the map.
        fn insert(&self, key: K, value: V) -> (Self, bool);

        /// Remove a key-value pair from the map. Return true if the key
        /// was present in the map, otherwise false.
        fn remove(&self, key: &K) -> (Self, bool);
    }
}

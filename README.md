rs-persistent-datastructures
============================

A Hash Array Mapped Trie implementation based on the
[Ideal Hash Trees](http://lampwww.epfl.ch/papers/idealhashtrees.pdf) paper by Phil Bagwell.
This is the persistent map datastructure used in Scala's and Clojure's standard libraries.
The idea to use a special *collision node* to deal with hash collisions is taken from Clojure's
implementation.

## Usage
```rust
let mut map = HamtMap::new();

for i in range(0, size) {
    map = map.plus(i, i);
}

if map.find(&0) == Some(1) {
    ...
}

let (without_10, size_changed_10) = map.remove(&10);
let (without_20, size_changed_20) = map.remove(&20);

for (k, v) in map.iter() {
    ...
}

```

## Performance
Looks pretty good so far, for a fully persistent data structure. The benchmarks below were done on
a Core i7-4712MQ, with random numbers and the compile flags `-C lto -C opt-level=3 -C target-feature=+popcnt`.

### Lookup
Times (in microseconds) for one thousand lookups in a collection with *ELEMENT COUNT* elements (where key and value types are u64).

| ELEMENT COUNT | HAMT    | HASHMAP |
|:--------------|:-------:|:-------:|
| 10            |      33 |      36 |
| 1000          |      40 |      54 |
| 100000        |      62 |      56 |

In percent over std::HashMap (less than 100% means faster, more means slower than std::HashMap).

| ELEMENT COUNT | HAMT     | HASHMAP  |
|:--------------|:--------:|:--------:|
| 10            |      91% |     100% |
| 1000          |      74% |     100% |
| 100000        |     110% |     100% |

The HAMT is in the same ballpark as the std::HashMap, even for larger collections.
~~Also, LLVM unfortunately does not (yet) properly translate the `cntpop` intrinsic function
(which could be just one CPU instruction on many architectures, but is translated to a much more
expensive instruction sequence currently).~~ As pointed out [on reddit](http://www.reddit.com/r/rust/comments/1xa8uy/a_persistent_map_implementation_like_in_clojure/cf9xm3a), properly configuring LLVM
(e.g. by setting the target-cpu option) is necessary for it to issue the popcnt instruction.

### Insertion
Times (in microseconds) for one thousand insertions into a collection with *ELEMENT COUNT* elements (again, key and value type is u64).

| ELEMENT COUNT | HAMT    | HASHMAP |
|:--------------|:-------:|:-------:|
| 10            |     101 |      44 |
| 1000          |     145 |     115 |
| 100000        |    1156 |      32 |

In percent over std::HashMap (less than 100% means faster, more means slower than std::HashMap).

| ELEMENT COUNT | HAMT     | HASHMAP  |
|:--------------|:--------:|:--------:|
| 10            |     229% |     100% |
| 1000          |     126% |     100% |
| 100000        |    3612% |     100% |

As can be seen, the HAMT holds up pretty well against the non-persistent std::HashMap.

In conclusion, even with (atomic, multithreaded) refcounting a HAMT can perform pretty well :)

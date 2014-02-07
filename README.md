rs-persistent-datastructures
============================

Mostly an implemention of a Hash Array Mapped Trie implementation based on the
[Ideal Hash Trees](http://lampwww.epfl.ch/papers/idealhashtrees.pdf) paper by Phil Bagwell.
This is the datastructure used by Scala's and Clojure's standard library as map implementation.
The idea to use a special *collision node* to deal with hash collisions is taken from Clojure's
implementation.

## Usage
```rust
let mut map = HamtMap::new();

for i in range(0, size) {
    map = map.plus(i, i);
}

if map.find(&0) {
    ...
}

let (without_10, size_changed_10) = map.remove(&10);
let (without_20, size_changed_20) = map.remove(&20);
```

## Performance
Looks pretty good so far, for a fully persistent data structure.

### Lookup
Times (in microseconds) for 1000 lookup in a collection with *ELEMENT COUNT* elements (key & value == u64).
The red black is also persistent and implemented in rbtree.rs 
(based on [Matt Might's article](http://matt.might.net/articles/red-black-delete/)).

| ELEMENT COUNT | HAMT | REDBLACK TREE | HASHMAP |
|:--------------|:----:|:-------------:|:-------:|
| 10            | 46   | 17            | 51      |
| 1000          | 61   | 73            | 56      |
| 100000        | 93   | 300           | 55      |

Both persistent implementations are quite fast but don't scale as well as the std::HashMap.
The HAMT is about in the same ballpark as the std::HashMap, even for larger collections.
Both the HAMT and the regular HashMap still suffer a bit from Rust's currently slow hash
function. Otherwise, I guess they would be closer to the red-black tree for small collections.
Also, LLVM unfortunately does not (yet) properly translate the `cntpop` intrinsic function
(could be just one CPU instruction on many architectures, but is translated to a much more
expensive currently).

### Insertion
Times (in microseconds) for 1000 insertions in a collection with *ELEMENT COUNT* elements (key & value == u64).

| ELEMENT COUNT | HAMT | REDBLACK TREE | HASHMAP |
|:--------------|:----:|:-------------:|:-------:|
| 10            | 184  | 1466          | 151     |
| 1000          | 205  | 1731          | 174     |
| 100000        | 1255 | 3208          | 470     |

As can be seen, the HAMT holds up pretty well against the non-persistent std::HashMap. 
Mind that all modification operations are completely thread-safe.

In conclusion, even with (atomic, multithreaded) refcounting a HAMT can perform pretty well :)

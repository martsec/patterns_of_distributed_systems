# Patterns of Distributed Systems rust implementation

Book by Unmesh Joshi


Just some of the pattern implementations in rust.

## Execution

Code is inside examples. Use `cwe {example_file}.rs`


```hash
function cwe() {
    cargo watch -q -c -x "run -q --example '$1'"
}
```

## Extra wishes

Some cool libraries that might be good to explore:

* [x] [rkyv](https://rkyv.org/) Zero-copy 
* [ ] [blake3](https://www.youtube.com/watch?v=h-0KLCAEZgY) faster hashing algorithm
* [ ] Tokyo actors to simulate multiple servers?
* [ ] Better error handling. For example
  * Usage of `thiserror` and `anyhow` or `error-stack` to surface errors better.
  * https://www.youtube.com/watch?v=s5S2Ed5T-dc
  * https://www.youtube.com/watch?v=j-VQCYP7wyw
* Benchmarking
* [ ] [cargo-mutants](https://mutants.rs/welcome.html) For discovering what the tests are missing

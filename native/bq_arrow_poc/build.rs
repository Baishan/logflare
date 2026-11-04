fn main() {
    prost_build::compile_protos(&["proto/rows.proto"], &["proto/"]).unwrap();
}

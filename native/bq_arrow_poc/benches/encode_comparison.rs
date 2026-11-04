use bq_arrow_poc::dynamic::{
    encode_from_proto_dynamic, rows_to_ndjson_dynamic, rows_to_proto_bytes_dynamic,
    sample_rows_dynamic_no_conflict,
};
use bq_arrow_poc::{encode_from_json, encode_from_proto, rows_to_ndjson, rows_to_proto_bytes, sample_rows};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

fn bench_fixed_schema(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_storage_write_encode_fixed_schema");

    for batch_size in [50usize, 500, 2000] {
        let rows = sample_rows(batch_size);
        let ndjson = rows_to_ndjson(&rows);
        let proto_bytes = rows_to_proto_bytes(&rows);

        group.bench_with_input(
            BenchmarkId::new("baseline_json_double_parse", batch_size),
            &ndjson,
            |b, ndjson| b.iter(|| encode_from_json(ndjson).unwrap()),
        );

        group.bench_with_input(
            BenchmarkId::new("candidate_protobuf", batch_size),
            &proto_bytes,
            |b, proto_bytes| b.iter(|| encode_from_proto(proto_bytes).unwrap()),
        );
    }

    group.finish();
}

/// Same comparison, but on heterogeneous metadata (varying keys per row) -
/// tests whether the speed edge survives once the candidate path has to do
/// real per-batch type inference instead of just building from a fixed,
/// known shape.
fn bench_dynamic_schema(c: &mut Criterion) {
    let mut group = c.benchmark_group("bq_storage_write_encode_dynamic_schema");

    for batch_size in [50usize, 500, 2000] {
        let rows = sample_rows_dynamic_no_conflict(batch_size);
        let ndjson = rows_to_ndjson_dynamic(&rows);
        let proto_bytes = rows_to_proto_bytes_dynamic(&rows);

        group.bench_with_input(
            BenchmarkId::new("baseline_json_double_parse", batch_size),
            &ndjson,
            |b, ndjson| b.iter(|| encode_from_json(ndjson).unwrap()),
        );

        group.bench_with_input(
            BenchmarkId::new("candidate_protobuf", batch_size),
            &proto_bytes,
            |b, proto_bytes| b.iter(|| encode_from_proto_dynamic(proto_bytes).unwrap()),
        );
    }

    group.finish();
}

criterion_group!(benches, bench_fixed_schema, bench_dynamic_schema);
criterion_main!(benches);

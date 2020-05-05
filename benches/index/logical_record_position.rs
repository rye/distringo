use criterion::{criterion_group, criterion_main, Criterion};
use distringo::LogicalRecordPositionIndex;

pub fn benchmark(c: &mut Criterion) {
	let mut lrpi = LogicalRecordPositionIndex::new_with_size(10000);
	c.bench_function("lrpi insert 1", |b| b.iter(|| lrpi.insert(1, 1)));

	c.bench_function("lrpi insert 10", |b| {
		b.iter(|| {
			for i in (1..=10).map(|n| (n, n)) {
				lrpi.insert(i.0, i.1);
			}
		})
	});

	c.bench_function("lrpi extend 10", |b| {
		b.iter(|| {
			lrpi.extend((1..=10).map(|n| (n, n)));
		})
	});

	c.bench_function("lrpi insert 100", |b| {
		b.iter(|| {
			for i in (1..=100).map(|n| (n, n)) {
				lrpi.insert(i.0, i.1);
			}
		})
	});

	c.bench_function("lrpi extend 100", |b| {
		b.iter(|| {
			lrpi.extend((1..=100).map(|n| (n, n)));
		})
	});

	c.bench_function("lrpi insert 1000", |b| {
		b.iter(|| {
			for i in (1..=1000).map(|n| (n, n)) {
				lrpi.insert(i.0, i.1);
			}
		})
	});

	c.bench_function("lrpi extend 1000", |b| {
		b.iter(|| {
			lrpi.extend((1..=1000).map(|n| (n, n)));
		})
	});
}

criterion_group!(benches, benchmark);
criterion_main!(benches);

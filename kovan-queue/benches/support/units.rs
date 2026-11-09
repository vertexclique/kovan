//! Criterion measurements in units other than the time of a whole iteration, for benches whose
//! samples each take one reading: bytes (`BYTES`) and the nanoseconds of a single round
//! (`NANOS`). A bench returns its reading times the iterations from `iter_custom`, so the mean
//! per iteration criterion reports, compares and keeps as a baseline is the reading itself.
//! The benches turn criterion's plots off: a reading that is the same in every sample (a
//! steady byte count, a quantile in one bucket) has no spread for its density plot.

#![allow(dead_code)] // each bench uses one of the units

use criterion::Throughput;
use criterion::measurement::{Measurement, ValueFormatter};

pub struct Reading {
    units: [(&'static str, f64); 4],
}

pub const BYTES: Reading = Reading {
    units: [
        ("B", 1.0),
        ("KiB", 1024.0),
        ("MiB", 1048576.0),
        ("GiB", 1073741824.0),
    ],
};
pub const NANOS: Reading = Reading {
    units: [("ns", 1.0), ("us", 1e3), ("ms", 1e6), ("s", 1e9)],
};

impl Measurement for Reading {
    type Intermediate = ();
    type Value = f64;

    fn start(&self) {}

    // Only `iter_custom` is used: every reading comes from the bench, none from here.
    fn end(&self, _: ()) -> f64 {
        0.0
    }

    fn add(&self, a: &f64, b: &f64) -> f64 {
        a + b
    }

    fn zero(&self) -> f64 {
        0.0
    }

    fn to_f64(&self, value: &f64) -> f64 {
        *value
    }

    fn formatter(&self) -> &dyn ValueFormatter {
        self
    }
}

impl ValueFormatter for Reading {
    fn scale_values(&self, typical: f64, values: &mut [f64]) -> &'static str {
        let (unit, step) = self
            .units
            .iter()
            .rev()
            .find(|(_, step)| typical >= *step)
            .copied()
            .unwrap_or(self.units[0]);
        values.iter_mut().for_each(|v| *v /= step);
        unit
    }

    fn scale_throughputs(&self, _: f64, _: &Throughput, _: &mut [f64]) -> &'static str {
        self.units[0].0
    }

    fn scale_for_machines(&self, _: &mut [f64]) -> &'static str {
        self.units[0].0
    }
}

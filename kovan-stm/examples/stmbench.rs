use kovan_stm::{Stm, TVar};
use std::time::Instant;
fn main() {
    let stm = Stm::new();
    let v = TVar::new(0u64);
    const N: u64 = 200_000;
    // read-only transactions (read_set populated, write_set empty)
    let mut best = f64::MAX;
    for _ in 0..7 {
        let t = Instant::now();
        for _ in 0..N {
            stm.atomically(|tx| {
                let x = tx.load(&v)?;
                Ok(x)
            });
        }
        let ns = t.elapsed().as_nanos() as f64 / N as f64;
        if ns < best {
            best = ns;
        }
    }
    println!("ro_txn:  {best:.0} ns/txn");
    // read+write transactions
    let mut best = f64::MAX;
    for _ in 0..7 {
        let t = Instant::now();
        for _ in 0..N {
            stm.atomically(|tx| {
                let x = tx.load(&v)?;
                tx.store(&v, x + 1)?;
                Ok(())
            });
        }
        let ns = t.elapsed().as_nanos() as f64 / N as f64;
        if ns < best {
            best = ns;
        }
    }
    println!("rw_txn:  {best:.0} ns/txn");
}

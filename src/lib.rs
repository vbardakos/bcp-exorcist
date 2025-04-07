mod reader;
use pyo3::{exceptions::asyncio::CancelledError, prelude::*};
use reader::TmpOptions;
use std::fs::{self, File};

#[pyfunction]
fn exorsize_csv(
    file: &str,
    sep: Option<u8>,
    eol: Option<u8>,
    chunk_size: Option<usize>,
) -> PyResult<()> {
    let bak = format!("{file}.bak");
    fs::rename(file, bak.as_str())?;
    let chunk_size = chunk_size.unwrap_or(1024 * 1024 * 4);

    let input = File::open(bak.as_str())?;
    let size = input.metadata()?.len();
    let output = File::create(file)?;
    let opts = TmpOptions {
        sep: sep.unwrap_or(b'\x1E'),
        eol: eol.unwrap_or(b'\x1D'),
    };

    match reader::exorsize_csv(input, output, size, chunk_size, &opts) {
        Ok(_) => {
            println!("✝️ exorcism completed ✝️");
            Ok(())
        }
        Err(e) => {
            fs::rename(file, format!("possessed_{file}"))?;
            fs::rename(bak, file)?;
            Err(PyErr::new::<CancelledError, _>(format!("✝️ exorcism failed: {e}")))
        }
    }
}

#[pymodule]
fn bcp_exorcist(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(exorsize_csv, m)?)?;
    Ok(())
}

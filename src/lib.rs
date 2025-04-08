mod reader;
use pyo3::{
    exceptions::{PyRuntimeError, PyTypeError},
    prelude::*,
};
use reader::TmpOptions;
use std::fs::{self, File};

fn unwrap_byte(input: Option<&[u8]>, default: u8) -> PyResult<u8> {
    let out = match input {
        Some(cs) if cs.len() == 1 => cs[0],
        None => default,
        Some(cs) if cs.len() == 0 => default,
        Some(cs) => {
            let msg = format!(
                "Input b'{}' should be a single byte; len: {}",
                String::from_utf8_lossy(cs),
                cs.len(),
            );
            return Err(PyErr::new::<PyTypeError, _>(msg));
        }
    };
    Ok(out)
}

/// Fixes a broken CSV file by processing it in batches.
///
/// This function receives a broken CSV file and fixes it by processing it in chunks.
/// The `filepath` is the path to the file that needs to be fixed. The `delim` and `newline`
/// parameters are the ASCII characters used as delimiters and newline characters in the broken CSV.
/// These characters are suggested to be uncommon ASCII characters. The default values are `\x1E`
/// for `delim` and `\x1D` for `newline`. The `chunk_size` parameter specifies the size of the batches
/// to process, with a default value of 4 MB.
///
/// # Arguments
///
/// * `filepath` - A string slice that holds the path to the file to be fixed.
/// * `delim` - An optional ASCII character used as the delimiter in the broken CSV. Default is `\x1E`.
/// * `newline` - An optional ASCII character used as the newline character in the broken CSV. Default is `\x1D`.
/// * `chunk_size` - An optional size for the batch size to process. Default is 4 MB.
///
///
/// # Example
///
/// ```python
/// from bcp_exorcist import exorcize_csv
///
/// try:
///     exorcize_csv("path/to/broken.csv", delim=b'\x1E', newline=b'\x1D', chunk_size=1024 * 1024)
///     print("Exorcism completed successfully!")
///
/// except TypeError as e:
///     print("params `delim` & `newline` should be a single byte;")
///     raise e
///
/// except FileNotFoundError as e:
///     print("param `filepath` is not valid;")
///     raise e
///
/// except RuntimeError as e:
///     print(f"Exorcism process failed;")
///     raise e
///
/// ```
#[pyfunction]
#[pyo3(text_signature = "(filepath, delim=None, newline=None, chunk_size=None)")]
fn exorcize_csv(
    filepath: &str,
    delim: Option<&[u8]>,
    newline: Option<&[u8]>,
    chunk_size: Option<usize>,
) -> PyResult<()> {
    let sep = unwrap_byte(delim, b'\x1E')?;
    let eol = unwrap_byte(newline, b'\x1D')?;

    let bak = format!("{filepath}.bak");
    fs::rename(filepath, bak.as_str())?;
    let chunk_size = chunk_size.unwrap_or(1024 * 1024 * 4);

    let input = File::open(bak.as_str())?;
    let size = input.metadata()?.len();
    let output = File::create(filepath)?;
    let opts = TmpOptions { sep, eol };

    match reader::exorcize_csv(input, output, size, chunk_size, &opts) {
        Ok(_) => {
            println!("✝️ exorcism completed ✝️");
            Ok(())
        }
        Err(e) => {
            fs::rename(filepath, format!("{filepath}.broken"))?;
            fs::rename(bak, filepath)?;
            Err(PyErr::new::<PyRuntimeError, _>(format!(
                "✝️ exorcism failed: {e}"
            )))
        }
    }
}

#[pymodule]
fn bcp_exorcist(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(exorcize_csv, m)?)?;
    Ok(())
}

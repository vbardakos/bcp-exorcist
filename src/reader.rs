use memchr::memchr3_iter;
use std::io::{self, BufReader, BufWriter, Read, Write};

#[derive(Debug)]
pub(crate) struct TmpOptions {
    pub(crate) sep: u8,
    pub(crate) eol: u8,
}

impl Default for TmpOptions {
    fn default() -> Self {
        TmpOptions {
            sep: b'\x1E',
            eol: b'\x1D',
        }
    }
}

#[inline(always)]
pub(crate) fn exorcize_csv<R, W>(
    input: R,
    output: W,
    size: u64,
    chunk_size: usize,
    opts: &TmpOptions,
) -> io::Result<()>
where
    R: Read,
    W: Write,
{
    let mut reader = BufReader::new(input);
    let mut writer = BufWriter::new(output);

    let mut buf = vec![0u8; chunk_size];
    let mut out = Vec::with_capacity(chunk_size * 3);

    if size > 0 {
        out.push(b'"');
    }

    loop {
        let read = reader.read(&mut buf)?;

        if read == 0 {
            break;
        }

        // write before to truncate the last
        writer.write_all(&out)?;

        // clear buffer
        out.clear();

        exorcize_csv_batch(&buf[..read], &mut out, opts.sep, opts.eol)?;
    }

    handle_closing(&mut out, &mut writer)
}

#[inline(always)]
fn exorcize_csv_batch(haystack: &[u8], buf: &mut Vec<u8>, sep: u8, eol: u8) -> io::Result<()> {
    let mut idx = 0;
    for pos in memchr3_iter(sep, eol, b'"', haystack) {
        buf.extend_from_slice(&haystack[idx..pos]);

        match haystack[pos] {
            c if c == sep => {
                if pos > 0 && haystack[pos - 1] == b'\\' {
                    buf.push(b'\\');
                }
                buf.extend_from_slice(b"\",\"");
            }
            c if c == eol => {
                if pos > 0 && haystack[pos - 1] == b'\\' {
                    buf.push(b'\\');
                }
                buf.extend_from_slice(b"\"\n\"");
            }
            _ => buf.extend_from_slice(b"\\\""),
        }

        idx = pos + 1;
    }

    if idx < haystack.len() {
        buf.extend_from_slice(&haystack[idx..]);
    }
    Ok(())
}

#[inline(always)]
fn handle_closing<W>(out: &mut Vec<u8>, writer: &mut BufWriter<W>) -> io::Result<()>
where
    W: Write,
{
    let len = out.len();
    if len > 1 {
        match out[len - 1] {
            b'"' => {
                if out[len - 2] == b'\n' {
                    out.pop();
                }
            }
            b'\n' => {}
            _ => out.push(b'"'),
        }
    }

    writer.write_all(&out)?;
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use io::{Seek, SeekFrom};
    use rstest::*;
    use std::io::Cursor;

    #[rstest]
    fn test_default_tmp_options() {
        let opts = TmpOptions::default();
        assert_eq!(opts.sep, b'\x1E');
        assert_eq!(opts.eol, b'\x1D');
    }

    #[rstest]
    fn test_exorcize_csv_empty_input() {
        let input = Cursor::new(Vec::new());
        let output = Vec::new();
        let opts = TmpOptions::default();

        let result = exorcize_csv(input, output, 0, 1024, &opts);
        assert!(result.is_ok());
    }

    #[rstest]
    fn test_exorcize_csv_basic() {
        let input_data = b"field1\x1Efield2\x1Dfield3";
        let input = Cursor::new(input_data.to_vec());
        let mut output = Cursor::new(Vec::new());
        let opts = TmpOptions::default();

        let result = exorcize_csv(input, &mut output, input_data.len() as u64, 1024, &opts);
        assert!(result.is_ok());

        let result = output.seek(SeekFrom::Start(0));
        assert!(result.is_ok());

        let mut result = String::new();
        output.read_to_string(&mut result).unwrap();
        let expected_output = "\"field1\",\"field2\"\n\"field3\"";
        assert_eq!(result, expected_output);
    }

    #[rstest]
    #[case(
        b"field1\\\x1Efield2\\\x1Dfield3",
        "\"field1\\\\\",\"field2\\\\\"\n\"field3\""
    )]
    fn test_exorcize_csv_with_escape_characters(#[case] data: &[u8], #[case] expected: &str) {
        let input = Cursor::new(data.to_vec());
        let mut output = Cursor::new(Vec::new());
        let opts = TmpOptions::default();

        let result = exorcize_csv(input, &mut output, data.len() as u64, 1024, &opts);
        assert!(result.is_ok());

        let result = output.seek(SeekFrom::Start(0));
        assert!(result.is_ok());

        let mut result = String::new();
        output.read_to_string(&mut result).unwrap();
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case("field1\x1Efield2\x1Efield3\x1D", "field1\",\"field2\",\"field3\"\n\"")]
    #[case(
        "field1\x1Dfield2\x1Dfield3\x1D",
        "field1\"\n\"field2\"\n\"field3\"\n\""
    )]
    #[case(
        "\"\"field\",\"field\",field\"\x1Efield3\x1D",
        "\\\"\\\"field\\\",\\\"field\\\",field\\\"\",\"field3\"\n\""
    )]
    #[case("\x1E\x1E\x1D", "\",\"\",\"\"\n\"")]
    #[case("\\\x1E\\\x1E\\\x1D", "\\\\\",\"\\\\\",\"\\\\\"\n\"")]
    #[case("\0\x1E\0\x1E\0\x1D", "\0\",\"\0\",\"\0\"\n\"")]
    fn test_exorcize_csv_batch(#[case] haystack: &str, #[case] expected: &str) {
        let mut buf = Vec::new();
        let sep = b'\x1E';
        let eol = b'\x1D';

        let result = exorcize_csv_batch(haystack.as_bytes(), &mut buf, sep, eol);
        assert!(result.is_ok());
        assert_eq!(buf, expected.as_bytes());
    }

    #[rstest]
    #[case("field1\",\"field2\"", "field1\",\"field2\"")]
    #[case("field1\",\"field2\"\n", "field1\",\"field2\"\n")]
    #[case("field1\",\"field2\"\n\"", "field1\",\"field2\"\n")]
    #[case("field1\",\"field2", "field1\",\"field2\"")]
    fn test_handle_closing(#[case] buf: &str, #[case] exp: &str) {
        let mut writer = BufWriter::new(Vec::new());

        let result = handle_closing(&mut buf.as_bytes().to_vec(), &mut writer);
        assert!(result.is_ok());

        assert_eq!(writer.into_inner().unwrap(), exp.as_bytes());
    }
}

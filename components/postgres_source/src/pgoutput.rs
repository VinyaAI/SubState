//! Minimal pgoutput decoder (insert / update / delete / relation / begin / commit).

use anyhow::{bail, Context, Result};
use serde_json::{Map, Number, Value};

#[derive(Debug, Clone)]
pub struct Relation {
    pub rel_id: u32,
    pub schema: String,
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleValue {
    Null,
    Unchanged,
    Text(String),
}

#[derive(Debug, Clone)]
pub enum PgOutput {
    Begin { final_lsn: u64 },
    Commit { commit_lsn: u64 },
    Relation(Relation),
    Insert { rel_id: u32, tuple: Vec<TupleValue> },
    Update { rel_id: u32, new_tuple: Vec<TupleValue> },
    Delete { rel_id: u32, key_tuple: Vec<TupleValue> },
    Other,
}

pub fn parse_messages(mut data: &[u8]) -> Result<Vec<PgOutput>> {
    let mut out = Vec::new();
    while !data.is_empty() {
        out.push(parse_one(&mut data)?);
    }
    Ok(out)
}

fn parse_one(buf: &mut &[u8]) -> Result<PgOutput> {
    let tag = read_u8(buf)?;
    match tag {
        b'B' => {
            let final_lsn = read_u64(buf)?;
            let _ts = read_u64(buf)?;
            let _xid = read_u32(buf)?;
            Ok(PgOutput::Begin { final_lsn })
        }
        b'C' => {
            let _flags = read_u8(buf)?;
            let commit_lsn = read_u64(buf)?;
            let _end = read_u64(buf)?;
            let _ts = read_u64(buf)?;
            Ok(PgOutput::Commit { commit_lsn })
        }
        b'R' => Ok(PgOutput::Relation(parse_relation(buf)?)),
        b'I' => {
            let rel_id = read_u32(buf)?;
            let kind = read_u8(buf)?;
            if kind != b'N' {
                bail!("pgoutput insert missing new tuple");
            }
            Ok(PgOutput::Insert {
                rel_id,
                tuple: parse_tuple(buf)?,
            })
        }
        b'U' => {
            let rel_id = read_u32(buf)?;
            let mut kind = read_u8(buf)?;
            if kind == b'K' || kind == b'O' {
                let _old = parse_tuple(buf)?;
                kind = read_u8(buf)?;
            }
            if kind != b'N' {
                bail!("pgoutput update missing new tuple");
            }
            Ok(PgOutput::Update {
                rel_id,
                new_tuple: parse_tuple(buf)?,
            })
        }
        b'D' => {
            let rel_id = read_u32(buf)?;
            let kind = read_u8(buf)?;
            if kind != b'K' && kind != b'O' {
                bail!("pgoutput delete missing key/old tuple");
            }
            Ok(PgOutput::Delete {
                rel_id,
                key_tuple: parse_tuple(buf)?,
            })
        }
        b'T' | b'Y' | b'M' | b'O' => {
            // Truncate / type / message / origin — ignore remainder of this record.
            *buf = &[];
            Ok(PgOutput::Other)
        }
        _ => {
            *buf = &[];
            Ok(PgOutput::Other)
        }
    }
}

fn parse_relation(buf: &mut &[u8]) -> Result<Relation> {
    let rel_id = read_u32(buf)?;
    let schema = read_cstring(buf)?;
    let name = read_cstring(buf)?;
    let _replica_identity = read_u8(buf)?;
    let natts = read_u16(buf)? as usize;
    let mut columns = Vec::with_capacity(natts);
    for _ in 0..natts {
        let _flags = read_u8(buf)?;
        columns.push(read_cstring(buf)?);
        let _typid = read_u32(buf)?;
        let _typmod = read_u32(buf)?;
    }
    Ok(Relation {
        rel_id,
        schema,
        name,
        columns,
    })
}

fn parse_tuple(buf: &mut &[u8]) -> Result<Vec<TupleValue>> {
    let natts = read_u16(buf)? as usize;
    let mut values = Vec::with_capacity(natts);
    for _ in 0..natts {
        let tag = read_u8(buf)?;
        match tag {
            b'n' => values.push(TupleValue::Null),
            b'u' => values.push(TupleValue::Unchanged),
            b't' | b'b' => {
                let len = read_u32(buf)? as usize;
                if buf.len() < len {
                    bail!("pgoutput tuple value truncated");
                }
                let bytes = &buf[..len];
                *buf = &buf[len..];
                let text = String::from_utf8_lossy(bytes).into_owned();
                values.push(TupleValue::Text(text));
            }
            other => bail!("unknown pgoutput tuple tag {}", other),
        }
    }
    Ok(values)
}

pub fn tuple_to_object(columns: &[String], values: &[TupleValue]) -> Map<String, Value> {
    let mut map = Map::new();
    for (name, value) in columns.iter().zip(values.iter()) {
        match value {
            TupleValue::Null | TupleValue::Unchanged => {
                map.insert(name.clone(), Value::Null);
            }
            TupleValue::Text(text) => {
                map.insert(name.clone(), text_to_json(text));
            }
        }
    }
    map
}

pub fn text_to_json(text: &str) -> Value {
    if text == "true" {
        return Value::Bool(true);
    }
    if text == "false" {
        return Value::Bool(false);
    }
    if let Ok(n) = text.parse::<i64>() {
        return Value::Number(n.into());
    }
    if let Ok(n) = text.parse::<f64>() {
        if let Some(num) = Number::from_f64(n) {
            return Value::Number(num);
        }
    }
    Value::String(text.to_string())
}

fn read_u8(buf: &mut &[u8]) -> Result<u8> {
    if buf.is_empty() {
        bail!("unexpected eof");
    }
    let value = buf[0];
    *buf = &buf[1..];
    Ok(value)
}

fn read_u16(buf: &mut &[u8]) -> Result<u16> {
    if buf.len() < 2 {
        bail!("unexpected eof");
    }
    let value = u16::from_be_bytes([buf[0], buf[1]]);
    *buf = &buf[2..];
    Ok(value)
}

fn read_u32(buf: &mut &[u8]) -> Result<u32> {
    if buf.len() < 4 {
        bail!("unexpected eof");
    }
    let value = u32::from_be_bytes(buf[..4].try_into().context("u32")?);
    *buf = &buf[4..];
    Ok(value)
}

fn read_u64(buf: &mut &[u8]) -> Result<u64> {
    if buf.len() < 8 {
        bail!("unexpected eof");
    }
    let value = u64::from_be_bytes(buf[..8].try_into().context("u64")?);
    *buf = &buf[8..];
    Ok(value)
}

fn read_cstring(buf: &mut &[u8]) -> Result<String> {
    let pos = buf
        .iter()
        .position(|&b| b == 0)
        .context("unterminated cstring")?;
    let s = std::str::from_utf8(&buf[..pos])
        .context("cstring utf8")?
        .to_string();
    *buf = &buf[pos + 1..];
    Ok(s)
}

pub fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn as u32)
}

pub fn parse_lsn(text: &str) -> Result<u64> {
    let (hi, lo) = text
        .split_once('/')
        .with_context(|| format!("invalid LSN '{text}'"))?;
    let high = u64::from_str_radix(hi, 16).with_context(|| format!("LSN hi '{hi}'"))?;
    let low = u64::from_str_radix(lo, 16).with_context(|| format!("LSN lo '{lo}'"))?;
    Ok((high << 32) | low)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_roundtrip() {
        let lsn = 0x0000_0016_00B3_748u64;
        assert_eq!(parse_lsn(&format_lsn(lsn)).unwrap(), lsn);
        assert_eq!(parse_lsn("0/16B3748").unwrap(), 0x16B3748);
    }

    #[test]
    fn text_to_json_guesses_scalars() {
        assert_eq!(text_to_json("12"), Value::from(12));
        assert_eq!(text_to_json("true"), Value::Bool(true));
        assert_eq!(text_to_json("nashville"), Value::String("nashville".into()));
    }

    #[test]
    fn parses_relation_and_insert() {
        let mut msg = Vec::new();
        msg.push(b'R');
        msg.extend_from_slice(&42u32.to_be_bytes());
        msg.extend_from_slice(b"public\0");
        msg.extend_from_slice(b"drivers\0");
        msg.push(0); // replica identity
        msg.extend_from_slice(&2u16.to_be_bytes());
        // col id
        msg.push(1);
        msg.extend_from_slice(b"id\0");
        msg.extend_from_slice(&23u32.to_be_bytes());
        msg.extend_from_slice(&(-1i32).to_be_bytes());
        // col name
        msg.push(0);
        msg.extend_from_slice(b"name\0");
        msg.extend_from_slice(&25u32.to_be_bytes());
        msg.extend_from_slice(&(-1i32).to_be_bytes());

        msg.push(b'I');
        msg.extend_from_slice(&42u32.to_be_bytes());
        msg.push(b'N');
        msg.extend_from_slice(&2u16.to_be_bytes());
        msg.push(b't');
        msg.extend_from_slice(&1u32.to_be_bytes());
        msg.push(b'1');
        msg.push(b't');
        let name = b"Alice";
        msg.extend_from_slice(&(name.len() as u32).to_be_bytes());
        msg.extend_from_slice(name);

        let parsed = parse_messages(&msg).unwrap();
        assert_eq!(parsed.len(), 2);
        match &parsed[0] {
            PgOutput::Relation(rel) => {
                assert_eq!(rel.name, "drivers");
                assert_eq!(rel.columns, vec!["id", "name"]);
            }
            other => panic!("{other:?}"),
        }
        match &parsed[1] {
            PgOutput::Insert { rel_id, tuple } => {
                assert_eq!(*rel_id, 42);
                assert_eq!(tuple[1], TupleValue::Text("Alice".into()));
            }
            other => panic!("{other:?}"),
        }
    }
}

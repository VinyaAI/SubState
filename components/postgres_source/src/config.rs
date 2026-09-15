//! Snapshot-only Postgres settings. Process env lives in the CLI.

#[derive(Debug, Clone)]
pub struct Config {
    pub schema: String,
    pub tables: Option<Vec<String>>,
}

impl Config {
    pub fn new(schema: impl Into<String>, tables: Option<Vec<String>>) -> Self {
        Self {
            schema: schema.into(),
            tables,
        }
    }
}

pub fn parse_tables(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|table| !table.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::parse_tables;

    #[test]
    fn parse_tables_splits_and_trims() {
        assert_eq!(
            parse_tables(" drivers, jobs , "),
            vec!["drivers".to_string(), "jobs".to_string()]
        );
    }

    #[test]
    fn parse_tables_empty_is_empty() {
        assert!(parse_tables("").is_empty());
        assert!(parse_tables("  , , ").is_empty());
    }
}

//! The RFC4180 CSV parser behind the `csv` node's `spec: "rfc"` mode.
//!
//! Node-RED does not parse RFC mode with its own hand-written loop: it vendors a small CSV library
//! (`@node-red/nodes/core/parsers/lib/csv/index.js`) and drives it with options. This module is a
//! port of that library, so the node above it can stay a direct port of `70-CSV.js` too.
//!
//! The library's own notes describe where it is deliberately more forgiving than RFC4180: any
//! separator and quote character is accepted, `\r`, `\n` and `\r\n` all end a record, and only
//! single-character separators/quotes are supported. Data rows are parsed leniently (a quote in the
//! middle of a field is kept) while the column template and the header row are parsed strictly,
//! which is exactly the difference the upstream specs pin down.

use std::collections::BTreeMap;

use serde_json::Number;

use crate::runtime::model::{Variant, VariantObjectMap};
use crate::*;

/// Where the parser writes the rows: arrays of cells, or objects keyed by the headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputStyle {
    Array,
    #[default]
    Object,
}

/// One parsed cell.
///
/// The library keeps JavaScript's distinction between a cell that was not there at all (`null`, an
/// empty unquoted field) and one that held an empty string (`""`): `includeNullValues` and
/// `includeEmptyStrings` are told apart by exactly that difference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cell {
    Null,
    Text(String),
}

impl Cell {
    /// The cell as text, with `null` reading as an empty string (what a template or header row
    /// needs).
    pub fn text(&self) -> &str {
        match self {
            Cell::Null => "",
            Cell::Text(text) => text,
        }
    }

    fn is_empty_text(&self) -> bool {
        matches!(self, Cell::Text(text) if text.is_empty())
    }
}

/// The parser's options, mirroring `CSVParseOptions` of the vendored library.
#[derive(Debug, Clone)]
pub struct ParseOptions {
    pub cursor: usize,
    pub separator: char,
    pub quote: char,
    pub headers_only: bool,
    pub headers: Vec<String>,
    pub data_has_header_row: bool,
    pub output_header: bool,
    pub parse_numeric: bool,
    pub include_null_values: bool,
    pub include_empty_strings: bool,
    pub output_style: OutputStyle,
    pub strict: bool,
}

impl Default for ParseOptions {
    fn default() -> Self {
        Self {
            cursor: 0,
            separator: ',',
            quote: '"',
            headers_only: false,
            headers: Vec::new(),
            data_has_header_row: true,
            output_header: true,
            parse_numeric: false,
            include_null_values: false,
            include_empty_strings: true,
            output_style: OutputStyle::Object,
            strict: false,
        }
    }
}

/// The parsed result, mirroring the library's `CSVParseResult`.
#[derive(Debug, Clone, Default)]
pub struct ParseResult {
    /// The column names: the supplied headers, the first row, or generated `col1`, `col2`, ...
    pub headers: Vec<String>,
    /// The data when `output_style` is [`OutputStyle::Array`].
    pub rows: Vec<Vec<Cell>>,
    /// The data when `output_style` is [`OutputStyle::Object`].
    pub objects: Vec<VariantObjectMap>,
    /// Whether the first row of `rows` was consumed as the header row.
    pub first_row_is_header: bool,
    /// Where the parser stopped, so a caller can continue from there (a char index).
    pub cursor: usize,
}

/// Whether the cell has to be quoted: any quoteable character means the whole cell is wrapped, and a
/// quote inside it is doubled (RFC4180 2.6).
///
/// `quoteables` is the list of characters that trigger quoting, which the node tailors per call site
/// (a template is always comma-and-quote sensitive, an output cell follows the node's separator).
pub fn quote_cell(cell: &str, quote: char, quoteables: &[String]) -> String {
    let double_up = cell.contains(quote);
    let quote_char = if quoteables.iter().any(|q| cell.contains(q.as_str())) { quote } else { '\0' };
    let body = if double_up { cell.replace('"', "\"\"") } else { cell.to_string() };
    if quote_char == '\0' { body } else { format!("{quote_char}{body}{quote_char}") }
}

/// The quoteable characters the library falls back to when a call site does not name any.
pub fn default_quoteables(quote: char, separator: &str) -> Vec<String> {
    vec![quote.to_string(), separator.to_string(), "\r".to_string(), "\n".to_string()]
}

/// `templateArrayToColumnString()` of `70-CSV.js`: the column names as one string.
///
/// `keep_empty_columns` keeps empty columns and quotes with the node's output separator; without it
/// the string is the library's `header` - comma separated, empty columns dropped.
pub fn template_array_to_column_string(
    template: &[String],
    keep_empty_columns: bool,
    separator: &str,
    quoteables: &[String],
    quote: char,
) -> String {
    if keep_empty_columns {
        template.iter().map(|cell| quote_cell(cell, quote, quoteables)).collect::<Vec<_>>().join(separator)
    } else {
        let strict_quotables = default_quoteables(quote, ",");
        template
            .iter()
            .filter(|cell| !cell.is_empty())
            .map(|cell| quote_cell(cell, quote, &strict_quotables))
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// `columnStringToTemplateArray()` of `70-CSV.js`: parse a column template.
///
/// The template is parsed strictly, so a quote in the middle of a field - or data after a closing
/// quote - is an error. `Ok(None)` is the library's "not a usable template" (it did not parse to
/// exactly one row), which the node turns into its empty `['']` template.
pub fn template_from_string(template: &str, separator: char, quote: char) -> crate::Result<Option<Vec<String>>> {
    let options =
        ParseOptions { separator, quote, output_style: OutputStyle::Array, strict: true, ..Default::default() };
    let parsed = parse(template, &options)?;
    Ok(if parsed.rows.len() == 1 {
        Some(parsed.rows[0].iter().map(|cell| cell.text().to_string()).collect())
    } else {
        None
    })
}

/// Convert a char index into a byte index, so a caller can slice the input it parsed.
pub fn char_index_to_byte_index(input: &str, char_index: usize) -> usize {
    input.char_indices().nth(char_index).map(|(index, _)| index).unwrap_or(input.len())
}

/// Parse `input`, mirroring the vendored library's `parse()`.
pub fn parse(input: &str, options: &ParseOptions) -> crate::Result<ParseResult> {
    let chars: Vec<char> = input.chars().collect();
    let separator = options.separator;
    let quote = options.quote;

    let mut cursor = options.cursor.min(chars.len());
    let mut new_cell = true;
    let mut in_quote = false;
    let mut closed = false;
    let mut output: Vec<Vec<Cell>> = Vec::new();
    let mut row: Vec<Cell> = Vec::new();
    let mut cell = String::new();

    while cursor < chars.len() {
        let ch = chars[cursor];
        if in_quote {
            if ch == quote && chars.get(cursor + 1) == Some(&quote) {
                // An escaped quote inside a quoted cell.
                cell.push(quote);
                cursor += 2;
                new_cell = false;
                closed = false;
            } else if ch == quote {
                in_quote = false;
                cursor += 1;
                new_cell = false;
                closed = true;
            } else {
                cell.push(ch);
                new_cell = false;
                closed = false;
                cursor += 1;
            }
        } else if ch == separator {
            finalise_cell(&mut row, &mut cell, new_cell);
            cursor += 1;
            new_cell = true;
            closed = false;
        } else if ch == quote {
            if new_cell {
                in_quote = true;
                cursor += 1;
                new_cell = false;
                closed = false;
            } else if options.strict {
                return Err(invalid(format!("Quote found in the middle of an unquoted field, cursor {cursor}")));
            } else {
                // Lenient mode keeps a single quote when it is not followed by the end of the cell
                // or the record.
                cursor += 1;
                if let Some(&next) = chars.get(cursor)
                    && next != '\n'
                    && next != '\r'
                    && next != separator
                {
                    cell.push(ch);
                    if next == quote {
                        cursor += 1;
                    }
                }
            }
        } else if ch == '\n' || ch == '\r' {
            finalise_row(&mut output, &mut row, &mut cell, new_cell);
            if chars.get(cursor + 1) == Some(&'\n') {
                cursor += 2;
            } else {
                cursor += 1;
            }
            new_cell = true;
            closed = false;
            if options.headers_only {
                break;
            }
        } else if closed {
            if options.strict {
                return Err(invalid(format!("Data found after closing quote, cursor {cursor}")));
            }
            // Move back so the discarded character is read again as data.
            cursor -= 1;
            closed = false;
        } else {
            cell.push(ch);
            new_cell = false;
            cursor += 1;
        }
    }

    if options.strict && in_quote {
        return Err(invalid("Missing quote, unclosed cell".to_string()));
    }
    finalise_row(&mut output, &mut row, &mut cell, new_cell);

    let mut headers = options.headers.clone();
    let mut first_row_is_header = false;
    if !output.is_empty() {
        if !headers.is_empty() {
            // headers already supplied
        } else if options.data_has_header_row {
            headers.extend(output[0].iter().map(|cell| cell.text().to_string()));
            first_row_is_header = true;
        } else {
            for index in 0..output[0].len() {
                headers.push(format!("col{}", index + 1));
            }
        }
    }

    let mut result = ParseResult { headers, first_row_is_header, cursor, ..Default::default() };

    if options.output_style == OutputStyle::Array || options.headers_only {
        if !first_row_is_header && !options.headers_only && options.output_header && !result.headers.is_empty() {
            if output.is_empty() {
                output = vec![result.headers.iter().map(|h| Cell::Text(h.clone())).collect()];
            } else {
                output.insert(0, result.headers.iter().map(|h| Cell::Text(h.clone())).collect());
            }
            first_row_is_header = true;
        }
        if options.headers_only {
            result.first_row_is_header = false;
            return Ok(result);
        }
        result.first_row_is_header = first_row_is_header;
        result.rows = if first_row_is_header && !options.output_header { output.split_off(1) } else { output };
        return Ok(result);
    }

    result.objects = to_objects(&output, &result.headers, first_row_is_header, options);
    result.first_row_is_header = false;
    Ok(result)
}

/// The library's object output: one object per row, honouring the null/empty-string switches and
/// converting numeric cells when asked to.
fn to_objects(
    output: &[Vec<Cell>],
    headers: &[String],
    first_row_is_header: bool,
    options: &ParseOptions,
) -> Vec<VariantObjectMap> {
    let mut objects = Vec::new();
    let start = usize::from(first_row_is_header);
    for row in output.iter().skip(start) {
        let mut row_object: VariantObjectMap = BTreeMap::new();
        let mut is_empty = true;
        for (index, header) in headers.iter().enumerate() {
            if header.is_empty() {
                continue;
            }
            let cell = row.get(index).cloned().unwrap_or(Cell::Null);
            let mut value = match &cell {
                Cell::Null => Variant::Null,
                Cell::Text(text) => Variant::String(text.clone()),
            };
            if matches!(value, Variant::Null) && !options.include_null_values {
                continue;
            }
            if cell.is_empty_text() && !options.include_empty_strings {
                continue;
            }
            if options.parse_numeric
                && let Variant::String(text) = &value
                && !text.is_empty()
                && !skip_number_conversion(text)
                && let Some(number) = js_number(text)
            {
                value = Variant::Number(number);
            }
            row_object.insert(header.clone(), value);
            is_empty = false;
        }
        if !is_empty {
            objects.push(row_object);
        }
    }
    objects
}

/// The library's "leave numbers starting with 0, e and + as strings" rule, which is its inverted
/// spelling of the legacy mode's number pattern: skip a cell starting with `+`, with `0` followed by
/// a digit, or with `-0` followed by a digit.
fn skip_number_conversion(text: &str) -> bool {
    let trimmed = text.trim_start_matches(' ');
    let bytes = trimmed.as_bytes();
    match bytes.first() {
        Some(b'+') => true,
        Some(b'-') => bytes.len() >= 3 && bytes[1] == b'0' && bytes[2].is_ascii_digit(),
        Some(b'0') => bytes.len() >= 2 && bytes[1].is_ascii_digit(),
        _ => false,
    }
}

/// JavaScript's `+value` coercion, as far as a JSON number can carry it.
///
/// `Number()` also understands the `0x`/`0o`/`0b` literals and yields `Infinity` for some inputs;
/// neither survives JSON, so those cells stay strings here (the upstream specs only use decimal and
/// scientific notation).
fn js_number(text: &str) -> Option<Number> {
    let value: f64 = text.trim().parse().ok()?;
    if !value.is_finite() {
        return None;
    }
    if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        Some(Number::from(value as i64))
    } else {
        Number::from_f64(value)
    }
}

fn finalise_cell(row: &mut Vec<Cell>, cell: &mut String, new_cell: bool) {
    let text = std::mem::take(cell);
    // A cell with no characters at all is JavaScript's `null` when nothing was opened, and an empty
    // string when it came from `""`.
    row.push(if !text.is_empty() {
        Cell::Text(text)
    } else if new_cell {
        Cell::Null
    } else {
        Cell::Text(String::new())
    });
}

fn finalise_row(output: &mut Vec<Vec<Cell>>, row: &mut Vec<Cell>, cell: &mut String, new_cell: bool) {
    if !cell.is_empty() {
        finalise_cell(row, cell, new_cell);
    }
    if !row.is_empty() {
        output.push(std::mem::take(row));
    }
}

fn invalid(message: String) -> anyhow::Error {
    EdgelinkError::InvalidOperation(message).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object_honours_null_and_empty() {
        let options = ParseOptions {
            headers: vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into()],
            parse_numeric: true,
            ..Default::default()
        };
        // `"1",,"+3",,"-05"`: the quoted empty string is kept as an empty string, the empty
        // unquoted fields are `null` and are dropped.
        let parsed = parse("\"1\",,\"+3\",,\"-05\"\n", &options).unwrap();
        let row = &parsed.objects[0];
        assert_eq!(row.get("a"), Some(&Variant::from(1)));
        assert!(!row.contains_key("b"));
        assert_eq!(row.get("c"), Some(&Variant::from("+3")));
        assert!(!row.contains_key("d"));
        assert_eq!(row.get("e"), Some(&Variant::from("-05")));
    }

    #[test]
    fn parses_objects_with_nulls_and_empty_strings() {
        object_honours_null_and_empty();

        let options = ParseOptions {
            headers: vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into()],
            include_null_values: true,
            include_empty_strings: true,
            parse_numeric: true,
            ..Default::default()
        };
        let parsed = parse("\"1\",\"\",\"+3\",\"\",\"-05\"\n", &options).unwrap();
        let row = &parsed.objects[0];
        assert_eq!(row.get("b"), Some(&Variant::String(String::new())));
        assert_eq!(row.get("d"), Some(&Variant::String(String::new())));
    }

    #[test]
    fn keeps_cr_and_lf_inside_quotes() {
        let options = ParseOptions { headers: vec!["a".into(), "b".into()], ..Default::default() };
        let parsed = parse("\"with a\nnew line\",\"and\ryes\"\n", &options).unwrap();
        let row = &parsed.objects[0];
        assert_eq!(row.get("a"), Some(&Variant::from("with a\nnew line")));
        assert_eq!(row.get("b"), Some(&Variant::from("and\ryes")));
    }

    #[test]
    fn lenient_mode_keeps_quotes_in_the_middle_of_a_field() {
        let options = ParseOptions { headers: vec!["a".into(), "b".into()], ..Default::default() };
        let parsed = parse("\"with,a\"n,odd\n", &options).unwrap();
        assert_eq!(parsed.objects[0].get("a"), Some(&Variant::from("with,a\"n")));
        assert_eq!(parsed.objects[0].get("b"), Some(&Variant::from("odd")));
    }

    #[test]
    fn strict_mode_rejects_an_unquoted_quote_and_data_after_a_quote() {
        let strict = ParseOptions { strict: true, output_style: OutputStyle::Array, ..Default::default() };
        assert!(parse("\"a\",  \"b\" \n", &strict).is_err());
        assert!(parse("\"a\" ,b\n", &strict).is_err());
        // A clean template parses to exactly one row.
        let parsed = parse("a,b b,\"c,c\",\" d, d \"\n", &strict).unwrap();
        assert_eq!(parsed.rows.len(), 1);
        assert_eq!(parsed.rows[0].iter().map(Cell::text).collect::<Vec<_>>(), vec!["a", "b b", "c,c", " d, d "]);
    }

    #[test]
    fn numbers_starting_with_zero_or_plus_stay_strings() {
        let options = ParseOptions {
            headers: vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into(), "f".into()],
            parse_numeric: true,
            ..Default::default()
        };
        let parsed = parse("123,0123,+123,e123,E123,-123\n", &options).unwrap();
        let row = &parsed.objects[0];
        assert_eq!(row.get("a"), Some(&Variant::from(123)));
        assert_eq!(row.get("b"), Some(&Variant::from("0123")));
        assert_eq!(row.get("c"), Some(&Variant::from("+123")));
        assert_eq!(row.get("d"), Some(&Variant::from("e123")));
        assert_eq!(row.get("e"), Some(&Variant::from("E123")));
        assert_eq!(row.get("f"), Some(&Variant::from(-123)));
    }

    #[test]
    fn header_string_quotes_and_drops_empty_columns() {
        let template = vec!["a".to_string(), "b b".to_string(), "c,c".to_string(), " d, d ".to_string()];
        let quotables = default_quoteables('"', ",");
        assert_eq!(template_array_to_column_string(&template, false, ",", &quotables, '"'), "a,b b,\"c,c\",\" d, d \"");
        let with_empty = vec!["a".to_string(), String::new(), "d".to_string()];
        assert_eq!(template_array_to_column_string(&with_empty, false, ",", &quotables, '"'), "a,d");
    }
}

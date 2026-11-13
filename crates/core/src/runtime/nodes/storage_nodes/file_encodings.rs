//! Compact single-byte maps generated from the pinned iconv-lite implementation.
//! No Node.js or system conversion library is needed at runtime.

#[rustfmt::skip]
#[path = "file_encodings_data.rs"]
mod data;

type Table = (&'static [u16; 256], &'static [(u16, u8)]);

fn table(label: &str) -> Option<Table> {
    let normalized: String = label.chars().filter(char::is_ascii_alphanumeric).flat_map(char::to_lowercase).collect();
    data::LABELS.iter().find(|(name, _)| *name == normalized).map(|(_, index)| data::TABLES[*index])
}

pub(super) fn supports(label: &str) -> bool {
    table(label).is_some()
}

pub(super) fn encode(text: &str, label: &str) -> Option<Vec<u8>> {
    let (_, encode) = table(label)?;
    Some(
        text.encode_utf16()
            .map(|unit| {
                encode.binary_search_by_key(&unit, |(code, _)| *code).map(|index| encode[index].1).unwrap_or(b'?')
            })
            .collect(),
    )
}

pub(super) fn decode(bytes: &[u8], label: &str) -> Option<String> {
    let (decode, _) = table(label)?;
    Some(String::from_utf16_lossy(&bytes.iter().map(|byte| decode[usize::from(*byte)]).collect::<Vec<_>>()))
}

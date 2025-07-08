/// Equals to Javascript's `parseFloat`
pub fn parse_float_lossy<T: std::str::FromStr>(input: &str) -> Option<T> {
    let input = input.trim();
    let mut end_idx = 0;
    let mut has_decimal_point = false;
    let mut has_exponent = false;
    let mut has_digits = false;

    for (i, c) in input.char_indices() {
        if c.is_ascii_digit() {
            has_digits = true;
            end_idx = i + 1;
        } else if c == '.' && !has_decimal_point && !has_exponent {
            has_decimal_point = true;
            end_idx = i + 1;
        } else if (c == 'e' || c == 'E') && has_digits && !has_exponent {
            has_exponent = true;
            has_digits = false;
            end_idx = i + 1;
        } else if (c == '+' || c == '-')
            && (i == 0 || input.chars().nth(i - 1).map(|p| p == 'e' || p == 'E').unwrap_or(false))
        {
            end_idx = i + 1;
        } else {
            break;
        }
    }

    if has_exponent && !has_digits {
        end_idx =
            input.char_indices().take_while(|&(_, c)| c != 'e' && c != 'E').map(|(i, _)| i).last().map_or(0, |i| i + 1);
    }

    if end_idx > 0 { input[..end_idx].parse::<T>().ok() } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_f64() {
        assert_eq!(parse_float_lossy::<f64>("123.456"), Some(123.456));
        assert_eq!(parse_float_lossy::<f64>("-123.456"), Some(-123.456));
        assert_eq!(parse_float_lossy::<f64>("1.23e+10"), Some(1.23e10));
        assert_eq!(parse_float_lossy::<f64>("+12.34"), Some(12.34));
    }

    #[test]
    fn test_valid_f32() {
        assert_eq!(parse_float_lossy::<f32>("123.456"), Some(123.456_f32));
        assert_eq!(parse_float_lossy::<f32>("-123.456"), Some(-123.456_f32));
        assert_eq!(parse_float_lossy::<f32>("1.23e+10"), Some(1.23e10_f32));
        assert_eq!(parse_float_lossy::<f32>("+12.34"), Some(12.34_f32));
    }

    #[test]
    fn test_with_whitespace() {
        assert_eq!(parse_float_lossy::<f64>("  123.456  "), Some(123.456));
        assert_eq!(parse_float_lossy::<f64>("  -12.34  "), Some(-12.34));
    }

    #[test]
    fn test_invalid_numbers() {
        assert_eq!(parse_float_lossy::<f64>("abc"), None);
        assert_eq!(parse_float_lossy::<f64>("abc123"), None);
        assert_eq!(parse_float_lossy::<f64>("123abc"), Some(123.0));
        assert_eq!(parse_float_lossy::<f64>(""), None);
        assert_eq!(parse_float_lossy::<f64>("5.0 deg"), Some(5.0));
        assert_eq!(parse_float_lossy::<f64>("6 deg"), Some(6.0));
    }

    #[test]
    fn test_edge_cases() {
        assert_eq!(parse_float_lossy::<f64>("."), None);
        assert_eq!(parse_float_lossy::<f64>("..123"), None);
        assert_eq!(parse_float_lossy::<f64>("123..456"), Some(123.0));
        assert_eq!(parse_float_lossy::<f64>("1.23e"), Some(1.23));
    }
}

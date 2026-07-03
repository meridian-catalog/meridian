//! `${ENV_VAR}` interpolation over bundle string values.
//!
//! Every string in a parsed YAML document is scanned for `${NAME}` references
//! and each is replaced with the value of the named environment variable. This
//! keeps secrets (webhook signing secrets, storage credentials) out of the
//! committed bundle file — the file references them by name; the values arrive
//! from the environment at parse time.
//!
//! Rules:
//! - `${NAME}` expands to the value of `NAME`; an undefined variable is an
//!   error (fail closed — better than silently sending an empty secret).
//! - `$${` is a literal `${` escape, for the rare value that needs the
//!   sequence verbatim.
//! - A malformed reference (`${` with no closing `}`) is an error.
//! - `NAME` is `[A-Za-z_][A-Za-z0-9_]*`, the POSIX environment-variable shape.

use super::BundleError;

/// Recursively interpolates every string in a YAML value in place.
pub(crate) fn interpolate_value(
    value: &mut serde_yaml::Value,
    resolve_env: &dyn Fn(&str) -> Option<String>,
) -> Result<(), BundleError> {
    match value {
        serde_yaml::Value::String(s) => {
            *s = interpolate_str(s, resolve_env)?;
            Ok(())
        }
        serde_yaml::Value::Sequence(items) => {
            for item in items {
                interpolate_value(item, resolve_env)?;
            }
            Ok(())
        }
        serde_yaml::Value::Mapping(map) => {
            // Interpolate values only; keys are field names, not data.
            for (_key, val) in map.iter_mut() {
                interpolate_value(val, resolve_env)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Expands all `${NAME}` references in a single string.
fn interpolate_str(
    input: &str,
    resolve_env: &dyn Fn(&str) -> Option<String>,
) -> Result<String, BundleError> {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'$' {
            // Escape: `$${` -> literal `${`.
            if i + 1 < bytes.len() && bytes[i + 1] == b'$' {
                out.push('$');
                i += 2;
                continue;
            }
            if i + 1 < bytes.len() && bytes[i + 1] == b'{' {
                let close = input[i + 2..].find('}').ok_or_else(|| {
                    BundleError::msg(format!(
                        "unterminated ${{...}} reference in value {input:?}"
                    ))
                })?;
                let name = &input[i + 2..i + 2 + close];
                if !is_valid_env_name(name) {
                    return Err(BundleError::msg(format!(
                        "invalid environment variable name {name:?} in value {input:?}"
                    )));
                }
                let resolved = resolve_env(name).ok_or_else(|| {
                    BundleError::msg(format!(
                        "environment variable {name:?} referenced by the bundle is not set"
                    ))
                })?;
                out.push_str(&resolved);
                i += 2 + close + 1;
                continue;
            }
        }
        // Not a reference: copy this byte as part of a UTF-8 char.
        let ch = input[i..].chars().next().expect("valid char boundary");
        out.push(ch);
        i += ch.len_utf8();
    }

    Ok(out)
}

/// True for a POSIX-shaped environment variable name.
fn is_valid_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |name: &str| map.get(name).cloned()
    }

    #[test]
    fn expands_single_reference() {
        let resolve = env(&[("SECRET", "s3cr3t")]);
        assert_eq!(interpolate_str("${SECRET}", &resolve).unwrap(), "s3cr3t");
    }

    #[test]
    fn expands_embedded_and_multiple() {
        let resolve = env(&[("HOST", "example.com"), ("PORT", "443")]);
        assert_eq!(
            interpolate_str("https://${HOST}:${PORT}/hook", &resolve).unwrap(),
            "https://example.com:443/hook"
        );
    }

    #[test]
    fn leaves_plain_strings_untouched() {
        let resolve = env(&[]);
        assert_eq!(
            interpolate_str("s3://bucket/prefix", &resolve).unwrap(),
            "s3://bucket/prefix"
        );
    }

    #[test]
    fn escape_yields_literal() {
        let resolve = env(&[]);
        assert_eq!(
            interpolate_str("price is $${amount}", &resolve).unwrap(),
            "price is ${amount}"
        );
    }

    #[test]
    fn undefined_variable_is_error() {
        let resolve = env(&[]);
        assert!(interpolate_str("${MISSING}", &resolve).is_err());
    }

    #[test]
    fn unterminated_reference_is_error() {
        let resolve = env(&[("X", "1")]);
        assert!(interpolate_str("${X", &resolve).is_err());
    }

    #[test]
    fn invalid_name_is_error() {
        let resolve = env(&[]);
        assert!(interpolate_str("${1BAD}", &resolve).is_err());
        assert!(interpolate_str("${a-b}", &resolve).is_err());
    }

    #[test]
    fn recurses_into_mapping_and_sequence() {
        let resolve = env(&[("A", "x"), ("B", "y")]);
        let mut value: serde_yaml::Value =
            serde_yaml::from_str("k: ${A}\nlist:\n  - ${B}\n  - plain").unwrap();
        interpolate_value(&mut value, &resolve).unwrap();
        let k = value.get("k").and_then(serde_yaml::Value::as_str).unwrap();
        assert_eq!(k, "x");
        let list = value
            .get("list")
            .and_then(serde_yaml::Value::as_sequence)
            .unwrap();
        assert_eq!(list[0].as_str().unwrap(), "y");
        assert_eq!(list[1].as_str().unwrap(), "plain");
    }
}

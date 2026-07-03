//! S3 session-policy templates.
//!
//! The policy attached to an STS `AssumeRole` call *intersects* with the
//! role's own policy, so the vended session can never exceed the role — and
//! the templates here never grant anything beyond the one table prefix:
//!
//! - object actions (`GetObject`; plus `PutObject`/`DeleteObject` for
//!   read-write) on `arn:aws:s3:::{bucket}/{prefix}/*` only;
//! - `ListBucket` on the bucket only under a `s3:prefix` condition pinned
//!   to the table prefix.
//!
//! No statement uses a wildcard broader than the table prefix.

use serde_json::json;

use crate::AccessMode;

/// Renders the session policy for one table prefix.
///
/// `bucket` and `key_prefix` come from a parsed [`crate::TableScope`]
/// (`key_prefix` has no leading/trailing slash and is never empty).
#[must_use]
pub fn s3_session_policy(bucket: &str, key_prefix: &str, access: AccessMode) -> String {
    let object_actions = match access {
        AccessMode::Read => json!(["s3:GetObject"]),
        AccessMode::ReadWrite => json!(["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]),
    };
    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": object_actions,
                "Resource": [format!("arn:aws:s3:::{bucket}/{key_prefix}/*")],
            },
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": [format!("arn:aws:s3:::{bucket}")],
                "Condition": {
                    "StringLike": {
                        "s3:prefix": [
                            key_prefix,
                            format!("{key_prefix}/*"),
                        ],
                    },
                },
            },
        ],
    });
    policy.to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    fn parse(policy: &str) -> Value {
        serde_json::from_str(policy).expect("policy is valid JSON")
    }

    #[test]
    fn read_policy_is_exactly_get_and_scoped_list() {
        let policy = parse(&s3_session_policy("bkt", "wh/ns/t-uuid", AccessMode::Read));
        let expected = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject"],
                    "Resource": ["arn:aws:s3:::bkt/wh/ns/t-uuid/*"],
                },
                {
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket"],
                    "Resource": ["arn:aws:s3:::bkt"],
                    "Condition": {
                        "StringLike": { "s3:prefix": ["wh/ns/t-uuid", "wh/ns/t-uuid/*"] },
                    },
                },
            ],
        });
        assert_eq!(policy, expected);
    }

    #[test]
    fn read_write_policy_adds_put_and_delete_only() {
        let policy = parse(&s3_session_policy("bkt", "p", AccessMode::ReadWrite));
        assert_eq!(
            policy["Statement"][0]["Action"],
            serde_json::json!(["s3:GetObject", "s3:PutObject", "s3:DeleteObject"])
        );
        // The list statement is identical to the read policy's.
        assert_eq!(
            policy["Statement"][1],
            parse(&s3_session_policy("bkt", "p", AccessMode::Read))["Statement"][1]
        );
    }

    #[test]
    fn no_wildcard_escapes_the_table_prefix() {
        let policy = s3_session_policy("bkt", "wh/ns/t-uuid", AccessMode::ReadWrite);
        let value = parse(&policy);
        for statement in value["Statement"].as_array().expect("statements") {
            for resource in statement["Resource"].as_array().expect("resources") {
                let arn = resource.as_str().expect("arn string");
                assert!(
                    arn == "arn:aws:s3:::bkt" || arn.starts_with("arn:aws:s3:::bkt/wh/ns/t-uuid/"),
                    "resource {arn:?} escapes the table prefix"
                );
            }
        }
        // The only `*` anywhere sits behind the table prefix.
        for (index, _) in policy.match_indices('*') {
            let lead = &policy[..index];
            assert!(
                lead.ends_with("bkt/wh/ns/t-uuid/") || lead.ends_with("wh/ns/t-uuid/"),
                "wildcard not anchored to the table prefix: ...{}",
                &policy[index.saturating_sub(30)..=index]
            );
        }
    }
}

//! AWS Signature Version 4 verification for the S3 test fixture.
//!
//! Only what a header-signed, empty-body object request needs: SHA-256,
//! HMAC-SHA256 and the canonical request. No date or HMAC dependency.

use sha2::{Digest, Sha256};

/// SHA-256 of the empty body, the payload hash of every GET and HEAD.
pub const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

use crate::Credentials;

/// The parts of one request that the signature covers.
#[derive(Debug, Clone, Copy)]
pub struct SigningInput<'a> {
    pub method: &'a str,
    /// The `Host` header value, with the port when it is not the default.
    pub host: &'a str,
    /// The absolute request path before URI encoding, e.g. `/bucket/key`.
    pub path: &'a str,
    /// Further headers to sign, as lowercase name and value.
    pub headers: &'a [(&'a str, &'a str)],
    pub payload_sha256: &'a str,
    pub region: &'a str,
    pub unix_secs: u64,
}

/// The headers a signed request must carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedHeaders {
    pub authorization: String,
    pub amz_date: String,
}

/// Sign `input` for the `s3` service.
pub fn sign(credentials: &Credentials, input: &SigningInput<'_>) -> SignedHeaders {
    let (amz_date, date_stamp) = format_amz_time(input.unix_secs);
    sign_at(credentials, input, &amz_date, &date_stamp)
}

/// Sign `input` with an explicit `x-amz-date`, as a server does to check a
/// signature it received. The date stamp is the first eight characters.
pub fn sign_with_date(
    credentials: &Credentials,
    input: &SigningInput<'_>,
    amz_date: &str,
) -> SignedHeaders {
    let date_stamp = amz_date.get(..8).unwrap_or(amz_date);
    sign_at(credentials, input, amz_date, date_stamp)
}

fn sign_at(
    credentials: &Credentials,
    input: &SigningInput<'_>,
    amz_date: &str,
    date_stamp: &str,
) -> SignedHeaders {
    const SERVICE: &str = "s3";
    let scope = format!("{date_stamp}/{}/{SERVICE}/aws4_request", input.region);

    let mut headers: Vec<(&str, &str)> = vec![
        ("host", input.host),
        ("x-amz-content-sha256", input.payload_sha256),
        ("x-amz-date", amz_date),
    ];
    headers.extend_from_slice(input.headers);
    headers.sort_by_key(|(name, _)| *name);

    let canonical_headers: String = headers
        .iter()
        .map(|(name, value)| format!("{name}:{}\n", value.trim()))
        .collect();
    let signed_headers = headers
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(";");

    let canonical_request = format!(
        "{}\n{}\n\n{canonical_headers}\n{signed_headers}\n{}",
        input.method,
        canonical_uri(input.path),
        input.payload_sha256,
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex::encode(sha256(canonical_request.as_bytes())),
    );

    let k_date = hmac_sha256(
        format!("AWS4{}", credentials.secret_access_key).as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, input.region.as_bytes());
    let k_service = hmac_sha256(&k_region, SERVICE.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));

    SignedHeaders {
        authorization: format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            credentials.access_key_id,
        ),
        amz_date: amz_date.to_string(),
    }
}

/// The request path with each segment URI-encoded per the SigV4 rules.
pub fn canonical_uri(path: &str) -> String {
    path.split('/')
        .map(uri_encode_segment)
        .collect::<Vec<_>>()
        .join("/")
}

fn sha256(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut block = [0u8; BLOCK];
    if key.len() > BLOCK {
        block[..32].copy_from_slice(&sha256(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(block.map(|b| b ^ 0x36));
    inner.update(data);
    let inner: [u8; 32] = inner.finalize().into();
    let mut outer = Sha256::new();
    outer.update(block.map(|b| b ^ 0x5c));
    outer.update(inner);
    outer.finalize().into()
}

/// Unreserved characters (`A-Za-z0-9-_.~`) pass through; everything else
/// becomes `%XX` in uppercase.
fn uri_encode_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for b in segment.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Format a Unix timestamp (UTC) as `("YYYYMMDDTHHMMSSZ", "YYYYMMDD")`, using
/// Howard Hinnant's `civil_from_days`.
fn format_amz_time(unix_secs: u64) -> (String, String) {
    let days = (unix_secs / 86_400) as i64;
    let sod = unix_secs % 86_400;
    let (hour, minute, second) = (sod / 3600, (sod % 3600) / 60, sod % 60);

    let z = days + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };

    let date_stamp = format!("{year:04}{month:02}{day:02}");
    let amz_date = format!("{date_stamp}T{hour:02}{minute:02}{second:02}Z");
    (amz_date, date_stamp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_formatter_known_timestamps() {
        assert_eq!(
            format_amz_time(1_369_353_600),
            ("20130524T000000Z".into(), "20130524".into())
        );
        assert_eq!(
            format_amz_time(0),
            ("19700101T000000Z".into(), "19700101".into())
        );
        assert_eq!(
            format_amz_time(1_782_049_507),
            ("20260621T134507Z".into(), "20260621".into())
        );
    }

    #[test]
    fn hmac_matches_rfc_4231_case_2() {
        assert_eq!(
            hex::encode(hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn empty_payload_constant_is_the_sha256_of_nothing() {
        assert_eq!(hex::encode(sha256(b"")), EMPTY_PAYLOAD_SHA256);
    }

    /// The "GET Object" example of the AWS SigV4 documentation for S3.
    #[test]
    fn ranged_get_matches_the_aws_documented_signature() {
        let credentials = Credentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
        };
        let signed = sign(
            &credentials,
            &SigningInput {
                method: "GET",
                host: "examplebucket.s3.amazonaws.com",
                path: "/test.txt",
                headers: &[("range", "bytes=0-9")],
                payload_sha256: EMPTY_PAYLOAD_SHA256,
                region: "us-east-1",
                unix_secs: 1_369_353_600,
            },
        );
        assert_eq!(signed.amz_date, "20130524T000000Z");
        assert_eq!(
            signed.authorization,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn canonical_uri_encodes_each_segment() {
        assert_eq!(canonical_uri("/bucket/a b/c"), "/bucket/a%20b/c");
    }
}

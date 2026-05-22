use super::*;

#[test]
fn sign_then_verify_roundtrip() {
    let key = HmacKey([7u8; 32]);
    let payload = b"hello world";
    let tag = sign(&key, payload);
    assert_eq!(tag.len(), AUTH_TAG_LEN);
    verify(&key, payload, &tag).expect("verify ok");
}

#[test]
fn verify_rejects_wrong_key() {
    let payload = b"hello world";
    let tag = sign(&HmacKey([7u8; 32]), payload);
    assert_eq!(
        verify(&HmacKey([8u8; 32]), payload, &tag),
        Err(DecodeError::AuthenticationFailed),
    );
}

#[test]
fn verify_rejects_tampered_payload() {
    let key = HmacKey([7u8; 32]);
    let payload = b"hello world";
    let tag = sign(&key, payload);
    assert_eq!(
        verify(&key, b"hello world!", &tag),
        Err(DecodeError::AuthenticationFailed),
    );
}

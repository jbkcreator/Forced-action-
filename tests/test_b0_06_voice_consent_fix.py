"""Self-check for the B0-06 PR-review fixes: server-owned voice-consent
disclosures (deps.resolve_voice_consent) and the compliance gate that
requires text+version alongside the timestamp.
"""
from src.api.deps import ConsentAcceptanceRequest, resolve_voice_consent


def _consent(**overrides):
    base = dict(terms_accepted=True)
    base.update(overrides)
    return ConsentAcceptanceRequest(**base)


def test_resolve_voice_consent_recognized_version():
    consent = _consent(voice_consent_accepted=True, voice_consent_version="2026.06")
    result = resolve_voice_consent(consent)
    assert result is not None
    text, version = result
    assert version == "2026.06"
    assert text  # server-owned disclosure text, not client-supplied


def test_resolve_voice_consent_ignores_client_supplied_text():
    consent = _consent(
        voice_consent_accepted=True,
        voice_consent_version="2026.06",
        voice_consent_text="whatever the client sent",
    )
    text, _ = resolve_voice_consent(consent)
    assert text != "whatever the client sent"


def test_resolve_voice_consent_missing_version():
    consent = _consent(voice_consent_accepted=True, voice_consent_version=None)
    assert resolve_voice_consent(consent) is None


def test_resolve_voice_consent_blank_version():
    consent = _consent(voice_consent_accepted=True, voice_consent_version="")
    assert resolve_voice_consent(consent) is None


def test_resolve_voice_consent_unrecognized_version():
    consent = _consent(voice_consent_accepted=True, voice_consent_version="not-a-real-version")
    assert resolve_voice_consent(consent) is None


def test_resolve_voice_consent_not_accepted():
    consent = _consent(voice_consent_accepted=False, voice_consent_version="2026.06")
    assert resolve_voice_consent(consent) is None


def test_resolve_voice_consent_no_consent_object():
    assert resolve_voice_consent(None) is None


if __name__ == "__main__":
    test_resolve_voice_consent_recognized_version()
    test_resolve_voice_consent_ignores_client_supplied_text()
    test_resolve_voice_consent_missing_version()
    test_resolve_voice_consent_blank_version()
    test_resolve_voice_consent_unrecognized_version()
    test_resolve_voice_consent_not_accepted()
    test_resolve_voice_consent_no_consent_object()
    print("OK")

"""LLM-assist tests: the default no-op proves nothing calls a network.

Critical invariant: with the default (no-op) fallback, an unsupported construct
stays unsupported. No test constructs a real provider or reaches a network.
"""

from __future__ import annotations

from meridian_sidecar import core
from meridian_sidecar.llm_assist import NoopLlmAssist, get_llm_assist
from meridian_sidecar.schemas import Status


def test_default_fallback_is_noop_and_unavailable():
    llm = get_llm_assist()
    assert llm.available is False


def test_noop_translate_returns_none():
    llm = NoopLlmAssist()
    result = llm.translate(
        sql="SELECT weird_construct()",
        from_dialect="spark",
        to_dialect="trino",
        error="boom",
    )
    assert result is None


def test_unsupported_stays_unsupported_with_noop_fallback():
    # SQLGlot raises; the no-op fallback cannot help -> unsupported, no network.
    resp = core.transpile(
        sql="SELECT FROM WHERE )(",
        from_dialect="spark",
        to_dialect="trino",
        llm=NoopLlmAssist(),
    )
    assert resp.status == Status.unsupported
    assert resp.sql is None


def test_provider_env_does_not_enable_network_by_default(monkeypatch):
    # Even if an operator sets a provider without a real adapter registered, the
    # factory must not enable a network-calling fallback.
    monkeypatch.setenv("MERIDIAN_LLM_ASSIST_PROVIDER", "anthropic")
    llm = get_llm_assist()
    assert llm.available is False


class _FakeAssist:
    """A stand-in that pretends to be a configured provider — but is pure,
    in-memory, and never touches a network. Proves the best-effort labelling
    path without any real LLM.
    """

    available = True

    def translate(self, *, sql, from_dialect, to_dialect, error):
        # Return a valid, parseable target-dialect statement.
        return "SELECT 1 AS recovered"


def test_fallback_result_is_best_effort_never_verified():
    resp = core.transpile(
        sql="SELECT FROM WHERE )(",
        from_dialect="spark",
        to_dialect="trino",
        llm=_FakeAssist(),
    )
    assert resp.status == Status.best_effort
    assert resp.sql == "SELECT 1 AS recovered"
    assert any(d.code == "llm_assist_used" for d in resp.diagnostics)


def test_invalid_fallback_result_discarded():
    class _BadAssist:
        available = True

        def translate(self, *, sql, from_dialect, to_dialect, error):
            return "SELECT FROM )("  # does not parse

    resp = core.transpile(
        sql="SELECT FROM WHERE )(",
        from_dialect="spark",
        to_dialect="trino",
        llm=_BadAssist(),
    )
    assert resp.status == Status.unsupported
    assert resp.sql is None

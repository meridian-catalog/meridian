"""LLM-assist fallback interface for transpilation.

THE RULE (non-negotiable): universal-view transpilation is DETERMINISTIC
SQLGlot first. This module is *only* a fallback for constructs SQLGlot cannot
translate. Its output is ALWAYS labeled ``best_effort`` and ALWAYS validated by
parse-back in the caller — never trusted blindly.

By default the fallback is a no-op that returns ``None`` (i.e. "still
unsupported"). It calls no network and reads no API key. A real provider is
wired in ONLY when the operator sets a BYO API key env var (OpenAI / Anthropic /
Bedrock / self-hosted). With no key configured, the fallback is simply
unavailable and the transpile status stays ``unsupported``.

Tests MUST use the default no-op. Nothing here may call an external LLM in a
test or by default.
"""

from __future__ import annotations

import os
from typing import Protocol


class LlmAssist(Protocol):
    """Pluggable transpilation fallback.

    An implementation receives a construct SQLGlot could not translate and
    returns a best-effort translation string, or ``None`` if it cannot help.
    The caller is responsible for validating (parse-back) and labelling any
    non-None result ``best_effort``.
    """

    @property
    def available(self) -> bool:
        """True only if the provider is configured and may be called."""
        ...

    def translate(
        self,
        *,
        sql: str,
        from_dialect: str,
        to_dialect: str,
        error: str,
    ) -> str | None:
        """Return a best-effort translation, or None if no help is possible."""
        ...


class NoopLlmAssist:
    """The default. Never configured, never calls out, always returns None.

    This is what proves "no network call without a key": ``available`` is always
    False and ``translate`` unconditionally returns ``None``.
    """

    @property
    def available(self) -> bool:
        return False

    def translate(
        self,
        *,
        sql: str,
        from_dialect: str,
        to_dialect: str,
        error: str,
    ) -> str | None:
        return None


# --- BYO-key wiring ---------------------------------------------------------
#
# To enable a real fallback, an operator sets ONE of these env vars to select a
# provider, plus the provider's API key:
#
#   MERIDIAN_LLM_ASSIST_PROVIDER = openai | anthropic | bedrock | self_hosted
#   (+ the matching key, e.g. ANTHROPIC_API_KEY / OPENAI_API_KEY, or an
#    endpoint URL for self_hosted)
#
# A provider adapter implements the ``LlmAssist`` protocol, constructs its
# client lazily from those env vars, and returns None on any error so a provider
# outage degrades to ``unsupported`` rather than failing the request. The
# concrete adapters are added in wave 2; the protocol above is the stable seam.
#
# ``get_llm_assist`` is the single factory the app calls at startup. Until an
# adapter is registered it always returns the no-op — so the default build
# cannot reach a network no matter what env is set.

_PROVIDER_ENV = "MERIDIAN_LLM_ASSIST_PROVIDER"


def get_llm_assist() -> LlmAssist:
    """Return the configured fallback, or the no-op default.

    Today: always the no-op (no adapters registered). The provider env var is
    read only to log intent; it never causes a network client to be built here.
    """
    provider = os.environ.get(_PROVIDER_ENV)
    if not provider:
        return NoopLlmAssist()
    # An adapter for `provider` would be constructed here in wave 2. Until then,
    # selecting a provider does NOT silently fall back to a network call: we
    # honour the safety default and return the no-op.
    return NoopLlmAssist()

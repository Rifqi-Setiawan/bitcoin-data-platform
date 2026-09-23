from unittest.mock import patch, MagicMock
from bitcoin_data_platform.committee.laya_guardrail import LayaCommitteeGuardrail, SemanticAuditVerdict


def test_laya_guardrail_pass_consistent():
    guardrail = LayaCommitteeGuardrail()
    verdict = guardrail.audit_memorandum(
        market_regime="NEUTRAL_CHOP",
        proposed_action="STANDARD_DCA",
        consensus_score=0.05,
        executive_summary="DCA normal dieksekusi",
        macro_thesis="Pasar netral",
        valuation_thesis="Mayer multiple wajar",
        technical_thesis="Volatilitas moderat",
    )
    assert verdict.status == "PASS"
    assert verdict.contradiction_detected is False


def test_laya_guardrail_quarantine_on_direct_contradiction():
    guardrail = LayaCommitteeGuardrail()
    verdict = guardrail.audit_memorandum(
        market_regime="CRISIS",
        proposed_action="EMERGENCY_HALT",
        consensus_score=-0.8,
        executive_summary="Sangat disarankan beli agresif sekarang juga!",
        macro_thesis="Pasar hancur",
        valuation_thesis="Undervalued",
        technical_thesis="Crash",
    )
    assert verdict.status == "QUARANTINE"
    assert verdict.contradiction_detected is True
    assert "Direct semantic contradiction" in verdict.explanation


@patch("urllib.request.urlopen")
def test_laya_guardrail_mocked_laya_contradiction(mock_urlopen):
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.read.return_value = b'{"data": {"answers": {"is_contradictory": {"noul": 0.88}}}}'
    mock_urlopen.return_value.__enter__.return_value = mock_resp

    guardrail = LayaCommitteeGuardrail()
    verdict = guardrail.audit_memorandum(
        market_regime="HIGH_VOL_RISK_OFF",
        proposed_action="DEFENSIVE_HOLD",
        consensus_score=-0.4,
        executive_summary="Kondisi pasar sangat tenang dan stabil tanpa risiko",
        macro_thesis="Stabil",
        valuation_thesis="Tenang",
        technical_thesis="Aman",
    )
    assert verdict.status == "REVIEW"
    assert verdict.contradiction_detected is True

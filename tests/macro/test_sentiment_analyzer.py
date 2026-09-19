"""Unit tests for lexical sentiment analysis and black swan sentinel (Group C)."""

from bitcoin_data_platform.macro.models import (
    AlertSeverity,
    MacroPillar,
)
from bitcoin_data_platform.macro.sentiment_analyzer import SentimentAnalyzer


def test_regulatory_pillar_classification() -> None:
    """19. Correctly categorizes SEC lawsuits and regulatory actions into REGULATORY pillar."""
    analyzer = SentimentAnalyzer()
    pillar, severity, polarity, tokens = analyzer.analyze_text(
        "SEC sues crypto exchange over unregistered securities and files lawsuit."
    )
    assert pillar == MacroPillar.REGULATORY
    assert severity == AlertSeverity.HIGH
    assert polarity < 0.0
    assert "sec sues" in tokens or "sec lawsuit" in tokens


def test_security_exploit_classification() -> None:
    """20. Detects exchange hacks and protocol bridge exploits into SECURITY_EXPLOIT pillar."""
    analyzer = SentimentAnalyzer()
    pillar, severity, polarity, tokens = analyzer.analyze_text(
        "Cross-chain bridge vulnerability discovered as protocol bug exploited by attacker."
    )
    assert pillar == MacroPillar.SECURITY_EXPLOIT
    assert polarity < 0.0
    assert "bridge vulnerability" in tokens or "protocol bug" in tokens


def test_critical_severity_black_swan() -> None:
    """21. Matches exchange insolvency and bankruptcy terms as CRITICAL severity."""
    analyzer = SentimentAnalyzer()
    pillar, severity, polarity, tokens = analyzer.analyze_text(
        "Major crypto exchange halts withdrawals amid sudden insolvency and bankruptcy filing."
    )
    assert severity == AlertSeverity.CRITICAL
    assert polarity < -0.5
    assert "halts withdrawals" in tokens or "insolvency" in tokens or "bankruptcy" in tokens


def test_institutional_adoption_polarity() -> None:
    """22. Scores ETF inflows and sovereign treasury allocations as positive."""
    analyzer = SentimentAnalyzer()
    pillar, severity, polarity, tokens = analyzer.analyze_text(
        "Record inflows seen as spot Bitcoin ETF inflows reach $1 billion in "
        "new treasury allocation."
    )
    assert pillar == MacroPillar.INSTITUTIONAL
    assert polarity > 0.0
    assert "record inflows" in tokens or "treasury allocation" in tokens or "etf inflow" in tokens


def test_general_neutral_news() -> None:
    """23. Scores generic commentary without matched keywords as GENERAL neutral."""
    analyzer = SentimentAnalyzer()
    pillar, severity, polarity, tokens = analyzer.analyze_text(
        "Bitcoin developers discuss standard technical block size proposals at "
        "developer conference."
    )
    assert pillar == MacroPillar.GENERAL
    assert severity == AlertSeverity.LOW
    assert polarity == 0.0
    assert tokens == []


def test_regex_boundary_matching() -> None:
    """24. Prevents partial word false positives (e.g. 'asset' or 'sector' vs 'sec')."""
    analyzer = SentimentAnalyzer()
    # The word "asset" or "second" should not match regex for "sec"
    pillar, severity, polarity, tokens = analyzer.analyze_text(
        "Digital asset sector moves forward in second quarter with no regulatory news."
    )
    assert "sec" not in tokens
    assert severity != AlertSeverity.CRITICAL


def test_polarity_saturation_clamp() -> None:
    """25. Ensures polarity is strictly bounded within [-1.0, +1.0] despite extreme scores."""
    analyzer = SentimentAnalyzer()
    extreme_text = (
        "doj indictment nationwide outright ban criminal charges asset freezing order "
        "exchange insolvency bank run stablecoin depeg halts withdrawals"
    )
    _, _, polarity, _ = analyzer.analyze_text(extreme_text)
    assert -1.0 <= polarity <= 1.0
    assert polarity == -1.0 or polarity < -0.99


def test_mixed_sentiment_resolution() -> None:
    """26. Balances mixed positive and negative keyword matches."""
    analyzer = SentimentAnalyzer()
    text = "Company announces treasury allocation despite receiving minor subpoena."
    pillar, _, polarity, tokens = analyzer.analyze_text(text)
    assert len(tokens) >= 2
    # The treasury allocation (+2.0) should outweigh the minor subpoena (-1.0)
    assert polarity > 0.0

import pytest

from research.state.money import CurrencyMismatchError, FxRate, Money


def test_same_currency_addition_sums_amounts():
    total = Money(1.5, "SOL") + Money(2.5, "SOL")
    assert total == Money(4.0, "SOL")


def test_mismatched_currency_addition_raises():
    with pytest.raises(CurrencyMismatchError):
        Money(1.0, "SOL") + Money(1.0, "USDC")


def test_mismatched_currency_subtraction_raises():
    with pytest.raises(CurrencyMismatchError):
        Money(1.0, "SOL") - Money(1.0, "USDC")


def test_convert_requires_explicit_rate_with_matching_base():
    rate = FxRate(rate=150.0, base="SOL", quote="USDC", source="binance", status="live")
    converted = Money(2.0, "SOL").convert(rate)
    assert converted == Money(300.0, "USDC")


def test_convert_rejects_mismatched_rate_base():
    rate = FxRate(rate=150.0, base="SOL", quote="USDC", source="binance", status="live")
    with pytest.raises(ValueError):
        Money(2.0, "BONK").convert(rate)

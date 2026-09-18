from __future__ import annotations

from dataclasses import dataclass


class CurrencyMismatchError(ValueError):
    def __init__(self, left: str, right: str):
        super().__init__(
            f"cannot combine amounts in {left!r} and {right!r} "
            "without an explicit FxRate conversion"
        )
        self.left = left
        self.right = right


@dataclass(frozen=True, slots=True)
class FxRate:
    """An explicit, attributable conversion rate. Never constructed implicitly."""

    rate: float
    base: str
    quote: str
    source: str
    status: str


@dataclass(frozen=True, slots=True)
class Money:
    """A price or quantity paired with its currency/unit. Never a bare float."""

    amount: float
    currency: str

    def __add__(self, other: object) -> Money:
        if not isinstance(other, Money):
            return NotImplemented
        if other.currency != self.currency:
            raise CurrencyMismatchError(self.currency, other.currency)
        return Money(self.amount + other.amount, self.currency)

    def __sub__(self, other: object) -> Money:
        if not isinstance(other, Money):
            return NotImplemented
        if other.currency != self.currency:
            raise CurrencyMismatchError(self.currency, other.currency)
        return Money(self.amount - other.amount, self.currency)

    def convert(self, rate: FxRate) -> Money:
        if rate.base != self.currency:
            raise ValueError(
                f"FxRate base {rate.base!r} does not match amount currency {self.currency!r}"
            )
        return Money(self.amount * rate.rate, rate.quote)

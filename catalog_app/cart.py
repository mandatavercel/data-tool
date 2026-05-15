"""세션 기반 장바구니 — 회사명 set으로 추적."""
from __future__ import annotations

import streamlit as st


_CART_KEY = "_catalog_cart"


def get_cart() -> set[str]:
    if _CART_KEY not in st.session_state:
        st.session_state[_CART_KEY] = set()
    return st.session_state[_CART_KEY]


def add_to_cart(company: str) -> None:
    cart = get_cart()
    cart.add(company)
    st.session_state[_CART_KEY] = cart


def remove_from_cart(company: str) -> None:
    cart = get_cart()
    cart.discard(company)
    st.session_state[_CART_KEY] = cart


def clear_cart() -> None:
    st.session_state[_CART_KEY] = set()


def in_cart(company: str) -> bool:
    return company in get_cart()


def cart_size() -> int:
    return len(get_cart())

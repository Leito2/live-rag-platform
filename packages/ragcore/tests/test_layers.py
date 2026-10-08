"""Enforces the hexagonal-lite dependency rule (ADR-0002): imports point inward only."""
import ast
import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "src" / "ragcore"

# layer → first-party layers and third-party packages it may import (stdlib is always allowed)
ALLOWED = {
    "domain": ({"ragcore.domain"}, {"pydantic"}),
    "ports": ({"ragcore.domain"}, set()),
    "application": ({"ragcore.domain", "ragcore.ports"}, None),  # None = any third-party (e.g. langgraph)
}


def layer_of(path: Path) -> str | None:
    rel = path.relative_to(SRC).parts
    name = rel[0].removesuffix(".py")
    return name if name in ALLOWED else None


def imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    mods = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            mods.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            mods.add(node.module)
    return mods


MODULES = [p for p in SRC.rglob("*.py") if layer_of(p)]


@pytest.mark.parametrize("path", MODULES, ids=lambda p: str(p.relative_to(SRC)))
def test_imports_point_inward(path: Path):
    first_party_ok, third_party_ok = ALLOWED[layer_of(path)]
    for mod in imported_modules(path):
        top = mod.split(".")[0]
        if top == "ragcore":
            inward = any(mod == ok or mod.startswith(ok + ".") for ok in first_party_ok)
            assert inward, f"{mod} crosses a layer"
        elif top not in sys.stdlib_module_names and third_party_ok is not None:
            assert top in third_party_ok, f"{mod} is I/O or framework code; it belongs in an adapter"

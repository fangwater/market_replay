from __future__ import annotations

from dataclasses import dataclass
from collections import defaultdict
import re


QUALIFIER_PATTERN = re.compile(r"(?:^|;)([^;]*)\[([^\]]+)\]")
MAPPING_VERSION = "cme_tas_action_v2"


@dataclass(frozen=True)
class ActionMapping:
    action: str
    is_inferred: bool
    rule: str


def qualifier_fields(qualifiers: str) -> dict[str, set[str]]:
    fields: dict[str, set[str]] = defaultdict(set)
    for raw_value, name in QUALIFIER_PATTERN.findall(qualifiers):
        value = raw_value.strip().strip('"').strip()
        fields[name].add(value)
    return fields


def action_mappings(qualifiers: str) -> tuple[ActionMapping, ...]:
    fields = qualifier_fields(qualifiers)
    trade_types = fields.get("TRD_TYPE", set())
    phases = fields.get("INST_PHASE", set())
    mappings: list[ActionMapping] = []
    if "4" in trade_types and "C" in phases:
        mappings.append(ActionMapping("CLOSE", False, "TRD_TYPE=4;INST_PHASE=C"))
    if "21" in trade_types and "O" in phases:
        mappings.append(
            ActionMapping("PRE_OPEN", False, "TRD_TYPE=21;INST_PHASE=O")
        )

    if "15" in trade_types:
        mappings.append(
            ActionMapping("NEW_PRICE_INDICATION", False, "TRD_TYPE=15")
        )
    if "17" in trade_types and "T" in phases:
        mappings.append(
            ActionMapping("TRADING", False, "TRD_TYPE=17;INST_PHASE=T")
        )
    return tuple(mappings)

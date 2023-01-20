from dataclasses import dataclass
from typing import List, Dict, Any

from etl.filter.condition import Condition


#########################################################################

@dataclass
class Filter:
    """
    To every item object found, a filter would be applied according to a list of Condition.
    Attributes
    ----------
    conditions: List[Condition]
        A list of conditions that will be use for discarding item results.
    """
    conditions: List[Condition]

    def apply(self, result: Dict[str, Any]) -> bool:
        for cond in self.conditions:
            if not cond.satisfies(result):
                return False
        return True

    def apply_to_all(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return list(filter(lambda r: self.apply(r), results))

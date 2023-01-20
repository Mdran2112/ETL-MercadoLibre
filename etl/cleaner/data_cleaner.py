from dataclasses import dataclass
from typing import List, Dict, Any, Optional


@dataclass
class DataCleaner:
    relevant_keys: Optional[List[str]] = None
    drop_keys: Optional[List[str]] = None

    def __post_init__(self):
        if not self.relevant_keys and not self.drop_keys:
            raise ValueError("relevant_keys or drop_keys must be defined.")

    def clean(self, atribs: Dict[str, Any]) -> Dict[str, Any]:
        keys_to_drop = set(atribs.keys()) - set(self.relevant_keys) if self.relevant_keys else self.drop_keys
        for key in keys_to_drop:
            atribs.pop(key, None)
        return atribs

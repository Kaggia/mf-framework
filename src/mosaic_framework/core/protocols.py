from typing import Protocol, List, Dict
import pandas as pd

class ProtocolAgroRule(Protocol):
    """Protocol defining interface for agronomic rules"""
    def prepare(self) -> pd.DataFrame:
        """Prepares data for rule evaluation"""
        ...

    def evaluate(self) -> Dict[str, List[float]]:
        """Evaluates the agronomic rule on prepared data"""
        ...

class ProtocolReflectiveAgroRule(Protocol):
    """Protocol defining interface for reflective agronomic rules"""
    def reflective_evaluate(self) -> Dict[str, List[float]]:
        """Evaluates the reflective agronomic rule"""
        ...

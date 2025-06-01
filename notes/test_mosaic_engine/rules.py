class SimpleComparativeRule():
    def __init__(self, **kwargs) -> None:
        self.column      = kwargs['column'] if 'column' in list(kwargs.keys()) else None
        self.is_implicit = kwargs['is_implicit'] if 'is_implicit' in list(kwargs.keys()) else None
        self.condition   = kwargs['condition'] if 'is_implicit' in list(kwargs.keys()) else None

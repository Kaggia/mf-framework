class Model():
    def __init__(self, **kwargs) -> None:
        self.rules = kwargs['rules'] if 'rules' in list(kwargs.keys()) else None
    
class Environment():
    """test environment class"""
    def __init__(self, **kwargs) -> None:
        self.test_var = kwargs['test_var'] if 'test_var' in list(kwargs.keys()) else None
        self.actor    = kwargs['actor']    if 'actor'    in list(kwargs.keys()) else None
    
    def test_method(self):
        return self.test_var
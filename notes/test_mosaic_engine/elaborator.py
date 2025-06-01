class Elaborator():
    def __init__(self, **kwargs) -> None:
        self.test_var = kwargs['test_var'] if 'test_var' in list(kwargs.keys()) else None

    def run(self):
        return self.test_var
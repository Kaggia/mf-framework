from concurrent.futures import ProcessPoolExecutor, as_completed

class ParallelCalculator(ProcessPoolExecutor):
    def __init__(self, max_workers=None):
        super().__init__(max_workers=max_workers)
        self.results = []

    @staticmethod
    def _some_internal_calculation(value):
        return value ** 2  # Esempio di calcolo

    # @staticmethod
    # def _another_internal_calculation(value):
    #     return value + 10  # Altro esempio di calcolo

    def entry_method(self, values):
        futures = []
        
        # Parallel calcs
        for value in values:
            future = self.submit(self._some_internal_calculation, value)
            futures.append(future)
                    
        # Raccoglie i risultati man mano che i task finiscono
        for future in as_completed(futures):
            result = future.result()
            self.results.append(result)
        
        return self.results

# Esempio di utilizzo
if __name__ == "__main__":
    try:
        values = [1, 2, 3, 4, 5]
        calculator = ParallelCalculator(max_workers=3)
        results = calculator.entry_method(values)
        print("Results:", results)
    except Exception as e:
        print(f"An error occurred: {e}")

import multiprocessing

#Extending Process class, in order to allow returning value
#using Queue structure. We use dict to grant parallel computing, 
#while mantaining the order, cause it's important in a pandas column
#IMPORTANT: calculations must be made in batches, cause creating
#processes all at once is too much.
class ValueProcess(multiprocessing.Process):
    def __init__(self, data, fnc, result_queue):
        super().__init__()
        self.data   = data
        self.fnc    = fnc
        self.result = None
        self.result_queue = result_queue

    def run(self):
        # Simuliamo un'elaborazione costosa
        result = {'id': self.data, 'result': self.fnc(self.data)}
        self.result_queue.put(result)


# from functions.models.processing import ValueProcess
# import multiprocessing
        
# def custom_function(data):
#     # Simuliamo un elaborazione costosa
#     result = data * 2
#     return result



# # Dati da elaborare
# data_list = [i for i in range(10)]
# # Creiamo una coda per i risultati
# result_queue = multiprocessing.Queue()


# # Creiamo e avviamo un processo per ogni dato nella lista
# processes = []
# for data in data_list:
#     process = ValueProcess(data, custom_function, result_queue)
#     process.start()
#     processes.append(process)

# results = list()
# # Attendiamo che tutti i processi terminino
# for process in processes:
#     process.join()
#     results.append(process.result)

# # Estraiamo i risultati dalla coda
# results = []
# while not result_queue.empty():
#     results.append(result_queue.get())

# print(results)


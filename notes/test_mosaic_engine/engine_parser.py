import ast
import importlib

def parse_model_file(file_path):
    with open(file_path, 'r') as file:
        model_content = file.read()
    
    class_calls = []

    tree = ast.parse(model_content)
    
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                class_name = node.func.id
                args = [ast.Expression(arg) if isinstance(arg, ast.Call) else arg for arg in node.args]
                kwargs = {kw.arg: (ast.Expression(kw.value) if isinstance(kw.value, ast.Call) else kw.value) for kw in node.keywords}
                class_calls.append((class_name, args, kwargs))
    
    return class_calls

def import_and_create_objects(modules_path, class_calls):
    modules = [importlib.import_module(m) for m in modules_path]
    objects = []

    for class_name, args, kwargs in class_calls:
        for m in modules:
            if hasattr(m, class_name):
                cls = getattr(m, class_name)
                # Passa gli argomenti come espressioni AST
                obj = cls(*args, **kwargs)
                objects.append(obj)
                print(f'Oggetto creato della classe {class_name} con args: {args} e kwargs: {kwargs}')

    return objects


if __name__ == "__main__":
    #File we are inspecting - usually a Model - 'File to run'
    model_file_path = 'agronomical_model.py'
    #Modules we are inspecting, in order to get all the classes
    env_module_path = ['environment', 'elaborator', 'model']

    # Parsing file model.py
    class_calls = parse_model_file(model_file_path)

    # Import modules e instanciate objects
    objects     = import_and_create_objects(env_module_path, class_calls)

    print("\nclass_calls:")
    for cc in class_calls:
        print(cc)

    print("\nobjects:")

    for o in objects:
        print(o)


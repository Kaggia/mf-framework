import json
import deque

def get_parsed_rules(self, infection_rules:dict, core_rules_modules:list):
    #(Breadth-First Search, BFS)
    def get_object(node, parent, key):
        """Placeholder for the get_object method."""
        # Implement your logic here
        # For now, we just print the node
        obj = None
        for m in core_rules_modules:
            if hasattr(m, node['func']):
                cls = getattr(m, node['func'])
                # Instanciate new object as AST expression
                # Need to cast each <_ast.Constant> to the value in it.
                obj = cls(*node['args'], **node['kwargs']) 

                print("\n----------------------")
                print(node)
                print(obj)
                print("\n----------------------")
    
                #print(f'Oggetto creato della classe {class_name} con args: {args} e kwargs: {kwargs}')                    

        # Replace the node with 1 in the parent
        if parent is not None and key is not None:
            parent[key] = obj
        return node  # Replace with actual object creation logic
                
    def collect_all_nodes_by_levels(nodes):
        """Collect all nodes level by level."""
        levels = []
        queue = deque([(node, 0, None, None) for node in nodes])
        max_level = 0
        
        while queue:
            current, level, parent, key = queue.popleft()
            if level > max_level:
                max_level = level
                levels.append([])
            
            if len(levels) <= level:
                levels.append([])
            levels[level].append((current, parent, key))
            
            for k, value in current.get("kwargs", {}).items():
                if isinstance(value, dict):
                    queue.append((value, level + 1, current["kwargs"], k))
                elif isinstance(value, list):
                    for i, item in enumerate(value):
                        if isinstance(item, dict):
                            queue.append((item, level + 1, value, i))
        
        return levels
    
    def visit(nodes):
        """Visit nodes starting from the lowest level."""
        levels = collect_all_nodes_by_levels(nodes)
        visited = set()

        for level_nodes in reversed(levels):
            for node, parent, key in level_nodes:
                get_object(node, parent, key)
                visited.add(id(node))
        
        # Replace the root nodes
        for i, node in enumerate(nodes):
            nodes[i] = get_object(node, None, None)

        return visited        
    
    def visit_root(unparsed_root:dict)->list:
        for m in core_rules_modules:
            if hasattr(m, unparsed_root['func']):
                cls = getattr(m, unparsed_root['func'])
                # Instanciate new object as AST expression
                # Need to cast each <_ast.Constant> to the value in it.
                parsed_root = cls(*unparsed_root['args'], **unparsed_root['kwargs']) 
                print(parsed_root, " has been set.")
                break
        return parsed_root

    visit(infection_rules)

    for i, n in enumerate(infection_rules):
        infection_rules[i] = visit_root(infection_rules[i])
    
    print(json.dumps(infection_rules, indent=4, default=str))


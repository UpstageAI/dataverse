
import ast

def python_is_script_executable(file_path, verbose=False):
    """
    check if a python script is executable
    in other words, check if the python script does not contains any declaration nodes
    (imports, functions, classes, etc.)

    declaration nodes:
    - imports
    - functions
    - classes
    - variables

    Args:
        file_path (str): path to the python script to check
    Returns:
        bool: True if the python script is executable, False otherwise
    """
    with open(file_path, 'r') as file:
        source_code = file.read()

    # Parse source code into an AST
    module = ast.parse(source_code)
    for node in module.body:
        if not isinstance(node, (
            ast.Import,
            ast.ImportFrom,
            ast.FunctionDef,
            ast.ClassDef,
            ast.Assign
        )):
            if verbose:
                print("found executable code: {}".format(node))
            return True

    if verbose:
        print("found no executable code")
    return False

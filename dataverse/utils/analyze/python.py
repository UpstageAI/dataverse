
import ast

def is_python_declaration_only(file_path, verbose=False):
    """
    check if a python script only contains declarations (imports, functions, classes, etc.)

    currently only should include the following:
    - imports
    - functions
    - classes
    - variables

    Args:
        file_path (str): path to the python script to check
    Returns:
        bool: True if the python script only contains declarations, False otherwise
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
                print("found non-declaration(execution) code: {}".format(node))
            return False

    if verbose:
        print("found only declaration nodes")
    return True

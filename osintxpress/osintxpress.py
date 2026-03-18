import os, importlib.util

def _load_module(module_name: str, path: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec and spec.loader:
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    return None

def _load_rust_pip_or_dev(_rust_lib_name: str = '_osintxpress', module_dev_path: str = None):
    # Determine the directory of this file
    py_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(py_dir)
    
    # Path for development builds (relative to project root)
    if module_dev_path is None:
        module_dev_path = os.path.join(project_root, 'target', 'release', f'lib{_rust_lib_name}.so')

    rust_lib = None
    
    # 1. Try to find it in the same directory as this file (e.g., installed via pip)
    for file in os.listdir(py_dir):
        if (file.startswith(f'lib{_rust_lib_name}') or file.startswith(_rust_lib_name)) and \
           file.endswith(('.so', '.pyd', '.dylib', '.dll')):
            rust_lib = _load_module(_rust_lib_name, os.path.join(py_dir, file))
            if rust_lib:
                break
    
    # 2. Try development path if not found
    if rust_lib is None and os.path.exists(module_dev_path):
        rust_lib = _load_module(_rust_lib_name, module_dev_path)

    if rust_lib is None:
        raise ImportError(f"Could not find Rust binary '{_rust_lib_name}' in '{py_dir}' or at '{module_dev_path}'")

    return rust_lib

## -- Rust module loading
_rust_lib = _load_rust_pip_or_dev()
globals().update({k: v for k, v in vars(_rust_lib).items() if not k.startswith('__')})

import os, sys, importlib.util


def _load_module(module_name: str, path: str):

    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec and spec.loader:
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    return None


def _load_rust_pip_or_dev(_rust_lib_name: str = '_osintxpress', module_dev_path: str = None):
    
    # Wheel load (pip install)
    try:
        from . import _osintxpress
        return _osintxpress
    except ImportError:
        pass
    
    if os.name == 'nt':
        expected_file = f'{_rust_lib_name}.dll' 
    elif sys.platform == 'darwin':
        expected_file = f'lib{_rust_lib_name}.dylib'
    else:
        expected_file = f'lib{_rust_lib_name}.so'

    py_dir = __file__.replace('\\', '/').rsplit('/', 1)[0]
    root_dir = py_dir.rsplit('/', 1)[0]
    dev_path = module_dev_path or f"{root_dir}/target/release/{expected_file}"
    
    # Search local directory (for maturin develop)
    for file in os.listdir(py_dir):
        if (file.startswith(f'lib{_rust_lib_name}') or file.startswith(_rust_lib_name)) and \
           file.endswith(('.so', '.pyd', '.dylib', '.dll')):
            return _load_module(_rust_lib_name, f'{py_dir}/{file}')
    
    # Maturin build in CI
    if os.path.exists(dev_path):
        return _load_module(_rust_lib_name, dev_path) 

    raise ImportError(f'Could not find Rust binary at {dev_path}')


_rust_lib = _load_rust_pip_or_dev()
globals().update({k: v for k, v in vars(_rust_lib).items() if not k.startswith('__')})
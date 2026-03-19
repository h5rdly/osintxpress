import os, importlib.util

def _load_module(module_name: str, path: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec and spec.loader:
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    return None


def _load_rust_pip_or_dev(_rust_lib_name: str = '_osintxpress', module_dev_path: str = None):

    _dev_path_linux = f'target/release/lib{_rust_lib_name}.so'
    _dev_path_windows = f'target/release/lib{_rust_lib_name}.dll'
    _dev_path_mac = f'target/release/lib{_rust_lib_name}.dylib'
    dev_path = _dev_path_windows if os.name == 'nt' else _dev_path_linux if os.name == 'posix' else _dev_path_mac

    module_dev_path = module_dev_path or dev_path

    rust_lib = None
    # Wheel load
    try:
        from . import _osintxpress
        rust_lib = _osintxpress
    except ImportError:
        pass
    else:
        return rust_lib
    
    for file in os.listdir(py_dir := __file__.replace('\\', '/').rsplit('/', 1)[0]):
        if (file.startswith(f'lib{_rust_lib_name}') or file.startswith(_rust_lib_name)) and \
           file.endswith(('.so', '.pyd', '.dylib', '.dll')):
            rust_lib = _load_module(_rust_lib_name, f'{py_dir}/{file}')
            break
    else:
        if os.path.exists(module_dev_path):
            rust_lib = _load_module(_rust_lib_name, _dev_path_linux)

    if rust_lib is None:
        raise ImportError('Could not find Rust binary')

    return rust_lib

## -- Rust module loading
_rust_lib = _load_rust_pip_or_dev()
globals().update({k: v for k, v in vars(_rust_lib).items() if not k.startswith('__')})

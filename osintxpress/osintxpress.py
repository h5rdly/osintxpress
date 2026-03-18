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
    module_dev_path = module_dev_path or _dev_path_linux

    rust_lib = None
    try:
        from . import _webtoken
        rust_lib = _webtoken
    except ImportError:
        pass

    for file in os.listdir(__file__.replace('\\', '/').rsplit('/', 1)[0]):
        if file.startswith(f'lib{_rust_lib_name}') and file.endswith(('.so', '.pyd', '.dylib', 'dll')):
            rust_lib = _load_module(_rust_lib_name, f'{py_dir}/{file}')
            break
    else:
        if os.path.exists(module_dev_path):
            rust_lib = _load_module(_rust_lib_name, _dev_path_linux)

    if rust_lib is None:
        raise ImportError('Could not find Rust binary')

    return rust_lib


## -- Rust module loading

rust_lib = _load_rust_pip_or_dev()
globals().update({k: v for k, v in vars(rust_lib).items() if not k.startswith('__')})
import os, sys, importlib.util, atexit, weakref


## -- Load Rust module

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
            mod = _load_module(_rust_lib_name, f'{py_dir}/{file}')
            if mod: return mod
    
    # Maturin build in CI
    if os.path.exists(dev_path):
        mod = _load_module(_rust_lib_name, dev_path)
        if mod: return mod

    raise ImportError(f'Could not find Rust binary. Tried local dir and {dev_path}')

_rust_lib = _load_rust_pip_or_dev()
globals().update({k: v for k, v in vars(_rust_lib).items() if not k.startswith('__')})

try:
    from .sources import SourceAdapter, SourceConfig
except:
    from sources import SourceAdapter, SourceConfig

## -- General helper functions

def _to_upper_snake_case(name: str) -> str:

    result = []
    for idx, char in enumerate(name):
        print(name)
        if char.isupper() and idx > 0 and not name[idx - 1].isupper():
            result.append('_')
        result.append(char)
    return "".join(result).upper()


def _is_rtl(text):
    
    for char in text:
        if char.isalpha():
            if 0x0590 <= ord(char) <= 0x06FF:
                return True
            return False 

    return False


## -- Python wrapping logic

def supported_sources() -> list[str]:
    return [k for k in dir(SourceAdapter) if not k.startswith('_')]


def login_telegram(api_id: int, api_hash: str, phone: str, session_path: str = "osint.session", code_callback=None):
    ''' Interactively generate a Telegram session file '''
    
    # If no callback is provided (like in a terminal script), use the standard input() prompt
    if code_callback is None:
        print(f"Connecting to Telegram for {phone}...")
        def default_get_code():
            return input(f"\n[!] Enter the 5-digit code Telegram sent to {phone}: ")
        code_callback = default_get_code
    
    # Call the Rust backend with whichever callback we are using
    _rust_lib.login_telegram(api_id, api_hash, phone, session_path, code_callback)
    
    # Only print this if we are running in the terminal mode
    if code_callback.__name__ == 'default_get_code':
        print(f"Telegram session saved to: {session_path}")
        print("You can now pass this session file path to engine.add_telegram_source()")


# Keep track of active engines so we can auto-cleanup on hot-reloads/exits
_active_engines = weakref.WeakSet()

def cleanup_engines():
    for engine in _active_engines:
        print("\n🛑 osintxpress: Gracefully shutting down Rust Engine...")
        engine.stop_all()

atexit.register(cleanup_engines)


class OsintEngine(_rust_lib.OsintEngine):

    def __init__(self, worker_threads=4):

        super().__init__() # PyO3 consumed 'worker_threads' during __new__ to allocate the Rust memory

        self.source_registry = {}
        _active_engines.add(self)


    def add_rest_source(self, adapter: SourceConfig, name=None, url=None, poll_interval_sec=60, headers=None):
        
        src_name = name or adapter.name.lower()
        final_url = url if url else adapter.default_url

        self.source_registry[src_name] = {
            'adapter': adapter, 'type': 'rest', 'interval': poll_interval_sec, 'headers': headers
        }
        
        super().add_rest_source(adapter.parser_type, src_name, final_url, poll_interval_sec, headers)


    def add_ws_source(self, adapter: SourceConfig, name=None, url=None, init_message=None):

        src_name = name or adapter.name.lower()
        final_url = url if url else adapter.default_url

        self.source_registry[src_name] = {
            'adapter': adapter, 'type': 'ws', 'interval': None, 'init_message': init_message
        }
        
        super().add_ws_source(adapter.parser_type, src_name, final_url, init_message)


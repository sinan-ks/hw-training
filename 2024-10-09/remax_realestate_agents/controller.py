import logging
from multiprocessing import Process

crawler_module = 'remax_crawler'
parser_module = 'remax_parser'
export_module = 'export'

def run_module(module_name):
    """Dynamically import and run the module's main functionality."""
    module = __import__(module_name)
    if hasattr(module, 'main'):
        module.main()  
    else:
        logging.error(f"Module {module_name} does not have a main() function.")

def start_processes():
    modules = [crawler_module, parser_module, export_module]
    
    for module_name in modules:
        process = Process(target=lambda name=module_name: run_module(name))
        process.start()
        process.join() 

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    start_processes()

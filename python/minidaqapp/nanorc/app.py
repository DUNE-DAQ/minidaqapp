from .module import ModuleGraph

class App:
    """A single daq_application in a system, consisting of a modulegraph
       and a hostname on which to run"""
    
    def __init__(self, modulegraph=None, host="localhost"):
        self.modulegraph = modulegraph if modulegraph else ModuleGraph()
        self.host = host

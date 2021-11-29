from .module import ModuleGraph

class App:
    """
    A single daq_application in a system, consisting of a modulegraph
    and a hostname on which to run
    """
    
    def __init__(self, modulegraph=None, host="localhost", name=None):
        self.modulegraph = modulegraph if modulegraph else ModuleGraph()
        self.host = host
        self.name = name

    ## Private, do not redeclare
    def __finalise(self, system):
        for module in self.modulegraph:
            module.finalise(system)
        self.finalise(system)

    ## User code goes here
    def finalise(self, system):
        pass

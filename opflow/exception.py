
class ConstructorError(RuntimeError):
    def __init__(self, *args, **kwargs):
        RuntimeError.__init__(self,*args,**kwargs)

class OperationError(RuntimeError):
    def __init__(self, *args, **kwargs):
        RuntimeError.__init__(self,*args,**kwargs)
class TestRouter:
    def get_echo(self, *args, **kwds):
        return {**kwds, '.args': args}
    def set_echo(self, data, *args, **kwds):
        out = dict(data)
        if args:
            out['.args'] = args
        if kwds:
            out['.kwds'] = kwds
        return out
    def put_echo(self, data, *args, **kwds):
        return {'status': "ok", '.data': data, **kwds, '.args': args}
    def del_echo(self, *args, **kwds):
        return {'status': "ok", **kwds, '.args': args}
    def run_echo0(self, action, data, *args, **kwds):
        return {'action': action, '.data': data, '.kwds': kwds, '.args': args}

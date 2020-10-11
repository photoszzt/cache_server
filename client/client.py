import redis


class CustomCache(redis.Redis):

    def __init__(self, **kargs):
        super().__init__(**kargs)
        self.response_callbacks['MTSADD'] = int
        self.response_callbacks['MTSREM'] = int
        self.response_callbacks['MTSMEMBERS'] = lambda r: r and set(r) or set()

    def mtsadd(self, name, *values):
        return self.execute_command('MTSADD', name, *values)

    def mtsmembers(self, name):
        return self.execute_command('MTSMEMBERS', name)

    def mtsrem(self, name, *values):
        return self.execute_command('MTSREM', name, *values)

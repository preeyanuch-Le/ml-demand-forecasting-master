from datetime import datetime, timedelta

def timeit(f):
    def timed(*args, **kw):

        startTime = datetime.now()
        result = f(*args, **kw)
        running_time = timedelta(seconds=((datetime.now() - startTime).total_seconds()))

        print(f'{f.__name__} took: {running_time}')
        return result

    return timed
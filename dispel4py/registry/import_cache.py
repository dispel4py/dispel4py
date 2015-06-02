from datetime import datetime as d


class ImportCache(object):
    """
    A cache for import code and packages
    """
    EXPIRATION_SECS = 10 * 60  # 10 minutes

    def __init__(self):
        self.cache = {}

    def add(self, name, obj=None):
        """
        Add the given entry to the cache; replace if already present
        """
        self.cache[name] = (obj, d.now())

    def get(self, name):
        """
        Return the object corresponding to name from the cache, if present
        and valid; otherwise throw an ImportCacheException
        """
        if self.contains(name):
            return self.cache[name][0]
        else:
            raise ImportCacheException()

    def is_recent(self, name):
        """
        Return true if the entry `name` is still valid.
        Assumes `name` is in the cache.
        """
        return (d.now() - self.cache[name][1]).seconds <=\
            ImportCache.EXPIRATION_SECS

    def contains(self, name):
        """
        If the cache contains the given name and it is recent, return True;
        otherwise return False (after removing it from the cache if applicable)
        """
        if name in self.cache:
            if self.is_recent(name):
                return True
            else:
                self.cache.pop(name)
        return False

    def __str__(self):
        return str(self.cache)


class ImportCacheException(Exception):
    pass

if __name__ == '__main__':
    from time import sleep
    c = ImportCache()
    ImportCache.EXPIRATION_SECS = 5
    c.add('Hello')
    for i in range(0, 10):
        print '(' + str(i) + ') ' + str(c.contains('Hello'))
        sleep(1)

    print c

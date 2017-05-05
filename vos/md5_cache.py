
import sqlite3, logging
READBUF = 8192

class MD5_Cache:

    def __init__(self, cache_db="/tmp/#vos_cached.db#"):
        """Setup the sqlDB that will contain the cache table"""
        self.cache_db = cache_db

        ## initialize the md5Cache db
        sqlConn = sqlite3.connect(self.cache_db)
        with sqlConn:
            sqlConn.execute("create table if not exists md5_cache (fname text PRIMARY KEY NOT NULL , md5 text, st_size int, st_mtime int)")
        ## build cache lookup if doesn't already exists

    def computeMD5(self, filename, block_size=READBUF):
        import hashlib
        md5 = hashlib.md5()
        r = open(filename, 'r')
        while True:
            buf = r.read(block_size)
            if len(buf) == 0:
                break
            md5.update(buf)
        r.close()
        return md5.hexdigest()


    def get(self, fname):
        """Get the MD5 for this fname from the SQL cache"""
        sqlConn = sqlite3.connect(self.cache_db)
        with sqlConn:
            cursor = sqlConn.execute("SELECT md5, st_size, st_mtime FROM md5_cache WHERE fname = ? ", (fname,))
            md5Row = cursor.fetchone()
        if md5Row is not None:
            return md5Row
        else:
            return None

    def delete(self, fname):
        """Delete a record from the cache MD5 database"""
        sqlConn = sqlite3.connect(self.cache_db)
        with sqlConn:
            sqlConn.execute("DELETE from md5_cache WHERE fname = ?", (fname,))


    def update(self, fname, md5, st_size, st_mtime):
        """Update a record in the cache MD5 database"""
        ## UPDATE the MD5 database
        sqlConn = sqlite3.connect(self.cache_db)
        try:
            with sqlConn:
                sqlConn.execute("DELETE from md5_cache WHERE fname = ?", (fname,))
                sqlConn.execute("INSERT INTO md5_cache (fname, md5, st_size, st_mtime) VALUES ( ?, ?, ?, ?)", (fname, md5, st_size, st_mtime))
        except Exception as e:
            logging.error(e)
        return md5


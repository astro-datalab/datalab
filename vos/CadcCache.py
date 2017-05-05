"""
A sparse file caching library
"""

import os
import sys
import stat
import time
import threading
from Queue import Queue
import traceback
import errno
from contextlib import nested
from errno import EACCES, EIO, ENOENT, EISDIR, ENOTDIR, ENOTEMPTY, EPERM, \
    EEXIST, ENODATA, ECONNREFUSED, EAGAIN, ENOTCONN
import ctypes
import ctypes.util
import logging

from SharedLock import SharedLock as SharedLock
from CacheMetaData import CacheMetaData as CacheMetaData
from logExceptions import logExceptions
import utils

libcPath = ctypes.util.find_library('c')
libc = ctypes.cdll.LoadLibrary(libcPath)

_flush_thread_count = 0

logger = logging.getLogger('cache')
logger.setLevel(logging.ERROR)
if sys.version_info[1] > 6:
    logger.addHandler(logging.NullHandler())

ZERO_LENGTH_MD5 = 'd41d8cd98f00b204e9800998ecf8427e'
CACHE_DATA_SUBDIR = 'data'
CACHE_METADATA_SUBDIR = 'metaData'
CACHE_CHECK_INTERVAL = 10  # Not clear why this is so low.


# TODO optionally disable random reads - always read to the end of the file.

class CacheCondition(object):
    """This extends threading.condtion (or it would if it were a class):
       There is an optional timeout associated with the condition.
       The timout starts runing when the condition lock is acquired.
       The timeout throws the CacheTimeout exception.
    """

    def __init__(self, lock, timeout=None):
        self.timeout = timeout
        self.myCondition = threading.Condition(lock)
        self.threadSpecificData = threading.local()
        self.threadSpecificData.endTime = None

    def __enter__(self):
        """To support the with construct.
        """

        self.acquire()
        return self

    def __exit__(self, a1, a2, a3):
        """To support the with construct.
        """
        #logger.debug("{}".format(kwargs))
        self.release()
        return

    def set_timeout(self):
        self.threadSpecificData.endTime = time.time() + self.timeout

    def clear_timeout(self):
        self.threadSpecificData.endTime = None

    def acquire(self, blocking=True):
        return self.myCondition.acquire(blocking)

    def release(self):
        self.myCondition.release()

    def wait(self):
        """Wait for the condition:"""

        if (not hasattr(self.threadSpecificData, 'endTime') or
                    self.threadSpecificData.endTime is None):
            self.myCondition.wait()
        else:
            time_left = self.threadSpecificData.endTime - time.time()
            if time_left < 0:
                self.threadSpecificData.endTime = None
                raise CacheRetry("Condition variable timeout")
            else:
                self.myCondition.wait(time_left)

    def notify_all(self):
        self.myCondition.notify_all()


class Cache(object):
    """
    Manages the cache for the vofs.

    The cache is the location where vofs will store the reads from VOSpace.  Once a file is written into the
    cache subsequent reads from vofs will deliver the cache content rather than reconnecting to the server.

    The cache maintains status of files stored in the cache area so that vofs can compare that information against
    node info coming from the service.

    The vofs calls will want a response within vofs.cacheTimeOut seconds so cache should raise a CacheTimeout before
    the filesystem times out.

    """
    IO_BLOCK_SIZE = 2 ** 14

    def __init__(self, cacheDir, maxCacheSize, read_only=False, timeout=60, maxFlushThreads=10):
        """Initialize the Cache Object

        Parameters:
        -----------
        @param cacheDir: The directory for the cache.
        @param maxCacheSize: The maximum cache size in megabytes
        @param read_only:  Is the cached data read-only
        @param timeout: number of seconds to wait before timing-out a cache read.
        @param maxFlushThreads: Maximum number of nodes to flush simultaneously
        """

        # Why set a custom checkinterval ?
        sys.setcheckinterval(CACHE_CHECK_INTERVAL)

        self.cacheDir = os.path.abspath(cacheDir)
        self.dataDir = os.path.join(self.cacheDir, CACHE_DATA_SUBDIR)
        self.metaDataDir = os.path.join(self.cacheDir, CACHE_METADATA_SUBDIR)
        self.timeout = timeout
        self.maxCacheSize = maxCacheSize
        self.read_only = read_only
        self.fileHandleDict = {}


        # When cache locks and file locks need to be held at the same time,
        # always acquire the cache lock first.
        self.cacheLock = threading.RLock()

        # A thread queue will be shared by FileHandles to flush nodes.
        # Figure out how many threads will be available now, but defer
        # initializing the queue / worker threads until the filesystem
        # is initialized (see vofs.init)
        self.maxFlushThreads = maxFlushThreads
        self.flushNodeQueue = None

        # ensure that the cache areas exist and have the desired permissions.
        utils.mkdir_p(self.dataDir, stat.S_IRWXU)
        utils.mkdir_p(self.metaDataDir, stat.S_IRWXU)
        # logger.debug("Initialized data and meta data Cache areas: {0} {1}".format(self.dataDir, self.metaDataDir))

    def __enter__(self):
        """
        This method allows Cache object to be used with the "with"
        construct.
        """
        return self

    def __exit__(self, type, value, traceback):
        """
        This method allows Cache object to be used with the "with"
        construct.
        """
        pass

    def __str__(self):
        return "DataCache: {0}, MetaDataCache: {1}, CacheSize: {2}".format(self.dataDir, self.metaDataDir, self.determineCacheSize())

    @logExceptions()
    def open(self, path, isNew, mustExist, ioObject, trustMetaData):
        """Open file with the desired modes

        Here we return a handle to the cached version of the file
        which is updated if older and out of sync with VOSpace.

        path - the path to the file.

        isNew - should be True if this is a completely new file, and False if
            reads should access the bytes from the file in the backing store.
        mustExist - The open mode requires the file to exist.
        trustMetaData - Trust that meta data exists.

        ioObject - the object that provides access to the backing store
        """
        # logger.debug("Getting filehandle for {0} {1} {2} {3} {4}".format(path, isNew, mustExist, ioObject, trustMetaData))
        # logger.debug(str(self))

        fileHandle = self.getFileHandle(path, isNew, ioObject)
        # logger.debug("Got filehandle: {0}".format(fileHandle))
        with fileHandle.fileCondition:
            # logger.debug(
            #     "Opening file {0}: isnew {1}: id {2}: Fully Cached {3}: Must Exist {4}: Trust MetaData {5}:".format(
            #         path, isNew, id(fileHandle), fileHandle.fullyCached, mustExist, trustMetaData))
            # If this is a new file, initialize the cache state, otherwise
            # leave it alone.
            if fileHandle.fullyCached is None:
                if isNew:
                    fileHandle.fileModified = True
                    fileHandle.setHeader(0, ZERO_LENGTH_MD5)
                elif os.path.exists(fileHandle.cacheMetaDataFile):
                    fileHandle.metaData = CacheMetaData(fileHandle.cacheMetaDataFile, None, None, None)
                    if fileHandle.metaData.getNumReadBlocks() == len(fileHandle.metaData.bitmap):
                        fileHandle.fullyCached = True
                        fileHandle.fileSize = os.path.getsize(fileHandle.cacheMetaDataFile)
                    else:
                        fileHandle.fullyCached = False
                        fileHandle.fileSize = fileHandle.metaData.size
                    if trustMetaData:
                        fileHandle.setHeader(fileHandle.metaData.size,
                                             fileHandle.metaData.md5sum)
                    else:
                        fileHandle.gotHeader = False
                else:
                    fileHandle.metaData = None
                    fileHandle.gotHeader = False
                    fileHandle.fullyCached = False

                if (not fileHandle.fullyCached and
                        (fileHandle.metaData is None or fileHandle.metaData.getNumReadBlocks() == 0)):
                    # If the cache file should be empty, empty it.
                    with fileHandle.ioObject.cacheFileDescriptorLock:
                        os.ftruncate(fileHandle.ioObject.cacheFileDescriptor, 0)
                        os.fsync(fileHandle.ioObject.cacheFileDescriptor)
                        fileHandle.fullyCached = True

            # For an existing file, start a data transfer to get the size and
            # md5sum unless the information is available and is trusted.
            # logger.debug("RefCount: {0}, gotHeader: {1}, fileModified: {2}, trustMetaData: {3}".format(
            #     fileHandle.refCount, fileHandle.gotHeader, fileHandle.fileModified, trustMetaData
            # ))
            if ((fileHandle.refCount == 1 or not fileHandle.gotHeader) and not fileHandle.fileModified and
                    (fileHandle.metaData is None or not trustMetaData)):
                # logger.debug("Doing a readData.")
                fileHandle.readData(0, 0, None)
                while (not fileHandle.gotHeader and
                               fileHandle.readException is None):
                    fileHandle.fileCondition.wait()
            if fileHandle.readException is not None:
                # If the file doesn't exist and is not required to exist, then
                # an ENOENT error is ok and not propegated. All other errors
                # are propegated.
                if not (isinstance(fileHandle.readException[1], EnvironmentError) and
                                fileHandle.readException[1].errno == errno.ENOENT and not mustExist):
                    raise fileHandle.readException[0], \
                        fileHandle.readException[1], \
                        fileHandle.readException[2]
                # The file didn't exist on the backing store but its ok
                fileHandle.fullyCached = True
                fileHandle.gotHeader = True
                fileHandle.fileSize = 0

        try:
            self.checkCacheSpace()
        except:
            pass

        return fileHandle

    def getFileHandle(self, path, createFile, ioObject):
        """Find an existing file handle, or create one if necessary.
        @param path: location of the file, relative to cache root.
        @param createFile: should the file be created if it doesn't exist
        @param ioObject: the ioObject is used to read/write this file from the backing (VOSpace)
        @rtype : FileHandle A file_like object that enables reading and writing to the cache object.
        """

        # logger.debug("Getting fileHandle for path:{0} createFile:{1} ioObject:{2}".format(path,
        #                                                                                   createFile,
        #                                                                                   ioObject))

        if createFile and self.read_only:
            raise OSError(EPERM, 'Create denied, cache marked readonly.')

        # Lock so only one file referenced at a time, avoids race issues.
        with self.cacheLock:
            try:
                newFileHandle = self.fileHandleDict[path]
                isNewFileHandle = False
            except KeyError:
                isNewFileHandle = True
                newFileHandle = FileHandle(path, self, ioObject)
                self.fileHandleDict[path] = newFileHandle

            if not isNewFileHandle and createFile:
                # We got an old file handle, but are creating a new file.
                # Mark the old file handle as obsolete and create a new
                # file handle.
                newFileHandle.obsolete = True
                if newFileHandle.metaData is not None:
                    try:
                        os.remove(newFileHandle.metaData.metaDataFile)
                    except OSError as e:
                        if e.errno != ENOENT:
                            raise
                newFileHandle = FileHandle(path, self, ioObject)
                del self.fileHandleDict[path]
                # Lock the newly acquired file handle to avoid any race
                # conditions after it is added to the dictionary and before
                # it is incremented.
                with newFileHandle.fileLock:
                    self.fileHandleDict[path] = newFileHandle
                    newFileHandle.refCount += 1
            else:
                newFileHandle.refCount += 1
            # logger.debug("RefCount: {0}".format(newFileHandle.refCount))
        return newFileHandle

    @logExceptions()
    def checkCacheSpace(self):
        """Clear the oldest files until cache_size < cache_limit"""

        # TODO - this really needs to be moved into a background thread which
        # wakes up when other methods think it needs to be done. Having
        # multiple threads do this is bad. It should also be done on a
        # schedule to allow for files which grow.
        (oldest_file, cacheSize) = self.determineCacheSize()
        while (cacheSize / 1024 / 1024 > self.maxCacheSize and oldest_file is not None):
            with self.cacheLock:
                if oldest_file[len(self.dataDir):] not in self.fileHandleDict:
                    # logger.debug("Removing file %s from the local cache" % oldest_file)
                    try:
                        os.unlink(oldest_file)
                        os.unlink(self.metaDataDir + oldest_file[len(self.dataDir):])
                    except OSError:
                        pass
                    self.removeEmptyDirs(os.path.dirname(oldest_file))
                    self.removeEmptyDirs(os.path.dirname(self.metaDataDir + oldest_file[len(self.dataDir):]))
            # TODO - Tricky - have to get a path to the meta data given the path to the data.
            (oldest_file, cacheSize) = self.determineCacheSize()

    def removeEmptyDirs(self, dirName):
        if os.path.commonprefix((dirName, self.cacheDir)) != self.cacheDir:
            raise ValueError("Path '%s' is not in the cache." % dirName)

        thisDir = dirName
        while thisDir != self.cacheDir:
            try:
                os.rmdir(thisDir)
            except OSError as e:
                if e.errno == ENOTEMPTY:
                    return
                elif e.errno == ENOENT:
                    pass
                else:
                    raise e

            thisDir = os.path.dirname(thisDir)

    def determineCacheSize(self):
        """Determine how much disk space is being used by the local cache"""
        # TODO This needs to be cleaned up. There has to be a more efficient way to clean up the cache.

        start_path = self.dataDir
        total_size = 0

        self.atimes = {}
        oldest_time = time.time()
        oldest_file = None
        for dirpath, dirnames, filenames in os.walk(start_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                with self.cacheLock:
                    inFileHandleDict = fp[len(self.dataDir):] not in self.fileHandleDict
                try:
                    osStat = os.stat(fp)
                except:
                    continue
                if inFileHandleDict and oldest_time > osStat.st_atime:
                    oldest_time = osStat.st_atime
                    oldest_file = fp
                total_size += osStat.st_size
        return oldest_file, total_size

    def unlinkFile(self, path):
        """Remove a file from the cache."""

        # logger.debug("unlink %s:" % path)

        if not os.path.isabs(path):
            raise ValueError("Path '%s' is not an absolute path." % path)

        with self.cacheLock:
            try:
                existingFileHandle = self.fileHandleDict[path]
            except KeyError:
                existingFileHandle = None

            if existingFileHandle is not None:
                with existingFileHandle.fileLock:
                    existingFileHandle.obsolete = True
                    del self.fileHandleDict[path]

            # Ignore errors that the file does not exist
            try:
                os.remove(self.metaDataDir + path)
            except OSError:
                pass
            try:
                os.remove(self.dataDir + path)
            except OSError:
                pass

            self.removeEmptyDirs(os.path.dirname(self.metaDataDir + path))
            self.removeEmptyDirs(os.path.dirname(self.dataDir + path))

    def renameFile(self, oldPath, newPath):
        """Rename a file in the cache."""

        if not os.path.isabs(oldPath):
            raise ValueError("Path '%s' is not an absolute path." % oldPath)
        if not os.path.isabs(newPath):
            raise ValueError("Path '%s' is not an absolute path." % newPath)
        newDataPath = self.dataDir + newPath
        newMetaDataPath = self.metaDataDir + newPath
        oldDataPath = self.dataDir + oldPath
        oldMetaDataPath = self.metaDataDir + oldPath
        if os.path.isdir(oldDataPath):
            raise ValueError("Path '%s' is a directory." % oldDataPath)
        if os.path.isdir(oldMetaDataPath):
            raise ValueError("Path '%s' is a directory." % oldMetaDataPath)

        with self.cacheLock:
            # Make sure the new directory exists.
            try:
                os.makedirs(os.path.dirname(newDataPath), stat.S_IRWXU)
            except OSError:
                pass
            try:
                os.makedirs(os.path.dirname(newMetaDataPath), stat.S_IRWXU)
            except OSError:
                pass

            try:
                existingFileHandle = self.fileHandleDict[oldPath]
                # If the file is active, rename its files with the lock held.
                with existingFileHandle.fileLock:
                    Cache.atomicRename((oldDataPath, newDataPath),
                                       (oldMetaDataPath, newMetaDataPath))
                    existingFileHandle.cacheDataFile = \
                        os.path.abspath(newDataPath)
                    existingFileHandle.cacheMetaDataFile = \
                        os.path.abspath(newMetaDataPath)
            except KeyError:
                # The file is not active, rename the files but there is no
                # data structure to lock or fix.
                Cache.atomicRename((oldDataPath, newDataPath),
                                   (oldMetaDataPath, newMetaDataPath))

    @staticmethod
    def atomicRename(*renames):
        """Atomically rename multiple paths. It isn't an error if one of the
           paths doesn't exist.
        """
        renamedList = []
        try:
            for pair in renames:
                if Cache.pathExists(pair[0]):
                    os.rename(pair[0], pair[1])
                    renamedList.append(pair)
        except:
            for pair in renamedList:
                os.rename(pair[1], pair[0])
            raise

    def renameDir(self, oldPath, newPath):
        """Rename a directory in the cache."""

        if not os.path.isabs(oldPath):
            raise ValueError("Path '%s' is not an absolute path." % oldPath)
        if not os.path.isabs(newPath):
            raise ValueError("Path '%s' is not an absolute path." % newPath)
        newDataPath = os.path.abspath(self.dataDir + newPath)
        newMetaDataPath = os.path.abspath(self.metaDataDir + newPath)
        oldDataPath = os.path.abspath(self.dataDir + oldPath)
        oldMetaDataPath = os.path.abspath(self.metaDataDir + oldPath)
        if os.path.isfile(oldDataPath):
            raise ValueError("Path '%s' is not a directory." % oldDataPath)
        if os.path.isfile(oldMetaDataPath):
            raise ValueError("Path '%s' is not a directory." % oldMetaDataPath)

        with self.cacheLock:
            # Make sure the new directory exists.
            try:
                os.makedirs(os.path.dirname(newDataPath), stat.S_IRWXU)
            except OSError:
                pass
            try:
                os.makedirs(os.path.dirname(newMetaDataPath), stat.S_IRWXU)
            except OSError:
                pass

            # Lock any active file in the cache. Lock them all so nothing tries
            # to open a file, do then rename, and then unlock them all. A happy
            # hunging ground for deadlocks.
            try:
                renamed = False
                lockedList = []
                for path in self.fileHandleDict:
                    if path.startswith(oldPath):
                        fh = self.fileHandleDict[path]
                        fh.fileLock.acquire()
                        lockedList.append(fh)
                Cache.atomicRename((oldDataPath, newDataPath),
                                   (oldMetaDataPath, newMetaDataPath))
                renamed = True
            finally:
                for fh in lockedList:
                    if renamed:
                        # Change the data file name and meta data file name in
                        # the file handle.
                        start = len(oldDataPath)
                        fh.cacheDataFile = os.path.abspath(self.dataDir +
                                                           newPath + fh.cacheDataFile[start:])
                        start = len(oldMetaDataPath)
                        fh.cacheMetaDataFile = os.path.abspath(
                            self.metaDataDir + newPath +
                            fh.cacheMetaDataFile[start:])
                    fh.fileLock.release()

    def getAttr(self, path):
        """Get the attributes of a cached file.

        This method will only return attributes if the cached file's attributes
        are better than the backing store's attributes. I.e. if the file is
        open and has been modified.
        """
        # logger.debug("gettattr %s:" % path)

        with self.cacheLock:
            # Make sure the file state doesn't change in the middle.
            try:
                fileHandle = self.fileHandleDict[path]
            except KeyError:
                return None
            with fileHandle.fileLock:
                if fileHandle.fileModified:
                    # logger.debug("file modified: %s" %
                    #              fileHandle.fileModified)
                    f = os.stat(fileHandle.cacheDataFile)
                    # logger.debug("size = %d:" % f.st_size)
                    return dict((name, getattr(f, name))
                                for name in dir(f)
                                if not name.startswith('__'))
                else:
                    return None

    @staticmethod
    def pathExists(path):
        """Return true if the file exists"""

        try:
            os.stat(path)
        except Exception as e:
            if isinstance(e, OSError) and (e.errno == errno.EEXIST or
                                                   e.errno == errno.ENOENT):
                return False
            else:
                raise
        return True


class CacheError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class CacheRetry(OSError):
    def __init__(self, value, path=None):
        self.value = value
        self.filename = path
        self.errno = EAGAIN

    def __str__(self):
        return repr(self.value)


class CacheAborted(Exception):
    # Thrown when a cache operation should be aborted.

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class IOProxy(object):
    """
    This is an abstract class used to provide functionality to do IO for the
    cache. The methods which raise the NotImplementedError exception must be
    implemented by the end user.
    """

    def __init__(self):
        """
        The initializer indicates if the IO object supports random read or
        random write.
        """

        self.lock = threading.RLock()
        self.cacheFile = None
        self.cache = None
        self.cacheFileDescriptor = None
        self.currentWriteOffset = None
        self.cacheFileDescriptorLock = threading.Lock()
        self.exception = None

    def get_md5(self):
        """
        Return the MD5sum of the remote file.
        """
        raise NotImplementedError("IOProxy.getMD5")

    def getSize(self):
        """
        Return the size of the remote file.
        """
        raise NotImplementedError("IOProxy.getMD5")

    def delNode(self, force=False):
        """
        Delete the remote file.
        """
        raise NotImplementedError("IOProxy.delNode")

    def writeToBacking(self):
        """
        Write a file in the cache to the remote file.

        Return the md5sum of the file written.
        """
        raise NotImplementedError("IOProxy.writeToBacking")

    def readFromBacking(self, size=None, offset=0,
                        blockSize=Cache.IO_BLOCK_SIZE):
        """
        Read a file from the remote system into cache.
        If size is None, write to the end of the file.
        offset is the offset into the file for the start of the read.
        blocksize is the recommended blocksize.

        The implementation must use the writeToCache method to write the data
        to the cache.
        """
        raise NotImplementedError("IOProxy.readFromBacking")

    def writeToCache(self, buffer, offset):
        """
        Function to write data to a the cache. This method should only be used
        by the implementation of the readFromBacking method.

        The method raises a CacheAborted exception if the preload should be
        aborted.
        """

        if (self.currentWriteOffset is not None and
                    self.currentWriteOffset != offset):
            # Only allow seeks to block boundaries
            if (offset % self.cache.IO_BLOCK_SIZE != 0 or
                    (self.currentWriteOffset % self.cache.IO_BLOCK_SIZE != 0
                     and self.currentWriteOffset != self.cacheFile.fileSize)):
                raise CacheError("Only seeks to block boundaries are "
                                 "permitted when writing to cache: %d %d %d %d" % (offset,
                                                                                   self.currentWriteOffset,
                                                                                   self.cache.IO_BLOCK_SIZE,
                                                                                   self.cacheFile.fileSize))
            self.currentWriteOffset = offset

        if offset + len(buffer) > self.cacheFile.fileSize:
            raise CacheError("Attempt to populate cache past the end " +
                             "of the known file size: %d > %d." %
                             (offset + len(buffer), self.cacheFile.fileSize))

        with self.cacheFileDescriptorLock:
            os.lseek(self.cacheFileDescriptor, offset, os.SEEK_SET)
            # Write the data to the data file.
            nextByte = 0
            # byteBuffer = bytes(buffer)
            while nextByte < len(buffer):
                nextByte += os.write(self.cacheFileDescriptor,
                                     buffer[nextByte:])
            os.fsync(self.cacheFileDescriptor)

        with self.cacheFile.fileCondition:
            # Set the mask bits corresponding to any completely read blocks.
            lastCompleteByte = offset + len(buffer)
            if lastCompleteByte != self.cacheFile.fileSize:
                lastCompleteByte = lastCompleteByte - (lastCompleteByte %
                                                       self.cache.IO_BLOCK_SIZE)
            firstBlock, numBlocks = self.blockInfo(offset, lastCompleteByte -
                                                   offset)
            if numBlocks > 0:
                self.cacheFile.metaData.setReadBlocks(firstBlock,
                                                      firstBlock + numBlocks - 1)
                self.cacheFile.fileCondition.notify_all()

            self.currentWriteOffset = offset + len(buffer)

            # Check to see if the current read has been aborted or the cache file removed while we weren't looking
            # and if so, throw an exception
            if self.cacheFile is None or (self.cacheFile.readThread.aborted and self.cacheFile.readThread.mandatoryEnd <= lastCompleteByte <= self.cacheFile.fileSize):
                # logger.debug("reading to cache aborted for %s" %
                #              self.cacheFile.path)
                raise CacheAborted("Read to cache aborted.")

        return nextByte

    def blockInfo(self, offset, size):
        """ Determine the blocks completed when "size" bytes starting at
        "offset" are written.
        """

        if size is None:
            return None, None

        firstBlock = offset / self.cache.IO_BLOCK_SIZE
        if size == 0:
            numBlocks = 0
        else:
            numBlocks = (((offset + size - 1) / self.cache.IO_BLOCK_SIZE) -
                         firstBlock + 1)
        return firstBlock, numBlocks

    def setCacheFile(self, cacheFile):
        self.cacheFile = cacheFile
        self.cache = cacheFile.cache


class FileHandle(object):
    def __init__(self, path, cache, ioObject):
        self.path = path
        self.cache = cache
        # logger.debug("creating a new File Handle for {0} using cache: {1}".format(path, cache))
        if not os.path.isabs(path):
            raise ValueError("Path '%s' is not an absolute path." % path)
        # TODO this part of the code assumed the VOSpace path serpartor and the local FS are the same. FIXME
        self.cacheDataFile = os.path.abspath(self.cache.dataDir + path)
        self.cacheMetaDataFile = os.path.abspath(self.cache.metaDataDir + path)
        self.metaData = None
        self.ioObject = ioObject
        ioObject.setCacheFile(self)

        try:
            os.makedirs(os.path.dirname(self.cacheDataFile), stat.S_IRWXU)
        except OSError as oer:
            if oer.errno == EEXIST:
                pass
            else:
                raise

        with self.ioObject.cacheFileDescriptorLock:
            self.ioObject.cacheFileDescriptor = os.open(self.cacheDataFile,
                                                        os.O_RDWR | os.O_CREAT)
            # Why is there an fstat here?
            info = os.fstat(self.ioObject.cacheFileDescriptor)

        # logger.debug("Created a cache file descriptor.")
        # When cache locks and file locks need to be held at the same time,
        # always acquire the cache lock first.
        # Lock for modifying the FileHandle object.
        self.fileLock = threading.RLock()
        self.fileCondition = CacheCondition(self.fileLock,
                                            timeout=cache.timeout)
        # Lock for modifying content of a file. A shared lock should be
        # acquired whenever the data is modified. An exclusive lock should be
        # acquired when data is flushed to the backing store.
        self.writerLock = SharedLock()
        self.refCount = 0
        self.fileModified = False
        self._fullyCached = False
        # Is this file now obsoleted by a new file.
        self.obsolete = False
        # Is the file flush out to vospace queued right now?
        self.flushQueued = None
        self.flushException = None
        self.readThread = None
        self.writeAborted = None
        self.gotHeader = False
        self.fileSize = None
        self.md5sum = None
        self.readException = None

    @property
    def fullyCached(self):
        return self._fullyCached

    @fullyCached.setter
    def fullyCached(self, fullyCached):
        import inspect
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        logger.debug("{} set to fullyCached to: {}".format(calframe[1][3], fullyCached))
        self._fullyCached = fullyCached

    def __enter__(self):
        return self

    def __exit__(self, a1, a2, a3):
        self.release()

    def setHeader(self, size, md5):
        """ Attempt to set the file size and md5sum."""
        # logger.debug("size: %s md5: %s" % (size, md5))
        import inspect
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        logger.debug("set header called with size: {} and md5: {}".format(size, md5))

        if self.gotHeader:
            return

        self.fileSize = size
        blocks, numBlock = self.ioObject.blockInfo(0, self.fileSize)

        # If the file had meta data and it hasn't change, abort the read
        # thread.
        if self.metaData is not None and self.metaData.md5sum == md5:
            with self.fileCondition:
                if self.readThread is not None:
                    self.readThread.aborted = True

        # If the md5sum isn't the same, the cache data is no good.
        if self.metaData is None or self.metaData.md5sum != md5:
            self.metaData = CacheMetaData(self.cacheMetaDataFile, numBlock,
                                          md5, size)
            self.fullyCached = False

        # logger.debug("metaData: {0} fullCached: {1}".format(self.metaData, self.fullyCached))
        # mark the object as fully cached if there are 0 blocks to read and a read thread hasn't started.
        if not self.fullyCached and self.metaData.getNumReadBlocks() == 0 and self.readThread is None:
            # If the cache file should be empty, empty it.
            with self.ioObject.cacheFileDescriptorLock:
                os.ftruncate(self.ioObject.cacheFileDescriptor, 0)
                os.fsync(self.ioObject.cacheFileDescriptor)
                self.fullyCached = True
        self.gotHeader = True

    def setReadException(self):
        self.readException = sys.exc_info()

    def flush(self):
        """Flush contents of file to backing.

        The flush of a modified file may respond by raising a CacheRetry
        exception if the flush to the backing store takes longer than the
        timeout specified for the cache. The caller should respond to this by
        repeating the call to release after satisfying the FUSE timeout
        requirement.
        """

        if self.ioObject.exception is not None:
            raise self.ioObject.exception

        # logger.debug("Flushing node %s: id %d: refCount %d: modified %s: obsolete: %s" %
        #              (self.path, id(self),
        #               self.refCount,
        #               self.fileModified,
        #               self.obsolete))
        self.fileCondition.set_timeout()
        # logger.debug("using the condition lock acquires the fileLock")
        with self.fileCondition:
            if self.refCount > 1:
                raise OSError(errno.EBUSY, "File refcount indicates files handle busy.")
            # logger.debug("Got the lock. Flushing: {0}".format(self.flushQueued))

            try:
                # Tell any running write thread to abort.
                while self.flushQueued is not None:
                    self.writeAborted = True
                    # logger.debug("Waiting for queued flush to complete.")
                    self.fileCondition.wait()
            except Exception as e:
                pass
                # logger.debug("{0}".format(e))

            try:
                self.writeAborted = False
            except Exception as e:
                pass
                # logger.debug("{0}".format(e))

            # Tell any running read thread to exit
            if self.readThread is not None:
                self.readThread.aborted = True

            # If flushing is not already in progress, submit to the thread
            # queue.
            if self.flushQueued is None and self.fileModified and not self.obsolete:
                self.refCount += 1

                # Acquire the writer lock exclusively. This will prevent
                # the file from being modified while it is being
                # flushed. This used to be inside flushNode when it was
                # executed immediately in a new thread. However, now that
                # this is done by a thread queue there could be a large
                # delay, so the lock is acquired by the thread that puts
                # it in the queue and waits for it to finish. The lock
                # is released by the worker thread doing the flush (and
                # it needs to steal the lock in order to do so...)
                # logger.debug("acquiring an exclusive write lock.")
                self.writerLock.acquire(shared=False)
                # logger.debug("Lock acquired.")

                if self.cache.flushNodeQueue is None:
                    raise CacheError("flushNodeQueue has not been initialized")

                self.cache.flushNodeQueue.put(self)
                self.flushQueued = True
                # logger.debug("queue size now %i" \
                #              % self.cache.flushNodeQueue.qsize())

            while (self.flushQueued is not None or
                           self.readThread is not None):
                # Wait for the flush to complete.
                # logger.debug("flushQueued: %s, readThread: %s" %
                #              (self.flushQueued, self.readThread))
                # logger.debug("Waiting for flush to complete.")
                self.fileCondition.wait()

            # Look for write failures.
            if self.flushException is not None:
                raise self.flushException[0], self.flushException[1], \
                    self.flushException[2]

            return 0

    def release(self):
        """Close the file.

        delete our reference to this cache object.
        """

        if self.refCount == 1:
            self.flush()
        self.deref()
        return 0

    def deref(self):
        # logger.debug("Getting locks so we can do a deref and close.")
        with nested(self.cache.cacheLock, self.fileLock,
                    self.ioObject.cacheFileDescriptorLock):
            # logger.debug("Lock acquired now doing the ref-reduction and close.")
            self.refCount -= 1
            if self.refCount == 0:
                # logger.debug("Closing the cache object now.")
                os.close(self.ioObject.cacheFileDescriptor)
                self.ioObject.cacheFileDescriptor = None
                if not self.obsolete:
                    # The entry in fileHandleDict may have been removed by
                    # unlink, so don't panic
                    if self.metaData is not None:
                        self.metaData.persist()
                    try:
                        del self.cache.fileHandleDict[self.path]
                    except KeyError:
                        pass
        return

    def getFileInfo(self):
        """Get the current file information for the file."""

        with self.ioObject.cacheFileDescriptorLock:
            info = os.fstat(self.ioObject.cacheFileDescriptor)
        return info.st_size, info.st_mtime

    @logExceptions()
    def flushNode(self):
        """Flush the file to the backing store.
        """

        global _flush_thread_count

        try:
            if self.ioObject.exception is not None:
                raise self.ioObject.exception

            _flush_thread_count = _flush_thread_count + 1

            # logger.debug("flushing node %s, working thread count is %i " \
            #              % (self.path, _flush_thread_count))
            self.flushException = None

            # Now that the flush has started we want this thread to own
            # the lock
            self.writerLock.steal()

            # Get the md5sum of the cached file
            size, mtime = self.getFileInfo()

            # Write the file to vospace.

            with self.ioObject.cacheFileDescriptorLock:
                os.fsync(self.ioObject.cacheFileDescriptor)
            md5 = self.ioObject.writeToBacking()

            # Update the meta data md5
            blocks, numBlocks = self.ioObject.blockInfo(0, size)
            self.metaData = CacheMetaData(self.cacheMetaDataFile,
                                          numBlocks, md5, size)
            if numBlocks > 0:
                self.metaData.setReadBlocks(0, numBlocks - 1)
            self.metaData.md5sum = md5
            self.metaData.persist()
            self.fileModified = False

        except Exception as e:
            # logger.debug("Flush node failed")
            self.flushException = sys.exc_info()
            # logger.debug(str(self.flushException))
        finally:
            self.flushQueued = None
            try:
                self.writerLock.release()
            except:
                pass
            self.deref()
            _flush_thread_count = _flush_thread_count - 1
            # logger.debug("finished flushing node %s, working thread count is %i " \
            #              % (self.path, _flush_thread_count))
            # Wake up any threads waiting for the flush to finish
            with self.fileCondition:
                self.fileCondition.notify_all()

        try:
            self.cache.checkCacheSpace()
        except:
            pass

        return

    def write(self, data, size, offset):
        """Write data to the file.
        This method will raise a CacheRetry error if the response takes longer
        than the timeout.
        """

        # logger.debug("writting %d bytes at %d to %d " % (size, offset,
        #                                                  self.ioObject.cacheFileDescriptor))

        if self.ioObject.exception is not None:
            raise self.ioObject.exception

        if self.fileSize is None:
            self.fileSize = self.ioObject.getSize()

        # Acquire a shared lock on the file
        with self.writerLock(shared=True):

            # Ensure the entire file is in cache.
            # TODO (optimization) It isn't necessary to always read the file.
            # Only if the write would cause a gap in the written data. If
            # there is never a gap, the file would only need to be read on
            # release in order to fill in the end of the file (only if the
            # last data written is before the end of the old file.
            # However, this creates a tricky problem of filling in only
            #      the part of the last cache block which has not been written.
            #      Also, it isn't necessary to wait for the whole file to be
            #      read. The write could proceed when the blocks being written
            #      are read. Another argument to makeCached could be the blocks
            #      to wait for, separate from the last mandatory block.
            self.makeCached(offset, self.fileSize)

            r = self.ioObject.cacheFileDescriptor
            with self.ioObject.cacheFileDescriptorLock:
                # seek and write.
                os.lseek(r, offset, os.SEEK_SET)
                wroteBytes = libc.write(r, data, size)
                if wroteBytes < 0:
                    raise CacheError("Failed to write to cache file")

                # Update file size if it changed
                if offset + size > self.fileSize:
                    self.fileSize = offset + size
                self.fileModified = True

        return wroteBytes

    @logExceptions()
    def read(self, size, offset):
        """Read data from the file.
        This method will raise a CacheRetry error if the response takes longer
        than the timeout.

        TODO: Figure out a way to add a buffer to the parameters so the buffer
        isn't allocated for each read.
        """

        logger.debug("reading %d bytes at %d " % (size, offset))

        if self.ioObject.exception is not None:
            raise self.ioObject.exception

        self.fileCondition.set_timeout()

        # Ensure the required blocks are in the cache
        self.makeCached(offset, size)

        r = self.ioObject.cacheFileDescriptor
        with self.ioObject.cacheFileDescriptorLock:
            # seek using python, avoid 32bit offset limit in libc32
            os.lseek(r, offset, os.SEEK_SET)
            cbuffer = ctypes.create_string_buffer(size)
            # do the read in libc to avoid passing around a string
            retsize = libc.read(r, cbuffer, size)
            if retsize < 0:
                raise CacheError("Failed to read from cache file")

            # if this read didn't work try again,  some bytes will come if there is a readThread active.
            # while retsize == 0 and self.readThread is not None:
            #     logger.debug("Sleeping while we wait for data to arrive due to {}.".format(self.readThread))
            #     time.sleep(5)
            #     os.lseek(r, offset, os.SEEK_SET)
            #     retsize = libc.read(r, cbuffer, size)
            #
            # # and one more try as there could be a race on the final write into cache.
            # if retsize == 0:
            #     os.lseek(r, offset, os.SEEK_SET)
            #     retsize = libc.read(r, cbuffer, size)

        logger.debug("got {} bytes from {} after all that.".format(retsize, os.lseek(r, 0, os.SEEK_CUR)))

        if retsize != size:
            newcbuffer = ctypes.create_string_buffer(cbuffer[0:retsize],
                                                     retsize)
            cbuffer = newcbuffer

        return cbuffer

    @logExceptions()
    def makeCached(self, offset, size):
        """Ensure the specified data is in the cache file.

        This method will raise a CacheRetry error if the response takes longer
        than the timeout.
        """
        firstBlock, numBlock = self.ioObject.blockInfo(offset, size)
        logger.debug("Looking for blocks in range {} to {}".format(firstBlock, firstBlock+numBlock))
        # If the whole file is cached, return
        if self.fullyCached or numBlock == 0:
            logger.debug("fully cached: {}, numBlocks: {}".format(self.fullyCached, numBlock))
            return

        lastBlock = firstBlock + numBlock - 1

        requiredRange = self.metaData.getRange(firstBlock, lastBlock)

        # If the required part of the file is cached, return
        if requiredRange == (None, None):
            return

        # There is a current read thread and it will "soon" get to the required
        # data, modify the mandatory read range of the read thread.

        # Acquiring self.fileCondition acquires self.fileLock
        logger.debug("Waiting for lock to see if we should abort current read thread.")
        with self.fileCondition:
            logger.debug("Checking if a new read thread is the best option.")
            if self.readThread is not None:
                start = requiredRange[0] * Cache.IO_BLOCK_SIZE
                size = ((requiredRange[1] - requiredRange[0] + 1) *
                        Cache.IO_BLOCK_SIZE)
                startNewThread = self.readThread.isNewReadBest(start, size)

            while self.readThread is not None:
                if startNewThread:
                    logger.debug("aborting the read thread for %s" % self.path)
                    # abort the thread
                    self.readThread.aborted = True

                    # wait for the existing thread to exit. This may time out
                    self.fileCondition.wait()
                else:
                    while (self.metaData.getRange(firstBlock, lastBlock) !=
                               (None, None) and
                                   self.readThread is not None):
                        self.fileCondition.wait()

                if (self.metaData.getRange(firstBlock, lastBlock) ==
                        (None, None)):
                    break

            # Make sure the required range hasn't changed
            requiredRange = self.metaData.getRange(firstBlock, lastBlock)
            if requiredRange == (None, None):
                return

            # No read thread running, start one.
            startByte = requiredRange[0] * Cache.IO_BLOCK_SIZE
            mandatorySize = min((requiredRange[1] + 1) * Cache.IO_BLOCK_SIZE,
                                self.fileSize) - startByte

            # Figure out where the optional end of the read should be.
            nextRead = self.metaData.getNextReadBlock(requiredRange[1])
            if nextRead == -1:
                # Reading right to the end of the file.
                optionalSize = self.fileSize - startByte
            else:
                # Reading to an intermediate end point.
                optionalSize = (nextRead * Cache.IO_BLOCK_SIZE) - startByte

            # logger.debug(" Starting a cache read thread for %d %d %d" %
            #              (startByte, mandatorySize, optionalSize))
            self.readData(startByte, mandatorySize, optionalSize)

            # Wait for the data be be available.
            while (self.metaData.getRange(firstBlock, lastBlock) !=
                       (None, None) and self.readThread is not None):
                self.fileCondition.wait()

    def fsync(self):
        if self.ioObject.exception is not None:
            raise self.ioObject.exception
        with self.fileLock:
            if self.ioObject.cacheFileDescriptor is not None:
                with self.ioObject.cacheFileDescriptorLock:
                    os.fsync(self.ioObject.cacheFileDescriptor)

    def truncate(self, length):
        # logger.debug("Truncate %s" % (self.path, ))

        if self.ioObject.exception is not None:
            raise self.ioObject.exception

        # Acquire an exclusive lock on the file
        with self.writerLock(shared=False):
            with self.fileCondition:
                if self.fileSize is not None and length == self.fileSize:
                    return

                # Tell any running read thread to exit
                if self.readThread is not None:
                    self.readThread.aborted = True
                    self.fileCondition.notify_all()

                while self.readThread is not None:
                    self.fileCondition.wait()

            # Ensure the required part of the file is in cache.
            if length != 0:
                self.makeCached(0, min(length, self.fileSize))
            with nested(self.fileLock, self.ioObject.cacheFileDescriptorLock):
                os.ftruncate(self.ioObject.cacheFileDescriptor, length)
                os.fsync(self.ioObject.cacheFileDescriptor)
                self.fileModified = True
                self.fullyCached = True
                self.fileSize = length

    @logExceptions()
    def readData(self, startByte, mandatorySize, optionalSize):
        """Read the data range from the backing store in a thread"""

        # logger.debug("Getting data: %s %s %s" % (startByte, mandatorySize,
        #                                          optionalSize))
        self.readThread = CacheReadThread(startByte, mandatorySize,
                                          optionalSize, self)
        self.readThread.start()


class CacheReadThread(threading.Thread):
    CONTINUE_MAX_SIZE = 1024 * 1024 / 2

    def __init__(self, start, mandatorySize, optionSize, fileHandle):
        """ CacheReadThread class is used to start data transfer from the back
            end in a separate thread. It also decides whether it can
            accommodate new requests or new CacheReadThreads is required.

            start - start reading position
            mandatorySize - mandatory size that needs to be read
            optionSize - optional size that can be read beyond the mandatory
            fileHandle - file handle """
        super(CacheReadThread, self).__init__(target=self.execute)
        # threading.Thread.__init__(self, target=self.execute)
        self.startByte = start
        self.mandatoryEnd = start + mandatorySize
        self.optionSize = optionSize
        if optionSize is not None:
            self.optionEnd = start + optionSize
        else:
            self.optionEnd = None
        self.aborted = False
        self.fileHandle = fileHandle
        self.currentByte = start
        self.traceback = traceback.extract_stack()

    def setCurrentByte(self, byte):
        """ To set the current byte being successfully cached"""
        self.currentByte = byte

    def isNewReadBest(self, start, size):
        """
        To determine if a new read request can be satisfied with the
        existing thread or a new thread is required. It returns true if a
        new read is required or false otherwise.
        Must be called with the #fileLock acquired
        """

        if start < self.startByte:
            return True
        if self.optionEnd is not None and (start + size) > (self.optionEnd):
            self.mandatoryEnd = self.optionEnd
            return True
        readRef = max(self.mandatoryEnd, self.currentByte)
        if (start <= readRef) or ((start - readRef)
                                      <= CacheReadThread.CONTINUE_MAX_SIZE):
            self.mandatoryEnd = max(self.mandatoryEnd, start + size)
            return False
        return True

    @logExceptions()
    def execute(self):
        try:
            self.fileHandle.readException = None
            self.fileHandle.ioObject.readFromBacking(self.optionSize,
                                                     self.startByte)
            with self.fileHandle.fileCondition:
                logger.debug("setFullyCached? %s %s %s %s" % (self.aborted,
                                                              self.startByte, self.optionSize,
                                                              self.fileHandle.fileSize))
                if self.aborted:
                    return
                elif (self.startByte == 0 and
                          (self.optionSize is None or
                                   self.optionSize == self.fileHandle.fileSize)):
                    self.fileHandle.fullyCached = True
                    # logger.debug("setFullyCached")
                elif self.fileHandle.fileSize == 0:
                    self.fileHandle.fullyCached = True
                else:
                    if self.fileHandle.fileSize is not None:
                        firstBlock, numBlocks = self.fileHandle.ioObject. \
                            blockInfo(0, self.fileHandle.fileSize)
                        requiredRange = self.fileHandle.metaData.getRange(
                            firstBlock, firstBlock + numBlocks - 1)
                        if requiredRange == (None, None):
                            self.fileHandle.fullyCached = True
                            # TODO - The file is fully cached, verify that the file
                            # matches the vospace content. Is that overly strict - the
                            # original VOFS did this, but it was subject to a much
                            # smaller read window. Also it is not invalid for the file
                            # to be replaced in vospace, and for this client to
                            # continue to serve the existing file.
        except Exception as e:
            logger.error("Exception in thread started at:\n%s" % \
                         ''.join(traceback.format_list(self.traceback)))
            logger.error(str(e))
            logger.error(traceback.format_exc())
            self.fileHandle.setReadException()
            raise
        finally:
            # logger.debug("read thread finished")
            with self.fileHandle.fileCondition:
                if self.fileHandle.readThread is not None:
                    self.fileHandle.readThread = None
                    self.fileHandle.fileCondition.notify_all()


class FlushNodeQueue(Queue):
    """
    This class implements a thread queue for flushing nodes
    """

    def __init__(self, maxFlushThreads=10):
        """Initialize the FlushNodeQueue Object

        Parameters:
        -----------
        maxFlushThreads : int - Maximum number of flush threads
        """

        Queue.__init__(self)

        # Start the worker threads
        self.maxFlushThreads = maxFlushThreads
        for i in range(self.maxFlushThreads):
            t = threading.Thread(target=self.worker)
            t.daemon = True
            t.start()

        # logger.debug("started a FlushNodeQueue with %i workers" \
        #              % self.maxFlushThreads)

    def join(self):
        # logger.debug("FlushNodeQueue waiting until all work is done")
        Queue.join(self)

    def worker(self):
        """A worker is a thin wrapper for FileHandle.flushNode()
        """
        while True:
            fileHandle = self.get()
            fileHandle.flushNode()
            self.task_done()

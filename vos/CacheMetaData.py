
import os
import cPickle as pickle

import BitVector
import logging

logger = logging.getLogger('cache')

class CacheMetaData(object):

    def __init__(self, metaDataFile, blocks, md5sum, size):
        """
        Creates an instance of CacheMetaData for the given file. If the same
        file with the same md5sum already exists in the file cache metadata
        the returned instance is updated with the existing bitmap information
        otherwise a new corresponding bitmap is created
        metaDataFile - name of the metadata file to persist to
        blocks - number of blocks required to store the entire file. If None,
                the file must exist.
        md5sum - md5sum of the file. If None, the file must exist.
        size - Size of the file. If None, the file must exist.
        """

        self.metaDataFile = metaDataFile
        self.blocks = blocks is None and 0 or blocks
        self.md5sum = md5sum
        self.size = size
        self.bitmap = None
        if os.path.exists(self.metaDataFile):
            f = open(self.metaDataFile, 'rU')
            persisted = pickle.load(f)
            if self.md5sum is None or persisted.md5sum == self.md5sum:
                #persisted bitmap still valid. Used that instead
                self.bitmap = persisted.bitmap
                self.size = persisted.size
                self.md5sum = persisted.md5sum
            f.close()

        if self.bitmap is None:
            self.bitmap = BitVector.BitVector(size=self.blocks)

    def __str__(self):
        """To create a print representation that is informative."""
        return "CacheMetaData: metaDataFile=%r bitmap=%r md5sum=%r size=%r" % (self.metaDataFile,
                                                                               self.bitmap,
                                                                               self.md5sum,
                                                                               self.size)

    def __repr__(self):
        return "CacheMetaData(metaDataFile=%r, blocks=%r, md5sum=%r, size=%r)" % (self.metaDataFile,
                                                                                  self.blocks,
                                                                                  self.md5sum,
                                                                                  self.size)
    def setReadBlocks(self, start, end):
        """ To mark several blocks as read (start and end inclusive). """
        startBlock = start
        endBlock = end
        if start < 0:
            startBlock = self.bitmap.length() + start
        if end < 0:
            endBlock = self.bitmap.length() + end
        if startBlock > endBlock:
            raise ValueError('''Incorrect interval, max is %d > %d''' %
                    (startBlock, endBlock))
        for i in range(startBlock, endBlock + 1):
            self.setReadBlock(i)

    def setReadBlock(self, block):
        """ To mark a block as read """
        self.bitmap[block] = 1

    def getBit(self, position):
        """ To return the value of the bit at position """
        return self.bitmap[position]

    def getNumReadBlocks(self):
        """ To return the number of read blocks """
        return self.bitmap.count_bits_sparse()

    def getRange(self, start, end):
        """ To return the range of blocks the client needs to download in order
            to get the given interval of blocks (start and end inclusive)

            Returned result will be the (start, end) tuple where
            start - start of the range
            end - end of the range

            Note: The returned tuple for when all the blocks in the requested
            interval are already downloaded is (None, None)
            """
        startBlock = start
        endBlock = end
        if start < 0:
            startBlock = self.bitmap.length() + start
        if end < 0:
            endBlock = self.bitmap.length() + end
        if startBlock > endBlock:
            raise ValueError('''Incorrect interval''')

        for i in range(startBlock, endBlock + 1):
            if self.bitmap[i] == 0:
                startBlock = i
                break
            if i == endBlock:
                #all the blocks are cached already
                return (None, None)

        for i in reversed(range(startBlock, endBlock + 1)):
            if self.bitmap[i] == 0:
                endBlock = i
                break

        return startBlock, endBlock

    def getNextReadBlock(self, start):
        """
        To return the next block after start that is already read. Returned
        value is the first block that is already read or -1 if none of the
        subsequent blocks is read
        """
        return self.bitmap.next_set_bit(start)

    def delete(self):
        """ To delete bitmap information both from persistence layer
            and from current object """
        os.remove(self.metaDataFile)
        self.bitmap.reset(0)

    def persist(self):
        """To persist cache file metadata for the current file """
        if not os.path.exists(os.path.dirname(self.metaDataFile)):
            os.makedirs(os.path.dirname(self.metaDataFile))
        f = open(self.metaDataFile, 'w+')
        pickle.dump(self, f, -1)
        f.close()

    @staticmethod
    def deleteCacheMetaData(metaDataFile):
        """ To delete an existing cache metadata file """
        os.remove(metaDataFile)

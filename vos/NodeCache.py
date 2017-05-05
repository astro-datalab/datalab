# A node cache class, extended from dict.

import threading
import vos


class NodeCache(dict):
    """ usage:
         # Create a node cache:
         nodeCache = NodeCache()

         with nodeCache.volatile(nodeURI):
             # Do things which make the nodes cached under nodeURI unreliable.
             # The cache will be cleared on entry

         with nodeCache.watch(nodeURI) as watch:
             # Do things which shouldn't be cached when the node is
             # volatile.
             watch.insert(node)
             # The node will not be cached if the tree became volatile
             # at any point while the nodeURI was being watched.
    """

    def __init__(self, *args):
        """ Initialize the node cache."""
        dict.__init__(self, args)
        self.lock = threading.Lock()
        self.watchedNodes = []
        self.volatileNodes = []

    def watch(self, uri):
        """Factory for watch objects"""
        return self.Watch(self, uri.rstrip('/'))

    def volatile(self, uri):
        """Factory for volatile objects."""
        return self.Volatile(self, uri.rstrip('/'))

    def __missing__(self, key):
        """Attempting to access a non-cached node returns None rather than
           raising an exception."""
        return None

    def __setitem__(self, key, object):
        """If an node is directly inserted into the cache, automatically create
           a watch."""
        with self.watch(key) as w:
            w.insert(object)

    def __getitem__(self, key):
        return dict.__getitem__(self, key.rstrip('/'))

    def __contains__(self, key):
        return dict.__contains__(self, key.rstrip('/'))

    class Volatile(object):
        """ Objects that mark a code segment where a uri is volatile and
            shouldn't be used from the cache."""

        def __init__(self, nodeCache, uri):
            self.nodeCache = nodeCache
            self.uri = uri.rstrip('/')

        def __enter__(self):
            """ Mark any sub-trees being watched as being dirty
                add to self.nodeCache.volatileNodes.
                Remove any cached nodes in the volatile subtree.
            """

            with self.nodeCache.lock:
                # Add this volatile objecty to a list of all active volatile
                # objects.
                self.nodeCache.volatileNodes.append(self)

                # Remove any cached nodes in the volatile sub-tree.
                for uri in self.nodeCache.keys():
                    if uri.startswith(self.uri):
                        del self.nodeCache[uri]

                # Mark any watched nodes in the volatile sub-tree dirty
                for watchedNode in self.nodeCache.watchedNodes:
                    if watchedNode.uri.startswith(self.uri):
                        watchedNode.dirty = True

            return self

        def __exit__(self, exc_type, exc_value, traceback):
            """ Remove this volitile object from the list of active volatiles.
            """
            with self.nodeCache.lock:
                self.nodeCache.volatileNodes.remove(self)

    class Watch(object):
        """ Objects that mark a code segment where a node has been read from
            vospace, and is intended to be cached.
        """

        def __init__(self, nodeCache, uri):
            self.nodeCache = nodeCache
            self.uri = uri
            self.dirty = False

        def __enter__(self):
            with self.nodeCache.lock:
                # Add this watch object to the list of active watch objects.
                self.nodeCache.watchedNodes.append(self)

                # Check to see if this watch object is in an existing volatile
                # tree. If it is, mark this watch object as dirty.
                for thisVolatile in self.nodeCache.volatileNodes:
                    if self.uri.startswith(thisVolatile.uri):
                        self.dirty = True
                        return self
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            with self.nodeCache.lock:
                self.nodeCache.watchedNodes.remove(self)

        def insert(self, object):
            """ Insert an object in the cache, but only if the watch is not
            dirty."""
            if not self.dirty:
                dict.__setitem__(self.nodeCache, self.uri, object)

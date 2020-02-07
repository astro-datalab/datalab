"""Data Lab helpers for clustering."""

__authors__ = 'Robert Nikutta <nikutta@noao.edu>, Data Lab <datalab@noao.edu>'
__version__ = '20200204' # yyyymmdd

# 3rd party
import numpy as np


def findClusters(x,y,method='MiniBatchKMeans',**kwargs):  # x,y can be for instance ra,dec

    """Find 2D clusters from x & y data.

    Parameters
    ----------
    x, y : seq (e.g. tuple,list,1-d array)
        Location of points in (x,y) feature space, e,g, RA & Dec, but
        x & y need not be spatial in nature.

    method : str
        Cluster finding method from :mod:`sklearn.cluster` to
        use. Default: 'MiniBatchKMeans' (a streaming implementation of
        KMeans), which is very fast, but not the most robust. 'DBSCAN'
        is much more robust, but MUCH slower. For other methods,
        consult :mod:`sklearn.cluster`.

    **kwargs
        Any other keyword arguments will be passed to the cluster
        finding method. If method='MiniBatchKMeans' or 'KMeans',
        n_clusters (integer number of clusters to find) must be
        passed, e.g.

        .. code-block:: python

           clusters = findClusters(x,y,method='MiniBatchKMeans',n_clusters=3)

    """

    import sklearn.cluster as C #import KMeans, MiniBatchKMeans, DBSCAN

    if method in ('MiniBatchKMeans','KMeans'):
        if 'n_clusters' not in kwargs:
            kwargs['n_clusters'] = 3
            
    try:
        METHOD = getattr(C,method)(**kwargs)
    except Exception as e:
        print (str(e))
        raise

    X = np.matrix(zip(x,y))
    
    clusters = METHOD.fit(X)

    return clusters


def constructOutlines(x,y,clusterlabels):  # compute convex hull, one per cluster label

    """Construct the convex hull (outline) of points in (x,y) feature space,

    Parameters
    ----------
    x, y : seq (e.g. tuple,list,1-d array)
        Location of points in (x,y) feature space (e,g, RA & Dec).

    Returns
    -------
    hull : instance
        The convex hull of points (x,y), an instance of
        :class:`scipy.spatial.qhull.ConvexHull`.

    Example
    -------
    Given `x` & `y` coordinates as 1d sequences:

    .. code-block:: python

       points = np.vstack((x,y)).T  # make 2-d array of correct shape
       hull = constructOutlines(x,y)
       plt.plot(points[hull.vertices,0], points[hull.vertices,1], 'r-', lw=2) # plot the hull
       plt.plot(points[hull.vertices[0],0], points[hull.vertices[0],1], 'r-') # closing last point of the hull

    """

    from scipy.spatial import ConvexHull
    points = np.vstack((x,y)).T
    hull = ConvexHull(points)
        
    return hull

"""Data Lab plotting helpers.""" 

__authors__ = 'Robert Nikutta <nikutta@noao.edu>, Data Lab <datalab@noao.edu>'
__version__ = '20200204' # yyyymmdd

# 3rd party
import numpy as np
import pylab as plt

# own
from helpers.cluster import constructOutlines


def plotSkymapScatter(x,y,c=None,clusterlabels=None,s=3,plot='both',xlabel='RA',ylabel='Dec',clabel='',title='',projection='aitoff',**kwargs):

    """Plot an all-sky projection of data x,y.

    Parameters
    ----------
    x, y : sequences
        1-d sequence of data (both same length), typically RA & Dec,
        to plotted via scatter onto an all-sky projection. If `c=None`
        and `clusterlabels=None`, the plot is a simple scatter plot.

    c : seq or None
        If not None, `c` is a 1-d sequence of the same length as `x` &
        `y`, and will be used as the value for the colormap applied to
        the datapoints (x,y). If c is not None, a colorbar will be
        plotted alongside the sky map.

    clusterlabels : seq or None
        If not None, `clusterlabels` is a 1-d sequence of same length
        as `x` & `y`, and carries 'label' values for each (x,y) pair
        designating that datapoint as member of some cluster or
        class. In this case, all scatter points with the same value in
        `clusterlabels` will be plotted in the same color.

    s : float
        Marker size for scatter plot. Will be passed to matplotlib's
        :func:`scatter()`. Default: s=3.

    plot : str
        Either 'both' (default) or 'scatter' or 'outlines'. If
        clusterlabels is not none, then plot='outlines' or plot='both'
        triggers the computation of outlines (as convex hulls) around
        all clusters identified by the labels in `clusterlabels`. If
        plot='both', the scatter points and the outlines will be
        plotted. If plot='outlines', only the outlines will be
        plotted.

    xlabel, ylabel : str
        x and y labels to be plotted. Defaults: 'RA' and 'Dec'. To
        turn off labels, supply ''.

    clabel : str
        Colorbar label (if c is not None).

    title : str
        Figure title. Default ''.

    projection : str
        Projection of the all-sky map. Default: 'aitoff'. For other
        options, check:

        .. code-block:: python

           from matplotlib import projections
           projections.get_projection_names()
              [u'aitoff', u'hammer', u'lambert', u'mollweide', u'polar', u'rectilinear']

    **kwargs : keyword arguments
        All kwargs will be passed on to pylab.scatter().

    """

    plt.figure(figsize=(14,7))
    ax = plt.subplot(111, projection=projection)

    if clusterlabels is not None:

        if plot in ('scatter','both'):
            for label in np.unique(clusterlabels):
                sel = (clusterlabels == label)
                im = plt.scatter(x[sel], y[sel], marker='o', s=s, edgecolors='none', alpha=1, label=label, **kwargs)

            plt.legend(loc='upper right',title='clusters',markerscale=5)

        if plot in ('outlines','both'):
            for label in np.unique(clusterlabels):
                sel = (clusterlabels == label)
                x_ = x[sel]
                y_ = y[sel]
                points = np.vstack((x_,y_)).T
                hull = constructOutlines(x_,y_,clusterlabels)
                plt.plot(points[hull.vertices,0], points[hull.vertices,1], 'r-', lw=2) # plot the hull
                plt.plot(points[hull.vertices[0],0], points[hull.vertices[0],1], 'r-') # closing last point of the hull

    else:
        im = plt.scatter(x, y, marker='o', s=s, c=c, edgecolors='none', alpha=1, **kwargs)

        if c is not None:
            cb = plt.colorbar(im)
            cb.set_label(clabel)

    plt.title(title,y=1.08)
    plt.grid(True)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

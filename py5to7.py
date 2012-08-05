def next(iterable, *args):
    """next(iterator[, default])

    Return the next item from the iterator. If default is given and the
    iterator is exhausted, it is returned instead of raising StopIteration.
    """
    if not hasattr(iterable, 'next'):
        raise TypeError(
            "%s object is not an iterator" % type(iterable).__name__)

    if not args:
        return iterable.next()

    try:
        return iterable.next()
    except StopIteration:
        return args[0]

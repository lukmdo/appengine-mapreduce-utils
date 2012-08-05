# Exension utilites for [appengine-mapreduce](http://code.google.com/p/appengine-mapreduce/)

## How to start

:collision: setup [appengine SDK](https://developers.google.com/appengine/downloads#Google_App_Engine_SDK_for_Python) DONT use mapreduce bundle

```bash
$ svn export http://appengine-mapreduce.googlecode.com/svn/trunk/python/src/mapreduce
$ python test_mapreduce_utils.py
```

## What you get:

- extends query filtering options intorduced in [appengine-mapreduce@324](http://code.google.com/p/appengine-mapreduce/source/detail?spec=svn329&r=324)
- adds```pre_map_filter``` for this cases when you can NOT filter on query and also filtering in ``map`` stage is alsp NOT an option
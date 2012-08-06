# Exension utilites for [appengine-mapreduce](http://code.google.com/p/appengine-mapreduce/)

## How to start
:collision: setup [appengine SDK](https://developers.google.com/appengine/downloads#Google_App_Engine_SDK_for_Python) DONT use mapreduce bundle

```bash
$ svn export http://appengine-mapreduce.googlecode.com/svn/trunk/python/src/mapreduce
$ python test_mapreduce_utils.py
```

## What you get:
- extends query filtering options intorduced in [appengine-mapreduce@324](http://code.google.com/p/appengine-mapreduce/source/detail?spec=svn329&r=324)
  - all filter options ```(=, !=, <, <=, >, >=, IN)```
  - filter where value is ```str(key)```
- Optional. Adds```filter_factory_spec``` for these cases when you can NOT filter on query neither on ``map`` stage


## Example setup
Using [google mapreduce helloworld example](https://developers.google.com/appengine/docs/python/dataprocessing/helloworld)
lets assume that app stores tokenized file data in ```Token``` and keeps back references to ```File```.

```python
class File(db.Model):
    name = db.StringProperty()
    ...
    last_update = db.DateTimeProperty(auto_now=True)

class Token(db.Model):
    file = db.ReferenceProperty(File, collection_name='file_tokens')
    type = db.StringProperty(choices=('A', 'B', 'C', 'D'))
    data = db.TextProperty()
    ...

# Mapper
def word_count_map(token_entity):
    """Modified Word Count map function"""
    yield (token_entity.data.lower(), "")

# Reducer
def word_count_reduce(key, values):
    """Word Count reduce function."""
    yield "%s: %d\n" % (key, len(values))
```
Just for demo purposes lets modify the original task a little. Instead of processing all the files lets analise cumulative word frequency tokens of type ```B``` or ```C``` from files that were updated after ```2012-12-12 12:12:12```.

Unless you use [ndb](https://developers.google.com/appengine/docs/python/ndb/) you can NOT filter on ```File``` properties when fetching from ```Token```. In scenarios when filtering on mapper is not a way out ```filter_factory``` is an option.

```python
def filter_factory_tokens_from_files(last_update):
    """Returns predicate function that will populate filter predicate:
       predicate = filter_factory(*filter_factory_args, **filter_factory_kwargs)
       data_for_mappers = filter(predicate, data)
    """
    query = File.all(keys_only=True).filter('last_update >', last_update)
    pass_file_keys = dict((k, True) for k in query)
    return lambda token, pass_file_keys=pass_file_keys:\
    Token.file.get_value_for_datastore(token) in pass_file_keys

# all we miss is the Pipeline definition...
class WordCountPipeline(base_handler.PipelineBase):

    def run(self, last_update, *args, **kwargs):
        """Analyses ``Tokens`` frequency filtered by ``filters``
        from files modified after ``last_update``.
        """
        yield mapreduce_pipeline.MapreducePipeline(
            "wordcount_example",
            "mapreduce_utils_example.word_count_map",
            "mapreduce_utils_example.word_count_reduce",
            input_reader_spec="mapreduce_utils.DatastoreQueryInputReader",
            output_writer_spec="mapreduce.output_writers.BlobstoreOutputWriter",
            mapper_params={
                "input_reader":{
                    "entity_kind": "mapreduce_utils_example.Token",
                    "filter_factory_spec": {
                        "name": "mapreduce_utils_example.foo_filter_factory"
                        "args": [last_update],
                    },
                    "filters": [
                        ("type", "IN", ['B', 'C']),
                    ],
                },
            },
            reducer_params={
                "mime_type": "text/plain",
            },
            shards=50)
...

# then just start the pipeline
some_date = datetime.datetime(2012, 12, 12, 12, 12, 12)

wordcount_pipeline = WordCountPipeline(some_date)
wordcount_pipeline.start(queue_name="mapreduce")  # use separate queue
```

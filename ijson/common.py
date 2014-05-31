'''
Backend independent higher level interfaces, common exceptions.
'''

class JSONError(Exception):
    '''
    Base exception for all parsing errors.
    '''
    pass

class IncompleteJSONError(JSONError):
    '''
    Raised when the parser expects data and it's not available. May be
    caused by malformed syntax or a broken source stream.
    '''
    def __init__(self):
        super(IncompleteJSONError, self).__init__('Incomplete or empty JSON data')

def parse(basic_events):
    '''
    An iterator returning parsing events with the information about their location
    with the JSON object tree. Events are tuples ``(prefix, type, value)``.

    Available types and values are:

    ('null', None)
    ('boolean', <True or False>)
    ('number', <int or Decimal>)
    ('string', <unicode>)
    ('map_key', <str>)
    ('start_map', None)
    ('end_map', None)
    ('start_array', None)
    ('end_array', None)

    Prefixes represent the path to the nested elements from the root of the JSON
    document. For example, given this document::

        {
          "array": [1, 2],
          "map": {
            "key": "value"
          }
        }

    the parser would yield events:

      ('', 'start_map', None)
      ('', 'map_key', 'array')
      ('array', 'start_array', None)
      ('array.item', 'number', 1)
      ('array.item', 'number', 2)
      ('array', 'end_array', None)
      ('', 'map_key', 'map')
      ('map', 'start_map', None)
      ('map', 'map_key', 'key')
      ('map.key', 'string', u'value')
      ('map', 'end_map', None)
      ('', 'end_map', None)

    '''
    path = []
    for event, value in basic_events:
        if event == 'map_key':
            prefix = '.'.join(path[:-1])
            path[-1] = value
        elif event == 'start_map':
            prefix = '.'.join(path)
            path.append(None)
        elif event == 'end_map':
            path.pop()
            prefix = '.'.join(path)
        elif event == 'start_array':
            prefix = '.'.join(path)
            path.append('item')
        elif event == 'end_array':
            path.pop()
            prefix = '.'.join(path)
        else: # any scalar value
            prefix = '.'.join(path)

        yield prefix, event, value


class ObjectBuilder(object):
    '''
    Incrementally builds an object from JSON parser events. Events are passed
    into the `event` function that accepts two parameters: event type and
    value. The object being built is available at any time from the `value`
    attribute.

    Example::

        from StringIO import StringIO
        from ijson.parse import basic_parse
        from ijson.utils import ObjectBuilder

        builder = ObjectBuilder()
        f = StringIO('{"key": "value"})
        for event, value in basic_parse(f):
            builder.event(event, value)
        print builder.value

    '''
    def __init__(self):
        def initial_set(value):
            self.value = value
        self.containers = [initial_set]

    def event(self, event, value):
        if event == 'map_key':
            self.key = value
        elif event == 'start_map':
            map = {}
            self.containers[-1](map)
            def setter(value):
                map[self.key] = value
            self.containers.append(setter)
        elif event == 'start_array':
            array = []
            self.containers[-1](array)
            self.containers.append(array.append)
        elif event == 'end_array' or event == 'end_map':
            self.containers.pop()
        else:
            self.containers[-1](value)

def items(prefixed_events, prefix):
    '''
    An iterator returning native Python objects constructed from the events
    under a given prefix.
    '''
    prefixed_events = iter(prefixed_events)
    try:
        while True:
            current, event, value = next(prefixed_events)
            if current == prefix:
                if event in ('start_map', 'start_array'):
                    builder = ObjectBuilder()
                    end_event = event.replace('start', 'end')
                    while (current, event) != (prefix, end_event):
                        builder.event(event, value)
                        current, event, value = next(prefixed_events)
                    yield builder.value
                else:
                    yield value
    except StopIteration:
        pass

class ijsondict(dict):
    '''
    A dictionary object which has an generator in place of the dictionary
    item to be iterated over.

    Parameters:

    - parser: a basic_parser object
    - prefix: string indicating location of array to target.

    An example expected usage for this kind class would be for streaming data 
    provided by a REST api similar to Neo4j's which first emits column information
    and then streams the rest of the data. In this case you might do something like:

    results = ijsondict(parser, "data.item")
    results = izip(results["columns"], results["data"])
    '''
    classmap = {'start_map':dict, 'start_array':list}
    def __init__(self, parser, prefix, **kwargs):
        self.keys_stack = []
        self.data_stack = []

        if isinstance(prefix, basestring):
            self.prefix = prefix.split('.')
        elif isinstance(prefix, list):
            self.prefix = prefix
        else:
            raise ValueError
        self._start_parse(parser)

    def _start_parse(self, parser):
        '''Build's up the dictionary until we locate the first 
        instance of the prefix, then defers the remainder of the job to 
        the generator function.
        '''
        keys_stack = self.keys_stack
        data_stack = self.data_stack

        first = True
        this = self

        prefix = self.prefix[:-1]
        for token, value in parser:
            if token.startswith('start_'):
                break_flag = False
                if keys_stack == prefix:
                    break_flag = True
                    this = self._json_generator(parser)
                else:
                    if first:
                        first = False
                    else:
                        this = self.classmap[token]() 

                if len(keys_stack):
                    thiskey = keys_stack[-1]
                    thisdata = data_stack[-1]
                    if isinstance(thisdata, dict):
                        thisdata[thiskey] = this
                    else:
                        thisdata.append(this)
                data_stack.append(this)
                keys_stack.append("item")
                if break_flag:
                    break
            elif token.startswith('end_'):
                keys_stack.pop()
                data_stack.pop()
            elif token == 'map_key':
                keys_stack[-1] = value
            else:
                key = keys_stack[-1]
                data = data_stack[-1]
                if isinstance(data, dict):
                    data[key] = value
                else:
                    data.append(value)

    def _json_generator(self, parser):
        '''Resumes from where _start_parse left of, yielding items and
        then continues building the dictionary.'''
        keys_stack = self.keys_stack
        data_stack = self.data_stack

        data_stack[-1] = []

        popped = None
        yield_flag = False
        for token, value in parser:
            if token.startswith('start_'):
                this = self.classmap[token]()
                if len(keys_stack):
                    thiskey = keys_stack[-1]
                    thisdata = data_stack[-1]
                    if isinstance(thisdata, dict):
                        thisdata[thiskey] = this
                    else:
                        thisdata.append(this)
                data_stack.append(this)
                keys_stack.append('item')
            elif token.startswith('end_'):
                keys_stack.pop()
                value = data_stack.pop()
                yield_flag = True
            elif token == 'map_key':
                keys_stack[-1] = value
            else:
                key = keys_stack[-1]
                data = data_stack[-1]
                if isinstance(data, dict):
                    data[key] = value
                else:
                    data.append(value)
                yield_flag = True

            if yield_flag and keys_stack == self.prefix:
                yield value
                yield_flag = False

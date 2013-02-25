Plugins
=======

A plugin is a class that Monolith uses to extract or inject
data.

Writing a plugin is done by using the :class:`aggregator.plugins.Plugin` base
class and overriding a few methods.

You can create:

- **source** plugins, that will be used to extract
  data from a source.
- **target** plugins, that will be used to inject data
  previously extracted.
- hybrid plugins, that implement both behaviors.

When a plugin is instanciate, it gets all options that were
defined in the configuration file section. The base
class constructor takes care of setting the **options** ibject
attribute.

When Monolith is run, a single instance of plugin is created
per source and target sections.


Source plugins
--------------

A source plugin must implement one method called **extract**.
The method takes two parameters: *start_date* and *end_date*, which
defines the range of the extraction. The plugin must
return an iterator containing lines of data.

Each line is a mapping that contains the following keys:

- **date**: the date of the data line - *mandatory*
- **category**: the category of the data - *optional*

Every extra key will be stored as data.

Example:

.. code-block:: python

   from aggregator.plugins import Plugin


   class MyPlugin(Plugin):

       def extract(self, start_date, end_date):
            date = start_date
            while date <= end_date:
                # extract data from somewhere ...
                data = get_data(date)

                # add date and category keys
                data['date'] = date
                data['category'] = 'mycategory'
                yield data
                date += datetime.timedelta(days=1)


Some plugins may need to purge the data once the extraction occurred.

To do this you need to implement the **purge** method:

.. code-block:: python

   from aggregator.plugins import Plugin


   class MyPlugin(Plugin):
       def purge(self, start_date, end_date):
           # purge source data for this date range


Target plugins
--------------

Target plugins need to use the same base class, but implement the **inject**
method. The method gets a iterable of lines to inject.

Example:

.. code-block:: python

   from aggregator.plugins import Plugin


   class MyPlugin(Plugin):

       def inject(self, batch):
           for line in batch:
               # put the data somewhere


Hybrid plugins
--------------

Hybrid plugins implement both behaviors. This can be useful if you want to
share a common set of options.

Example:

.. code-block:: python

   from aggregator.plugins import Plugin


   class MyPlugin(Plugin):

       def inject(self, batch):
           for line in batch:
               # put the data somewhere

       def extract(self, start_date, end_date):
            date = start_date
            while date <= end_date:
                # extract data from somewhere ...

       def purge(self, start_date, end_date):
           # purge source data for this date range

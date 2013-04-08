Configuration
=============

**monolith-extract** is based on a configuration file that defines
sequences to run.

Each source and each target are defined in a section *prefixed*
by **source:** or **target**.

The file also defines **phases**, that reunite a list of **sources**
and **targets**.

Last, a **sequence** is built using a list of **phases**.

Everything is run asynchronously but the system makes sure
a phase is over when starting a new one in a sequence.

This is useful when you need a two-phase strategy.

Each *target* and *source* section has two mandatory options:

- **id**: a unique identifier. The identifier is prefixed by
  **source:** or **target:** in the system, so you can have
- **use**: the fully qualified name of the plugin class
  that will be used.

The rest of the section is passed to the plugin.

Here's a full example:

.. code-block:: ini

    [monolith]
    timeout = 10
    history = mysql+pymysql://user:password@localhost/monolith
    sequence = extract, load

    [phase:extract]
    sources = ga
    targets = sql

    [phase:load]
    sources = sql
    targets = es

    [target:es]
    id = es
    use = monolith.aggregator.plugins.es.ESWrite
    url = http://es/is/here

    [source:sql]
    id = sql
    use = monolith.aggregator.db.Database
    database = mysql+pymysql://monolith:monolith@localhost/monolith

    [target:sql]
    id = sql
    use = monolith.aggregator.db.Database
    database = mysql+pymysql://monolith:monolith@localhost/monolith

    [source:ga]
    id = ga-pageviews
    use = monolith.aggregator.plugins.ganalytics.GoogleAnalytics
    metrics = ga:pageviews
    dimensions = browser
    oauth_token = %(here)s/auth.json
    profile_id = 12345678


**use** points to a callable that will be invoked with all the other variables
of the section and the variables defined in **monolith** to perform the work.

*monolith-extract* invokes in parallel every source & target callable, using
a queue where the data is produced and consumed - once the import is successful,
the script calls the purge method on every source, allowing them to cleanup
if needed.

**The call on purge() implies that the data was safely pushed in the MySQL
database**

*monolith-extract* can run the predefined sequence, or one passed as an option.
This is useful when you just need to replay a specific phase.


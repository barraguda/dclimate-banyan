Example usage
-------------

First, you define the data to be stored in a datastream:

.. doctest::
   :pyversion: ~= 3.9

   >>> from dc_banyan import (
   ...     DataDefinition,
   ...     Float,
   ...     Timestamp,
   ... )
   >>> data_definition = DataDefinition([
   ...     ("ts", Timestamp, True),
   ...     ("temp", Float, True),
   ... ])
   >>> data_definition
   DataDefinition([ColumnDefinition { position: 0, name: "ts", kind: Timestamp, index: true }, ColumnDefinition { position: 1, name: "temp", kind: Float, index: true }])

Before we can interact with any datastreams, we need a datastore. For real world
usage we'll want to use IPFS, but we can also use an in-memory store for testing
if there is no local IPFS node running:

.. doctest::
   :pyversion: ~= 3.9

   >>> from dc_banyan import (
   ...     ipfs_available,
   ...     ipfs_store,
   ...     memory_store,
   ... )
   >>> one_megabyte = 1<<20   # max amount of RAM to use for in-memory store
   >>> store = ipfs_store() if ipfs_available() else memory_store(one_megabyte)

Then we can make some records:

.. doctest::
   :pyversion: ~= 3.9

   >>> from datetime import datetime
   >>> records = [
   ...     data_definition.record({"ts": datetime(2023, 8, 14, 11, 14, 00), "temp": 81.2}),
   ...     data_definition.record({"ts": datetime(2023, 8, 14, 12, 14, 00), "temp": 88.2}),
   ... ]
   >>> records
   [Record { data_definition: DataDefinition([ColumnDefinition { position: 0, name: "ts", kind: Timestamp, index: true }, ColumnDefinition { position: 1, name: "temp", kind: Float, index: true }]), values: {"temp": Float(81.2), "ts": Timestamp(2023-08-14T11:14:00)} }, Record { data_definition: DataDefinition([ColumnDefinition { position: 0, name: "ts", kind: Timestamp, index: true }, ColumnDefinition { position: 1, name: "temp", kind: Float, index: true }]), values: {"temp": Float(88.2), "ts": Timestamp(2023-08-14T12:14:00)} }]


In order to store them, we need a datastream. Since we don't have one yet, we'll create a new one:

.. doctest::
   :pyversion: ~= 3.9

   >>> from dc_banyan import new_datastream
   >>> datastream = new_datastream(store, data_definition)

Because data in IPLD (and therefore IPFS and Banyan) data is immutable,
"modifying" the datastream really means we create a new datastream including the
new data, with a new content identifier (CID).

.. doctest::
   :pyversion: ~= 3.9

   >>> datastream = datastream.extend(records)
   >>> cid = datastream.cid
   >>> cid
   'bafyreibnskx6hfiwpnqcqguz75ieupi6tqr5p6a5yzphklkb5cyz4lis3a'

We can use the CID to get access to the datastream later:

.. doctest::
   :pyversion: ~= 3.9

   >>> from dc_banyan import load_datastream
   >>> datastream = load_datastream(cid, store, data_definition)
   >>> datastream[1]
   Record { data_definition: DataDefinition([ColumnDefinition { position: 0, name: "ts", kind: Timestamp, index: true }, ColumnDefinition { position: 1, name: "temp", kind: Float, index: true }]), values: {"temp": Float(88.2), "ts": Timestamp(2023-08-14T12:14:00)} }

We can even query the datastream (which isn't very interesting with just two records, but imagine millions of records):

.. doctest::
   :pyversion: ~= 3.9

   >>> query = (data_definition["temp"] >= 85.0) & (data_definition["temp"] < 90.0)
   >>> results = datastream.query(query)
   >>> list(results)
   [Record { data_definition: DataDefinition([ColumnDefinition { position: 0, name: "ts", kind: Timestamp, index: true }, ColumnDefinition { position: 1, name: "temp", kind: Float, index: true }]), values: {"temp": Float(88.2), "ts": Timestamp(2023-08-14T12:14:00)} }]


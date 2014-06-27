# RDF4H-HDBI

This is an HDBI backend for RDF4H.

Currently, it is intended as a method to store/load a whole graph (or its parts, at your own risk),
and does not work properly with *updating* the data. This means that once you load a graph
from the database, make some changes to it and try to store it, rdf4h-hdbi will not be so smart
as to properly add stuff, missing in the database or delete stuff which is no longer in the graph.
Ultimately, this results in *merging* old and new data and an invalid graph, as the result.
To ensure that the graph is stored properly, you have to wipe the data from the database before
storing the graph.


## Usage

Before doing anything, you may need to prepare the database using `createStorage`.

To start working with the backend, you are expected to supply a connection instance to it.
You have to import the requird module of HDBI, e.g. hdbi-sqlite to connect to your kind of database.

    import Database.HDBI.Sqlite
    import Data.RDF.Persistence

    -- Get your RDF graph ready (rdf).

    -- Create a connection instance.
    conn <- connectSqlite3 "my-rdf.sqlite"

    -- Store your RDF graph in the database.
    storeRDF conn "graph1" rdf

    -- Load another graph from the database.
    rdf1 <- loadRDF conn "graph2" :: IO TriplesGraph

    disconnect conn

If you no longer need the graph tables in the database, you can delete them
using `deleteStorage`.


## Authors and Contributors

* [Slava Kravchenko](https://github.com/cordawyn)

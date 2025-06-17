EBooklet
==================================

Introduction
------------
EBooklet is a pure python key-value file database that can be synced with a remote S3 system (AWS or otherwise). It builds upon the `Booklet python package <https://github.com/mullenkamp/booklet>`_. It allows for multiple serializers for values, but requires that the keys are strings (object name requirements in S3). In addition to the `MutableMapping <https://docs.python.org/3/library/collections.abc.html#collections-abstract-base-classes>`_ class API and the `dbm <https://docs.python.org/3/library/dbm.html>`_ methods (i.e. sync and prune), EBooklet contains some additional methods for managing the interactions between the local and remote data.
It is thread-safe on writes (using thread locks) and multiprocessing-safe (using file locks) including on the S3 remote (using object locking). Reads are not thread safe.

When an error occurs (e.g. trying to access a key that doesn't exist), Ebooklet will try to properly close the file and remove the file (object) locks. This will not sync any changes, so the user will lose any changes that were not synced. There will be circumstances that can occur that will not properly close the file, so care still needs to be made.

Installation
------------
Install via pip::

  pip install ebooklet


I'll probably put it on conda-forge once I feel appropriately motivated...


Booklet vs EBooklet
-------------------
The `Booklet python package <https://github.com/mullenkamp/booklet>`_ is a single file key/value database and is used as the foundation for EBooklet. Booklet manages the local data, while EBooklet manages the interaction between the remote data and the local data. It is best to familiarize yourself with Booklet before using EBooklet. This is especially true when you're not collaborating with others on a project and simply need to save and retrieve data occasionally from your remote.

EBooklet has been designed in a way that allows the user to primarily work using Booklet and then have their local files pushed up to the S3 remote later via EBooklet. In other words, you don't have to always open your file using EBooklet whenever you're doing work. If you're actively collaborating with others and data is being modified, then it is best to open the data using EBooklet to ensure data conflicts do not occur.

Unlike Booklet which uses threading and OS-level file locks (which are very fast), EBooklet uses an S3 locking method when a file is open for writing. This ensures that only a single process has write access to a remote database at a time, but it's also relatively slow (compared to file locks).


Connection objects
-------------------
To interact with remote S3 systems, you'll need to create an S3Connection object. The S3Connection object contains all of the parameters and credentials necessary to know where the remote database should live. If your writing to an S3 remote, then you'll need the access_key_id, access_key, database key, and bucket (at a minimum). There are additional options that include database url (if it's publicly accessible) and endpoint_url (if it's not AWS).


.. code:: python
  
  import ebooklet

  access_key_id = 'my key id associated with the access key'
  access_key = 'my super secret key'
  db_key = 'big_data.blt'
  bucket = 'big_bucket'
  endpoint_url = 'https://s3.us-west-001.backblazeb2.com' # Example for Backblaze (highly recommended S3 system)
  db_url = 'https://big_bicket.org/big_data.blt' # Public URL path to database

  remote_conn = ebooklet.S3Connection(access_key_id, access_key, db_key, bucket, endpoint_url=endpoint_url, db_url=db_url)


Once you have the S3Connection object, then you can pass it to the ebooklet.open function to open a database along with a local file path.


.. code:: python

  local_file_path = '/path_to_file/big_data.blt'

  db = ebooklet.open(remote_conn, local_file_path, flag='c', value_serializer='pickle')


If you're only going to open a database for reading and you have the db_url, then you don't even need to create the S3Connection object. You can simply pass the db_url string to the remote_conn parameter of the ebooklet.open function.

Be careful with the flags. Using the 'n' flag with ebooklet.open will delete the remote database in addition to the local database.


.. code:: python

  db = ebooklet.open(db_url, local_file_path, flag='r') # The database must exist in the remote to open with 'r'


All of the normal reading and writing API is identical to booklet (and the dbm API). But it is recommended to use the context manager to ensure the database is properly closed.


.. code:: python

  db['key1'] = ['one', 2, 'three', 4]

  value1 = db['key1']

  db.close()

  with ebooklet.open(remote_conn, local_file_path) as db:
    value1 = db['key1']


Interacting with the S3 remote database
----------------------------------------
Where EBooklet differs from Booklet in its API is when it's interacting with the S3 remote. This follows some of the concepts and terminology used by Git.

Changes
~~~~~~~~
The "changes" method produces a Change object that allows you to see what changes have exist between the local and remote, and it allows you to "push" the local changes to the remote.


.. code:: python

  with ebooklet.open(remote_conn, local_file_path, 'w') as db:
    changes = db.changes() # Open the Changes object

    for change in changes.iter_changes(): # Iterate through all of the differences between the local and remote
      print(change)

    changes.push()  # Push the changes in the local up to the remote


Other methods on the remote
~~~~~~~~~~~~~~~~~~~~~~~~~~~
The delete_remote method deletes an entire remote database.

The copy_remote method copies the current database to another remote location (using another S3Connection object). If both S3Connection objects use the same access_key and access_key_id, then the copy is directly remote to remote (using the S3 copy_object function). If the credentials are not the same, then it must first be downloaded locally then uploaded. Both S3Connection objects must be open for writing via EBooklet (though this might change in the future). 

The load_items method downloads the keys/values to the local database, but does not return those keys and values (unlike the get_items method).


Remote Connection Groups
------------------------
Remote connection groups allow for organizing and storing groups of S3Connection objects. All data from an S3Connection object is stored excluding the access_key and access_key_id. This could be used to grouping different versions of databases together or related databases.
Remote connection groups are currently quite basic, but the functionality may expand over time.

They function like a Booklet/EBooklet except that they have one additional method called "add" (and set has been removed). The keys are the UUIDs of the databases and the values are python dictionaries of the S3Connection parameters. The returned python dict also contains other metadata related to the database including the user-defined metadata.

The remote connection must already exist to be added to a remote connection group.


.. code:: python

  remote_conn_rcg = ebooklet.S3Connection(access_key_id_rcg, access_key_rcg, db_key_rcg, bucket_rcg, endpoint_url=endpoint_url_rcg, db_url=db_url_rcg)

  with ebooklet.open(remote_conn_rcg, local_file_path_rcg, 'n', remote_conn_group=True) as db_rcg:
    db_rcg.add(remote_conn)

    changes = db_rcg.changes()
    changes.push()






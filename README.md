# THE DATALAB COMMAND LINE CLIENT

``datalab`` is command-line Python client for NOIRLab's [Astro Data Lab](https://datalab.noirlab.edu).

It provides easy access to Data Lab functionalities:

* synchronous and asynchronous database queries (TAP)
* your remote file storage (VOSpace)
* your remote database tables (MyDB)

Authentication to Data Lab is based on a username and password.


## System requirements

* A Data Lab account
* Python 3.x (Python 3.8 recommended; **We are no longer supporting Python 2**)
* fuse or OSX-FUSE (if you want to mount the remote storage as a local filesystem)

For Ubuntu users:

* If the pip installation instructions below fail for you complaining about a missing library `libcurl4-openssl-dev`, please install it using your software/package manager.

## Installation

The ``astrodatalab`` package installs the ``datalab`` command line
client, and some Data Lab Python libraries that allow you to use Data
Lab functionality locally on your computer (for instance in Ipython
etc.)

### Install via pip

The easiest way to install the ``datalab`` client is via pip:

```
pip install --ignore-installed --no-cache-dir astro-datalab
```

The flags `--ignore-installed` and `--no-cache-dir` should ensure that the latest version is pulled freshly from the internet.
### Install from sources

You can also install the `datalab` client from source on 
[GitHub](https://github.com/astro-datalab/datalab.git):

1. Clone the repository and enter the directory:
   ```bash
   git clone https://github.com/astro-datalab/datalab.git
   cd datalab
   ```

2. Ensure you have the latest version of pip and setuptools:
   ```bash
   python -m pip install --upgrade pip setuptools
   ```

3. Build the package:
   ```bash
   python -m pip install build
   python -m build
   ```

4. Install the package:
   ```bash
   pip install dist/astro_datalab-<version>-py3-none-any.whl
   ```
   Replace `<version>` with the actual version of the package.

If you want it installed in your private Python repository (because you
maintain multiple Python instances on your machine), you can use the
`--user` flag:
   ```bash
   pip install --user dist/astro_datalab-<version>-py3-none-any.whl
   ```

Finally, if you intend to mount the virtual storage as a local filesystem,
you will need to touch a file in your home directory:
   ```bash
   touch ~/.netrc
   ```

Note: Replace `<version>` in the `pip install` command with the actual version
number of the `astro_datalab` package, such as `2.23.0`.

## Configuration update: If you upgraded from a version prior to v2.20.0

With version v2.20.0, the `datalab` package changed internal service
URLs to point to our new noirlab.edu domain (the old noao.edu domain
expired on Nov 29, 2021). If you had `datalab` installed previously,
your local configuration file will still point to the old domain and
you will see 'connection' errors when executing most commands.

To fix this, simply rename the old configuration file. When you first
run a `datalab` command again, a new and updated configuration file will
be created:

```
mv $HOME/.datalab/dl.conf $HOME/.datalab/dl.conf.bak  # renames old config file
datalab version  # any datalab command will create a new config file
```

Finally, log in again:

```
datalab login
```

and that should be it.

## Documentation

### ``datalab`` command line client

To check the currently installed version of `datalab`:

```
datalab --version

Task Version:  2.20.1
```

To get a list of available datalab commands (tasks):

```
datalab --help

Usage:

    % datalab <task> [task_options]

where <task> is one of:

                 cp - copy a file in Data Lab
             dropdb - Drop a user MyDB table
                get - get a file from Data Lab
             listdb - List the user MyDB tables
                 ln - link a file in Data Lab
              login - Login to the Data Lab
             logout - Logout of the Data Lab
                 ls - list a location in Data Lab
              mkdir - create a directory in Data Lab
                 mv - move a file in Data Lab
          mydb_copy - Rename a user MyDB table
        mydb_create - Create a user MyDB table
          mydb_drop - Drop a user MyDB table
        mydb_import - Import data into a user MyDB table
         mydb_index - Index data in a MyDB table
        mydb_insert - Insert data into a user MyDB table
          mydb_list - List the user MyDB tables
        mydb_rename - Rename a user MyDB table
      mydb_truncate - Truncate a user MyDB table
           profiles - List the available Query Manager profiles
                put - Put a file into Data Lab
           qresults - Get the async query results
            qstatus - Get an async query job status
              query - Query a remote data service in the Data Lab
                 rm - delete a file in Data Lab
              rmdir - delete a directory in Data Lab
             schema - Print data service schema info
           services - Print available data services
             status - Report on the user status
           svc_urls - Print service URLs in use
                tag - tag a file in Data Lab
            version - Print task version
             whoami - Print the current active user
```

You can get summaries of the arguments to a task with the ``help``
option:

```
datalab login help

The 'login' task takes the following parameters:

          user - Username of account in Data Lab [required]
      password - Password for account in Data Lab [required]
         mount - Mountpoint of remote Virtual Storage [optional]
  
       verbose - print verbose level log messages [optional]
         debug - print debug log level messages [optional]
       warning - print warning level log messages [optional]
```

The ``datalab`` command will prompt you for required arguments if you do not
provide them on the command line, e.g.:

```
datalab login

user (default: None): foousername
password (default: None): foouserpassword
Welcome to the Data Lab, foousername
```

Documentation for the ``datalab`` commands can be also found in the
[``docs/``](github.com/astro-datalab/datalab/tree/master/docs)
directory:

### ``dl`` Data Lab Python module

Once the client is installed, some Data Lab Python modules can be
imported and used in your Python programs locally, e.g.

```python
ipython
In [1]: from dl import queryClient as qc
In [2]: result = qc.query(sql='SELECT ra,dec from smash_dr1.object LIMIT 10')
In [3]: print(result)
ra,dec
175.215070742307,-38.4897863179213
175.241595469141,-38.4163769993698
175.25128999751,-38.4393292753547
175.265049366394,-38.424371697545
175.265160854504,-38.4915114547051
175.277267094536,-38.431267581266
175.302055158646,-38.4674421358985
175.328056295831,-38.4350989294865
175.334968899953,-38.4547709884234
175.34222308206,-38.4433633662239
```

A comprehensive [user manual](https://datalab.noirlab.edu/docs/manual/)
explains the many features of Data Lab.

<!---
### To mount virtual storage as a local directory at login

You can mount the virtual storage as a local directory at login by
using the optional <i>mount</i> argument. 

```
datalab login --user=<user> --password=<password> --mount=/tmp/vospace
```

This will attempt to mount the default virtual storage (at NOIRLab). If
you need to mount another one, you should use the ``datalab mount`` option.

### To mount virtual storage as a local directory once logged in

```
datalab mount
vospace (default: vos:):
mount (default: /tmp/vospace):
```

### To unmount virtual storage

You can either use a regular Unix command:

```
umount /tmp/vospace
```

or unmount the space when you log out of Data Lab:

```
datalab logout --unmount=/tmp/vospace
```
-->

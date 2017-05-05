
# DOCUMENTATION

<b>datalab</b> is command-line Python client for the [NOAO DataLab](http://datalab.noao.edu).

It provides easy access to DataLab functionalities:

1. remote storage (VOSpace)
1. (a)synchronous data queries (TAP)
1. job management

Authentication to DataLab is based on a username and password.


## SYSTEM REQUIREMENTS

* A Data Lab account
* Python 2.7 or later
* fuse or OSX-FUSE (if you want to mount the remote storage as a local filesystem)


## INSTALLATION

You can retrieve the [gitlab](http://gitlab.noao.edu/mjg/datalab.git)
distribution and install via:

```
git clone http://gitlab.noao.edu/mjg/datalab.git
cd datalab
python setup.py install
```

If you want it installed in your private Python repository (because you maintain mutiple Python instances on your machine) then do:

```
python setup.py install --user
```

Finally, if you intend to mount the virtual storage as a local
filesystem, you will need to touch a file in your home directory:

```
touch ~/.netrc
```
## TUTORIAL

Documentation for all the datalab commands can be found
[here](http://datalab.noao.edu/twiki/pub/DataLab/SoftwareDocs/DataLab_Command_Line_Client.pdf). Examples
of using the <b>datalab</b> command for working with the virtual storage and
querying can be found in the following Jupyter notebooks:

* [A Jupyter notebook for using the Storage Manager API](http://datalab.noao.edu/twiki/pub/DataLab/SoftwareDocs/How_to_use_the_Data_Lab_storage_manager_service.ipynb)
* [A Jupyer notebook for using the Query Manager API](http://datalab.noao.edu/twiki/pub/DataLab/SoftwareDocs/How_to_use_the_Data_Lab_query_manager_service.ipynb)

The <b>datalab</b> command will prompt you for required arguments if you do not
provide them on the command line, e.g.:

```
datalab login
user (default: None):
password (default: None):
```

You can also always get summaries of the arguments with the
<i>help</i> option:

```
datalab login help
The 'login' task takes the following parameters:
  debug - print debug log level messages [optional]
  verbose - print verbose level log messages [optional]
  warning - print warning level log messages [optional]
  user - username of account in DataLab [required]
  password - password for account in DataLab [required]
  mount - mountpoint of remove VOSpace [optional]
```

### To mount virtual storage as a local directory at login

You can mount the virtual storage as a local directory at login by
using the optional <i>mount</i> argument. 

```
datalab login --user=<user> --password=<password> --mount=/tmp/vospace
```

This will attempt to mount the default virtual storage (at NOAO). If
you need to mount another one, you should use the <b>datalab mount</b> option.

  
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

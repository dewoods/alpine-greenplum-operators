alpine-greenplum-operators
===================================

Set of Greenplum data manipulation plugin operators for [Alpine](http://www.alpinenow.com)

Installation
------------

Generate JAR using included Makefile:
    
    $ make

Copy generated JAR to Alpine plugin directory:

    $ cp GreenplumDataOperators.jar <INSTALL_PATH>/chorus/alpine/ALPINE_DATA_REPOSITORY/plugins/

Restart Alpine server:

    $ chorus_control.sh restart alpine

###Dependencies

- Java >= 1.6
- Alpine installed and running
- Greenplum data source registered in Alpine
- Alpine JAR files available in $CLASSPATH (located at <INSTALL_PATH>/chorus/alpine/apache-tomcat-<VERSION>/lib

Usage
-----

Once JAR is installed, all new operators will be available from the Alpine visual workflow tool.

Limitations
-----------

The following known limitations will be addressed in a future release:
- No column checking or validation, all column names and types in source and target tables must match

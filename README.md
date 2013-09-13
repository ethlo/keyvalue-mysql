MyCached
========

MySQL 5.6 introduced a key/value (memcached) API for faster data storage/reads (avoiding overhead of SQL).

NOTE: Also contains a legacy connector with the same API to work with older versions of MySQL, using regular SQL.

This project utilises the ethlo [Key/Value API (kvapi)](https://github.com/ethlo/kvapi).

Build status
============
[![Build Status](https://travis-ci.org/ethlo/mycached.png?branch=master)](https://travis-ci.org/ethlo/mycached)

Maven
=====

Repository: http://ethlo.com/maven

```
<dependency>
	<groupId>com.ethlo.mycached</groupId>
	<artifactId>mycached</artifactId>
	<version>1.0-SNAPSHOT</version>
</dependency>
```

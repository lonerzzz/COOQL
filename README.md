# COOQL
An object oriented fluent API for performing CRUD operations on a Cassandra database.

The Cassandra Object Oriented Query Language (COOQL) is a Domain Specific Language (DSL) wrapper for the Datastax Cassandra client that provides a way to write Cassandra Query Language (CQL) statements without writing CQL strings. Instead CQL statements are written using java methods.

The primary purpose of COOQL is to avoid runtime CQL errors and data type errors that arise because of the use of text as an intermediary between the database and the application. The API also prevents runtime errors due to operations that would violate CQL rules.

For example, the following statements can cause readily cause errors that are only detected at runtime

resultSet.getInt( "name" );	// Where 'name' is text and not an integer.

## What COOQL is:

COOQL provides a programmatic way of using the the CQL language without encountering runtime errors due to poorly formatted CQL statements or statements that attempt to perform operations that are not permitted or possible for the current table, column or key. The COOQL API generator interrogates the specified keyspace to create an API to the database.

The API represents all of the possible ways of accessing and manipulating the data in the form of a fluent API. To those who are unfamiliar, a fluent API is one where the output of one method links to the next possible operations such that a logical series of operations can be chained together as shown below.

```
int readingCount = (Integer)cooql.SELECT().FROM_sensorReading()._COUNT_STAR()
		.WHERE_subscriberId_AND_unitId_EQ( subscriberId, unitId )
		.AND().time_GTE( startingTime ).AND().time_LTE( endingTime )
		.executeQuery().populateObject( Integer.class );
```

## What COOQL is not:

COOQL is not an Object Relational Mapper (ORM). It is not intended to provide transactional abilities or somehow augment the Cassandra database functionality. It attempts to avoid database boilerplate code differently than an ORM and without hiding the query mechanisms that the CQL language provides.

Use of the API should be generally intuitive. It is most effectively used with a method completion interface such as in eclipse or other modern IDEs.

So here is how you use COOQL: TBD

Given a database table declared as follows:

Generate the API

Compile the code

Once the API is generated, it can be used as follows:

Insert Example


Update Example

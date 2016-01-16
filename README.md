# ProtocolTestData
Test objects generated for round trip testing.
=======================
This example project (in work) will demonstrate how code generation can be used to cleanly produce Jar resources to be used by downstream projects.  The idea is to simplify the build process by breaking the work into multiple Maven projects.  This project takes the Schema XML and in its build process produces a Jar which contains the needed Java classes.

Even more can be done here besides the simple generation of POJOs. Processing stages can be generated to interact with the specific fields of the schema.

Down stream projects need not deal with the code generation because they have simple Jars to use and assemble using a higher level of abstraction.


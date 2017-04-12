# Snappydata Connection Sequence

When first connection is created it goes to the primary locator and invokes the getAllServersWithPreferredServer() API to get all the servers in the system and the current "preferred" server. The "all servers" list can be used later if all locators go away, but for a start I think you can ignore it. If primary locator is unavailable then it tries the alternatives mentioned in "secondary-locators" property.

The connection to the locator is designated as "control connection" which is used subsequently for determining where to connect. The "preferred" server returned in the first call is used for creating the actual "data connection" over which the operations are sent. The "control connection" is used for all subsequent connections. Any new connection will first fire the "getPreferredServer" over the "control connection" to get the server to which to connect and establish the actual "data connection" to that server. The JDBC driver also handles failure in both the "control connection" (using secondary-locators and any remaining servers) and any "data connection" to re-establish them using the same procedure.

If you see the TestThrift.java example then it provides a simplified version of above: https://github.com/SnappyDataInc/snappy-store/blob/snappy/master/gemfirexd/tools/src/test/java/io/snappydata/app/TestThrift.java#L208

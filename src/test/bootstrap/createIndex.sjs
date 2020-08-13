var admin = require("/MarkLogic/admin.xqy");
var temporal = require("/MarkLogic/temporal.xqy");
var defaultTestDbName = "mlcp-unittest-db";
var defaultCopyDbName = "mlcp-unittest-copy-db";
var defaultTestDbPort = 5276;
var defaultCopyDbPort = 6276;

var testDbName = (typeof testDbName === "undefined")?defaultTestDbName:testDbName;
var copyDbName = (typeof copyDbName === "undefined")?defaultCopyDbName:copyDbName;
var testDbPort = (typeof testDbPort === "undefined")?defaultTestDbPort:testDbPort;
var copyDbPort = (typeof copyDbPort === "undefined")?defaultCopyDbPort:copyDbPort;

var config = admin.getConfiguration();
// Create test database if not exist
if (!admin.databaseExists(config, testDbName)) {
    var config = admin.databaseCreate(
        config,
        testDbName,
        xdmp.database("Security"),
        xdmp.database("Schemas")
    );
    config = admin.forestCreate(
        config,
        testDbName + "Forest",
        xdmp.host(),
        ""
    );
    admin.saveConfiguration(config);
    config = admin.getConfiguration();
    config = admin.databaseAttachForest(
        config,
        admin.databaseGetId(config, testDbName),
        admin.forestGetId(config, testDbName + "Forest")
    );
    config = admin.xdbcServerCreate(
      config,
      xdmp.group(),
      testDbName+"AppServer",
      "/",
      testDbPort,
      xdmp.modulesDatabase(),
      admin.databaseGetId(config, testDbName)
    );
    admin.saveConfiguration(config);
}

// Create copy database if not exist
if (!admin.databaseExists(config, copyDbName)) {
	var config = admin.databaseCreate(
        config, 
		copyDbName,
		xdmp.database("Security"),
		xdmp.database("Schemas")
	);
	config = admin.forestCreate(
        config,
		copyDbName + "Forest",
		xdmp.host(),
		""
	);
	admin.saveConfiguration(config);
	config = admin.getConfiguration();
	config = admin.databaseAttachForest(
        config,
		admin.databaseGetId(config, copyDbName),
		admin.forestGetId(config, copyDbName + "Forest")
	)
    config = admin.xdbcServerCreate(
      config,
      xdmp.group(),
      copyDbName+"AppServer",
      "/",
      copyDbPort,
      xdmp.modulesDatabase(),
      admin.databaseGetId(config, copyDbName)
    );
    admin.saveConfiguration(config);
}

// Set Index
config = admin.getConfiguration();
var testDb = xdmp.database(testDbName);
config = admin.databaseSetTripleIndex(config, testDb, true);
config = admin.databaseSetCollectionLexicon(config, testDb, true);

var validStart = admin.databaseRangeElementIndex(
    "dateTime", "http://sample.com/bitemporal", "valid-start", "", fn.false() );
var validEnd   = admin.databaseRangeElementIndex(
    "dateTime", "http://sample.com/bitemporal", "valid-end", "", fn.false() );
var systemStart = admin.databaseRangeElementIndex(
    "dateTime", "http://sample.com/bitemporal", "system-start", "", fn.false() );
var systemEnd   = admin.databaseRangeElementIndex(
    "dateTime", "http://sample.com/bitemporal", "system-end", "", fn.false() );
var hasValidStart = false;
var hasValidEnd = false;
var hasSysStart = false;
var hasSysEnd = false;

// Create range element index if not exist
var indexes = admin.databaseGetRangeElementIndexes(config, testDb);
for (var idx of indexes) {
    if (fn.deepEqual(validStart, idx)) hasValidStart = true;
    if (fn.deepEqual(validEnd, idx)) hasValidEnd = true;
    if (fn.deepEqual(systemStart, idx)) hasSysStart = true;
    if (fn.deepEqual(systemEnd, idx)) hasSysEnd = true;
}

if (!hasValidStart) {
	config = admin.databaseAddRangeElementIndex(config, testDb, validStart);
}

if (!hasValidEnd) {
	config = admin.databaseAddRangeElementIndex(config, testDb, validEnd);
}

if (!hasSysStart) {
	config = admin.databaseAddRangeElementIndex(config, testDb, systemStart);
}

if (!hasSysEnd) {
	config = admin.databaseAddRangeElementIndex(config, testDb, systemEnd);
}
admin.saveConfiguration(config);

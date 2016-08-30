var testDbName = (typeof testDbName === "undefined")?"Documents":testDbName;
var copyDstName = (typeof copyDstName === "undefined")?"copyDst":copyDstName;;
var admin = require("/MarkLogic/admin.xqy");
var temporal = require("/MarkLogic/temporal.xqy");

var config = admin.getConfiguration();
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

warnings = []
try {
	config = admin.databaseAddRangeElementIndex(config, testDb, validStart);
}
catch (e) {
  if (e.name !== "ADMIN-DUPLICATECONFIGITEM") {
    throw e;
  }
  else {
  	warnings.push(e.name + ": " + e.message)
  }
}

try {
	config = admin.databaseAddRangeElementIndex(config, testDb, validEnd);
}
catch (e) {
  if (e.name !== "ADMIN-DUPLICATECONFIGITEM") {
    throw e;
  }
  else {
  	warnings.push(e.name + ": " + e.message)
  }
}

try {
	config = admin.databaseAddRangeElementIndex(config, testDb, systemStart);
}
catch (e) {
  if (e.name !== "ADMIN-DUPLICATECONFIGITEM") {
    throw e;
  }
  else {
  	warnings.push(e.name + ": " + e.message)
  }
}

try {
	config = admin.databaseAddRangeElementIndex(config, testDb, systemEnd);
}
catch (e) {
  if (e.name !== "ADMIN-DUPLICATECONFIGITEM") {
    throw e;
  }
  else {
  	warnings.push(e.name + ": " + e.message)
  }
}

/*

admin.saveConfiguration(config);
config = admin.getConfiguration();

try {
	var validResult = temporal.axisCreate(
	    "valid", 
	    cts.elementReference(fn.QName("http://sample.com/bitemporal", "valid-start")), 
	    cts.elementReference(fn.QName("http://sample.com/bitemporal", "valid-end")));
}
catch (e) {
	if (e.name !== "TEMPORAL-DUPAXIS") {
		throw e;
	}
	else {
		warnings.push(e.name + ": " + e.message);
	}
}

try {
	var systemResult = temporal.axisCreate(
	    "system", 
	    cts.elementReference(fn.QName("http://sample.com/bitemporal", "system-start")), 
	    cts.elementReference(fn.QName("http://sample.com/bitemporal", "system-end")));
}
catch (e) {
	if (e.name !== "TEMPORAL-DUPAXIS") {
		throw e;
	}
	else {
		warnings.push(e.name + ": " + e.message);
	}
}


try {
	var collectionResult = temporal.collectionCreate(
	"mycollection", "system", "valid");
}
catch (e) {
	if (e.name !== "TEMPORAL-DUPCOLLECTION") {
		throw e;
	}
	else {
		warnings.push(e.name + ": " + e.message);
	}
}
*/

if (!admin.databaseExists(config, copyDstName)) {
	config = admin.databaseCreate(config, 
		copyDstName,
		xdmp.database("Security"),
		xdmp.database("Schemas")
	);
	config = admin.forestCreate(config,
		copyDstName + "Forest",
		xdmp.host(),
		""
	);
	admin.saveConfiguration(config);
	config = admin.getConfiguration();
	config = admin.databaseAttachForest(config,
		admin.databaseGetId(config, copyDstName),
		admin.forestGetId(config, copyDstName + "Forest")
	)
}

admin.saveConfiguration(config);
warnings;

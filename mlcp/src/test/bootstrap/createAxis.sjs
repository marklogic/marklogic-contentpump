var admin = require("/MarkLogic/admin.xqy");
var temporal = require("/MarkLogic/temporal.xqy");
warnings = [];

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

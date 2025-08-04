"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.cleanupMongoDBStore = exports.makeMongoDBStore = void 0;
var makeMongoDBStore_1 = require("./makeMongoDBStore");
Object.defineProperty(exports, "makeMongoDBStore", { enumerable: true, get: function () { return makeMongoDBStore_1.makeMongoDBStore; } });
Object.defineProperty(exports, "cleanupMongoDBStore", { enumerable: true, get: function () { return makeMongoDBStore_1.cleanupMongoDBStore; } });
module.exports = {
    makeMongoDBStore: require('./makeMongoDBStore').makeMongoDBStore,
    cleanupMongoDBStore: require('./makeMongoDBStore').cleanupMongoDBStore
};
//# sourceMappingURL=index.js.map
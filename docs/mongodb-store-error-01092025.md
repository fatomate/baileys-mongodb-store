[Direct] ✅ Label association upserted - upserted: 1, modified: 0, type: label_jid
[Event Handler] ✅ Add operation completed for 60168970072@s.whatsapp.net/73
[Direct] ✅ Label association upserted - upserted: 0, modified: 1, type: label_jid
[Event Handler] ✅ Add operation completed for 60189898199@s.whatsapp.net/73
[withConnection] Operation failed after 1 attempts: MongoServerError: E11000 duplicate key error collection: wabotdev.wabot_labelAssociations index: instanceId_1_chatId_1_labelId_1 dup key: { instanceId: "68B56FF52B090", chatId: "60168970072@s.whatsapp.net", labelId: "73" }
    at ReplaceOneOperation.execute (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/update.js:128:19)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async tryOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:207:20)
    at async executeOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:75:16)
    at async Collection.replaceOne (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/collection.js:217:16)
    at async /home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:619:24
    at async retryWithBackoff (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionRetry.js:43:28)
    at async withConnection (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:614:28)
    at async Object.upsertLabelAssociation (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:2945:28)
    at async EventEmitter.<anonymous> (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:3718:29) {
  errorLabelSet: Set(0) {},
  errorResponse: {
    index: 0,
    code: 11000,
    errmsg: 'E11000 duplicate key error collection: wabotdev.wabot_labelAssociations index: instanceId_1_chatId_1_labelId_1 dup key: { instanceId: "68B56FF52B090", chatId: "60168970072@s.whatsapp.net", labelId: "73" }',
    keyPattern: { instanceId: 1, chatId: 1, labelId: 1 },
    keyValue: {
      instanceId: '68B56FF52B090',
      chatId: '60168970072@s.whatsapp.net',
      labelId: '73'
    }
  },
  index: 0,
  code: 11000,
  keyPattern: { instanceId: 1, chatId: 1, labelId: 1 },
  keyValue: {
    instanceId: '68B56FF52B090',
    chatId: '60168970072@s.whatsapp.net',
    labelId: '73'
  }
}
[withConnection] Operation failed: MongoServerError: E11000 duplicate key error collection: wabotdev.wabot_labelAssociations index: instanceId_1_chatId_1_labelId_1 dup key: { instanceId: "68B56FF52B090", chatId: "60168970072@s.whatsapp.net", labelId: "73" }
    at ReplaceOneOperation.execute (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/update.js:128:19)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async tryOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:207:20)
    at async executeOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:75:16)
    at async Collection.replaceOne (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/collection.js:217:16)
    at async /home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:619:24
    at async retryWithBackoff (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionRetry.js:43:28)
    at async withConnection (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:614:28)
    at async Object.upsertLabelAssociation (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:2945:28)
    at async EventEmitter.<anonymous> (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:3718:29) {
  errorLabelSet: Set(0) {},
  errorResponse: {
    index: 0,
    code: 11000,
    errmsg: 'E11000 duplicate key error collection: wabotdev.wabot_labelAssociations index: instanceId_1_chatId_1_labelId_1 dup key: { instanceId: "68B56FF52B090", chatId: "60168970072@s.whatsapp.net", labelId: "73" }',
    keyPattern: { instanceId: 1, chatId: 1, labelId: 1 },
    keyValue: {
      instanceId: '68B56FF52B090',
      chatId: '60168970072@s.whatsapp.net',
      labelId: '73'
    }
  },
  index: 0,
  code: 11000,
  keyPattern: { instanceId: 1, chatId: 1, labelId: 1 },
  keyValue: {
    instanceId: '68B56FF52B090',
    chatId: '60168970072@s.whatsapp.net',
    labelId: '73'
  }
}
[Event Handler] Failed to process add label association for 60168970072@s.whatsapp.net/73: MongoServerError: E11000 duplicate key error collection: wabotdev.wabot_labelAssociations index: instanceId_1_chatId_1_labelId_1 dup key: { instanceId: "68B56FF52B090", chatId: "60168970072@s.whatsapp.net", labelId: "73" }
    at ReplaceOneOperation.execute (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/update.js:128:19)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async tryOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:207:20)
    at async executeOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:75:16)
    at async Collection.replaceOne (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/collection.js:217:16)
    at async /home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:619:24
    at async retryWithBackoff (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionRetry.js:43:28)
    at async withConnection (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:614:28)
    at async Object.upsertLabelAssociation (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:2945:28)
    at async EventEmitter.<anonymous> (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:3718:29) {
  errorLabelSet: Set(0) {},
  errorResponse: {
    index: 0,
    code: 11000,
    errmsg: 'E11000 duplicate key error collection: wabotdev.wabot_labelAssociations index: instanceId_1_chatId_1_labelId_1 dup key: { instanceId: "68B56FF52B090", chatId: "60168970072@s.whatsapp.net", labelId: "73" }',
    keyPattern: { instanceId: 1, chatId: 1, labelId: 1 },
    keyValue: {
      instanceId: '68B56FF52B090',
      chatId: '60168970072@s.whatsapp.net',
      labelId: '73'
    }
  },
  index: 0,
  code: 11000,
  keyPattern: { instanceId: 1, chatId: 1, labelId: 1 },
  keyValue: {
    instanceId: '68B56FF52B090',
    chatId: '60168970072@s.whatsapp.net',
    labelId: '73'
  }
}
[withConnection] Operation failed after 1 attempts: MongoServerError: E11000 duplicate key error collection: wabotdev.wabot_labelAssociations index: instanceId_1_chatId_1_labelId_1 dup key: { instanceId: "68B56FF52B090", chatId: "60134459345@s.whatsapp.net", labelId: "73" }
    at ReplaceOneOperation.execute (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/update.js:128:19)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async tryOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:207:20)
    at async executeOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:75:16)
    at async Collection.replaceOne (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/collection.js:217:16)
    at async /home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:619:24
    at async retryWithBackoff (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionRetry.js:43:28)
    at async withConnection (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:614:28)
    at async Object.upsertLabelAssociation (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:2945:28)
    at async EventEmitter.<anonymous> (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:3718:29) {
  errorLabelSet: Set(0) {},
  errorResponse: {
    index: 0,
    code: 11000,
    errmsg: 'E11000 duplicate key error collection: wabotdev.wabot_labelAssociations index: instanceId_1_chatId_1_labelId_1 dup key: { instanceId: "68B56FF52B090", chatId: "60134459345@s.whatsapp.net", labelId: "73" }',
    keyPattern: { instanceId: 1, chatId: 1, labelId: 1 },
    keyValue: {
      instanceId: '68B56FF52B090',
      chatId: '60134459345@s.whatsapp.net',
      labelId: '73'
    }
  },
  index: 0,
  code: 11000,
  keyPattern: { instanceId: 1, chatId: 1, labelId: 1 },
  keyValue: {
    instanceId: '68B56FF52B090',
    chatId: '60134459345@s.whatsapp.net',
    labelId: '73'
  }
}
[withConnection] Operation failed: MongoServerError: E11000 duplicate key error collection: wabotdev.wabot_labelAssociations index: instanceId_1_chatId_1_labelId_1 dup key: { instanceId: "68B56FF52B090", chatId: "60134459345@s.whatsapp.net", labelId: "73" }
    at ReplaceOneOperation.execute (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/update.js:128:19)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async tryOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:207:20)
    at async executeOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:75:16)
    at async Collection.replaceOne (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/collection.js:217:16)
    at async /home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:619:24
    at async retryWithBackoff (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionRetry.js:43:28)
    at async withConnection (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:614:28)
    at async Object.upsertLabelAssociation (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:2945:28)
    at async EventEmitter.<anonymous> (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:3718:29) {
  errorLabelSet: Set(0) {},
  errorResponse: {
    index: 0,
    code: 11000,
    errmsg: 'E11000 duplicate key error collection: wabotdev.wabot_labelAssociations index: instanceId_1_chatId_1_labelId_1 dup key: { instanceId: "68B56FF52B090", chatId: "60134459345@s.whatsapp.net", labelId: "73" }',
    keyPattern: { instanceId: 1, chatId: 1, labelId: 1 },
    keyValue: {
      instanceId: '68B56FF52B090',
      chatId: '60134459345@s.whatsapp.net',
      labelId: '73'
    }
  },
  index: 0,
  code: 11000,
  keyPattern: { instanceId: 1, chatId: 1, labelId: 1 },
  keyValue: {
    instanceId: '68B56FF52B090',
    chatId: '60134459345@s.whatsapp.net',
    labelId: '73'
  }
}
[Event Handler] Failed to process add label association for 60134459345@s.whatsapp.net/73: MongoServerError: E11000 duplicate key error collection: wabotdev.wabot_labelAssociations index: instanceId_1_chatId_1_labelId_1 dup key: { instanceId: "68B56FF52B090", chatId: "60134459345@s.whatsapp.net", labelId: "73" }
    at ReplaceOneOperation.execute (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/update.js:128:19)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
    at async tryOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:207:20)
    at async executeOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:75:16)
    at async Collection.replaceOne (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/collection.js:217:16)
    at async /home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:619:24
    at async retryWithBackoff (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/connectionRetry.js:43:28)
    at async withConnection (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:614:28)
    at async Object.upsertLabelAssociation (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:2945:28)
    at async EventEmitter.<anonymous> (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:3718:29) {
  errorLabelSet: Set(0) {},
  errorResponse: {
    index: 0,
    code: 11000,
    errmsg: 'E11000 duplicate key error collection: wabotdev.wabot_labelAssociations index: instanceId_1_chatId_1_labelId_1 dup key: { instanceId: "68B56FF52B090", chatId: "60134459345@s.whatsapp.net", labelId: "73" }',
    keyPattern: { instanceId: 1, chatId: 1, labelId: 1 },
    keyValue: {
      instanceId: '68B56FF52B090',
      chatId: '60134459345@s.whatsapp.net',
      labelId: '73'
    }
  },
  index: 0,
  code: 11000,
  keyPattern: { instanceId: 1, chatId: 1, labelId: 1 },
  keyValue: {
    instanceId: '68B56FF52B090',
    chatId: '60134459345@s.whatsapp.net',
    labelId: '73'
  }
}
messages.upsert received for instance 68B56FF52B090: 1 message(s)
[LID] Pattern 3: FromMe=false, only senderLid (waiting for phone discovery)
[LidHandler] Failed to get phone number from LID: MongoNotConnectedError: Client must be connected before running operations
    at autoConnect (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:95:19)
    at executeOperation (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/operations/execute_operation.js:38:40)
    at FindCursor._initialize (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/cursor/find_cursor.js:61:73)
    at FindCursor.cursorInit (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/cursor/abstract_cursor.js:632:38)
    at FindCursor.fetchBatch (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/cursor/abstract_cursor.js:666:24)
    at FindCursor.next (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/cursor/abstract_cursor.js:342:28)
    at Collection.findOne (/home/wabotdev/api-wabot-dev/public_html/node_modules/mongodb/lib/collection.js:277:34)
    at LidHandler.getPhoneNumberFromLid (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/utils/lidHandler.js:187:62)
    at EventEmitter.messagesUpsertHandler (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:3134:74) {
  errorLabelSet: Set(0) {}
}
Connection check failed for instance 68B56FF52B090: MongoNotConnectedError: Client must be connected before running operations
ℹ️ Index lastSeen_1 does not exist in collection wabot_lidMappings, skipping drop
[Event Handler] labels.association received - Type: remove, Association: label_jid, ChatId: 60196953307@s.whatsapp.net, LabelId: 74, MessageId: none
[Event Handler] labels.association received - Type: remove, Association: label_jid, ChatId: 60196953307@s.whatsapp.net, LabelId: 74, MessageId: none
[Event Handler] labels.association received - Type: remove, Association: label_jid, ChatId: 60196953307@s.whatsapp.net, LabelId: 74, MessageId: none
[Event Handler] labels.association received - Type: remove, Association: label_jid, ChatId: 60196953307@s.whatsapp.net, LabelId: 74, MessageId: none
Label association data received for instance id 68B56FF52B090 : {
  "type": "remove",
  "association": {
    "type": "label_jid",
    "chatId": "60196953307@s.whatsapp.net",
    "labelId": "74"
  }
}
[Event Handler] labels.association received - Type: add, Association: label_jid, ChatId: 60196953307@s.whatsapp.net, LabelId: 76, MessageId: none
[Event Handler] labels.association received - Type: add, Association: label_jid, ChatId: 60196953307@s.whatsapp.net, LabelId: 76, MessageId: none
[Event Handler] labels.association received - Type: add, Association: label_jid, ChatId: 60196953307@s.whatsapp.net, LabelId: 76, MessageId: none
[Event Handler] labels.association received - Type: add, Association: label_jid, ChatId: 60196953307@s.whatsapp.net, LabelId: 76, MessageId: none
Label association data received for instance id 68B56FF52B090 : {
  "type": "add",
  "association": {
    "type": "label_jid",
    "chatId": "60196953307@s.whatsapp.net",
    "labelId": "76"
  }
}
[Event Handler] labels.association received - Type: add, Association: label_jid, ChatId: 60134459345@s.whatsapp.net, LabelId: 73, MessageId: none
[Event Handler] labels.association received - Type: add, Association: label_jid, ChatId: 60134459345@s.whatsapp.net, LabelId: 73, MessageId: none
[Event Handler] labels.association received - Type: add, Association: label_jid, ChatId: 60134459345@s.whatsapp.net, LabelId: 73, MessageId: none
[Event Handler] labels.association received - Type: add, Association: label_jid, ChatId: 60134459345@s.whatsapp.net, LabelId: 73, MessageId: none
Label association data received for instance id 68B56FF52B090 : {
  "type": "add",
  "association": {
    "type": "label_jid",
    "chatId": "60134459345@s.whatsapp.net",
    "labelId": "73"
  }
}
[Event Handler] Processing remove operation for 60196953307@s.whatsapp.net/74
[Event Handler] Processing remove operation for 60196953307@s.whatsapp.net/74
[Event Handler] Processing remove operation for 60196953307@s.whatsapp.net/74
[Event Handler] Processing remove operation for 60196953307@s.whatsapp.net/74
[Event Handler] Processing add operation for 60196953307@s.whatsapp.net/76
[Event Handler] Processing add operation for 60196953307@s.whatsapp.net/76
[Event Handler] Processing add operation for 60196953307@s.whatsapp.net/76
[Event Handler] Processing add operation for 60196953307@s.whatsapp.net/76
[Event Handler] Processing add operation for 60134459345@s.whatsapp.net/73
[Event Handler] Processing add operation for 60134459345@s.whatsapp.net/73
[Event Handler] Processing add operation for 60134459345@s.whatsapp.net/73
[Event Handler] Processing add operation for 60134459345@s.whatsapp.net/73
[Event Handler] Failed to process remove label association for 60196953307@s.whatsapp.net/74: Error: Store is disconnected, operation cancelled
    at withConnection (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:591:19)
    at Object.deleteLabelAssociation (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:3018:34)
    at EventEmitter.<anonymous> (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:3722:45)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
[Event Handler] Failed to process remove label association for 60196953307@s.whatsapp.net/74: Error: Store is disconnected, operation cancelled
    at withConnection (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:591:19)
    at Object.deleteLabelAssociation (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:3018:34)
    at EventEmitter.<anonymous> (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:3722:45)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
[Event Handler] Failed to process add label association for 60196953307@s.whatsapp.net/76: Error: Store is disconnected, operation cancelled
    at withConnection (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:591:19)
    at Object.upsertLabelAssociation (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:2945:34)
    at EventEmitter.<anonymous> (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:3718:45)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
[Event Handler] Failed to process add label association for 60196953307@s.whatsapp.net/76: Error: Store is disconnected, operation cancelled
    at withConnection (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:591:19)
    at Object.upsertLabelAssociation (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:2945:34)
    at EventEmitter.<anonymous> (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:3718:45)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
[Event Handler] Failed to process add label association for 60134459345@s.whatsapp.net/73: Error: Store is disconnected, operation cancelled
    at withConnection (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:591:19)
    at Object.upsertLabelAssociation (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:2945:34)
    at EventEmitter.<anonymous> (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:3718:45)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
[Event Handler] Failed to process add label association for 60134459345@s.whatsapp.net/73: Error: Store is disconnected, operation cancelled
    at withConnection (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:591:19)
    at Object.upsertLabelAssociation (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:2945:34)
    at EventEmitter.<anonymous> (/home/wabotdev/api-wabot-dev/public_html/node_modules/@baileys/mongodb-store/dist/makeEnhancedMongoDBStore.js:3718:45)
    at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
permissions for module whatsapp_label_sync is defined for 68B56FF52B090 (from cache)
permissions for module whatsapp_label_sync is defined for 68B56FF52B090 (from cache)
permissions for module whatsapp_label_sync is defined for 68B56FF52B090 (from cache)
Queued remove operation for chat 60196953307@s.whatsapp.net, label 74
Queued add operation for chat 60196953307@s.whatsapp.net, label 76
Queued add operation for chat 60134459345@s.whatsapp.net, label 73
Job 5194 in labelAssociation-app is now active
Successfully processed remove automation for chat 60196953307@s.whatsapp.net, label 74
Job 5195 in labelAssociation-app is now active
Job 5196 in labelAssociation-app is now active
Successfully processed add automation for chat 60196953307@s.whatsapp.net, label 76
Successfully processed add automation for chat 60134459345@s.whatsapp.net, label 73
Job 5194 in labelAssociation-app completed successfully
Job 5195 in labelAssociation-app completed successfully
Job 5196 in labelAssociation-app completed successfully
[Direct] ✅ Label association deleted - type: label_jid
[Event Handler] ✅ Remove operation completed for 60196953307@s.whatsapp.net/74
[Direct] Warning: No label association found to delete - chatId: 60196953307@s.whatsapp.net, labelId: 74, type: label_jid
[Event Handler] ✅ Remove operation completed for 60196953307@s.whatsapp.net/74
[Direct] ✅ Label association upserted - upserted: 1, modified: 0, type: label_jid
[Event Handler] ✅ Add operation completed for 60196953307@s.whatsapp.net/76
[Direct] ✅ Label association upserted - upserted: 0, modified: 1, type: label_jid
[Event Handler] ✅ Add operation completed for 60196953307@s.whatsapp.net/76
[Direct] ✅ Label association upserted - upserted: 0, modified: 1, type: label_jid
[Event Handler] ✅ Add operation completed for 60134459345@s.whatsapp.net/73
[Direct] ✅ Label association upserted - upserted: 0, modified: 1, type: label_jid
[Event Handler] ✅ Add operation completed for 60134459345@s.whatsapp.net/73
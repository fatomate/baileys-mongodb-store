// Test double for the ESM-only `baileys` package, which ts-jest (CommonJS) cannot load.
// WebMessageInfo is a real protobufjs message with a uint64 messageTimestamp, so its
// toJSON() uses protobufjs toJSONOptions ({ longs: String }) exactly like WAProto.
import * as protobuf from 'protobufjs'

const root = protobuf.Root.fromJSON({
    nested: {
        MessageKey: { fields: { remoteJid: { type: 'string', id: 1 }, fromMe: { type: 'bool', id: 2 }, id: { type: 'string', id: 3 } } },
        Message: { fields: { conversation: { type: 'string', id: 1 } } },
        WebMessageInfo: {
            fields: {
                key: { type: 'MessageKey', id: 1 },
                message: { type: 'Message', id: 2 },
                messageTimestamp: { type: 'uint64', id: 3 },
            },
        },
    },
})

export const WebMessageInfo = root.lookupType('WebMessageInfo')

export const baileysMock = {
    proto: {
        WebMessageInfo,
        Message: {
            ProtocolMessage: {
                Type: {
                    REVOKE: 0,
                    MESSAGE_EDIT: 14,
                    HISTORY_SYNC_NOTIFICATION: 5,
                    APP_STATE_SYNC_KEY_SHARE: 6,
                    APP_STATE_SYNC_KEY_REQUEST: 7,
                    INITIAL_SECURITY_NOTIFICATION_SETTING_SYNC: 9,
                },
            },
        },
    },
    getAggregateVotesInPollMessage: () => [],
    updateMessageWithReceipt: () => undefined,
    updateMessageWithReaction: () => undefined,
    downloadContentFromMessage: async () => { throw new Error('not used in these tests') },
}

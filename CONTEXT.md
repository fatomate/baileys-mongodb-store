# Baileys MongoDB Store

WhatsApp conversation and identity vocabulary used by the Baileys store. These terms describe connection-side records, not Wabot's team address book or live chat interface.

## Language

**Instance**:
The durable identity of one account — what chatbots, automations, and live chat are scoped to. One account has exactly one instance, and it still exists while the account is disconnected; whether the account is currently connected is its connection state.
_Avoid_: Worker, process, session, environment, device

**Chat**:
A WhatsApp conversation on an instance, with an individual or a group. It is not the Wabot live chat interface.
_Avoid_: Live chat, subscriber, inbox

**Message**:
One sent or received WhatsApp item within a chat.
_Avoid_: Template, event, conversation

**Baileys contact**:
WhatsApp identity information held for a person on an instance. It is not a contact in Wabot's team address book.
_Avoid_: Wabot Contact, subscriber, customer

**JID**:
A WhatsApp address identifying a person, group, or other messaging destination. A JID is not necessarily based on a phone number.
_Avoid_: Phone number, user id, instance id

**LID**:
An opaque WhatsApp identity used instead of a phone-number address. It is not a phone number, a BSUID, or the identity of a linked device.
_Avoid_: Phone JID, BSUID, linked device

**Phone JID**:
A WhatsApp address based on a phone number, distinct from an LID.
_Avoid_: Bare phone number, LID, account

**LID mapping**:
The association between an LID and its corresponding phone-number identity within an instance.
_Avoid_: Nickname, Wabot Alias, subscriber identity

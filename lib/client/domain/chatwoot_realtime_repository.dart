import 'dart:async';

import 'package:chatwoot_sdk/client/data/api/chatwoot_client_api.dart';
import 'package:chatwoot_sdk/client/data/api/chatwoot_api_exception.dart';
import 'package:chatwoot_sdk/client/data/api/dto/chatwoot_contact_session_dto.dart';
import 'package:chatwoot_sdk/client/data/api/dto/chatwoot_conversation_dto.dart';
import 'package:chatwoot_sdk/client/data/api/dto/chatwoot_message_dto.dart';
import 'package:chatwoot_sdk/client/data/api/dto/chatwoot_message_sender_dto.dart';
import 'package:chatwoot_sdk/client/data/realtime_client/chatwoot_socket.dart';
import 'package:chatwoot_sdk/client/data/session_storage/session_storage.dart';
import 'package:chatwoot_sdk/client/data/session_storage/stored_chatwoot_session.dart';
import 'package:chatwoot_sdk/client/domain/data/chatwoot_cable.dart';
import 'package:chatwoot_sdk/client/domain/data/chatwoot_repository.dart';
import 'package:chatwoot_sdk/client/domain/model/chatwoot_connection_state.dart';
import 'package:chatwoot_sdk/client/domain/model/conversation/chatwoot_conversation.dart';
import 'package:chatwoot_sdk/client/domain/model/conversation/chatwoot_conversation_exception.dart';
import 'package:chatwoot_sdk/client/domain/model/message/attachment.dart';
import 'package:chatwoot_sdk/client/domain/model/message/chatwoot_message.dart';
import 'package:chatwoot_sdk/client/domain/model/message/message_sender.dart';
import 'package:chatwoot_sdk/client/domain/model/session/authorization_creds.dart';
import 'package:chatwoot_sdk/client/domain/model/session/chatwoot_contact.dart';
import 'package:chatwoot_sdk/client/domain/model/session/chatwoot_session.dart';
import 'package:chatwoot_sdk/client/domain/model/session/chatwoot_session_exception.dart';
import 'package:cross_file/cross_file.dart';
import 'package:uuid/uuid.dart';

/// Single data-layer implementation of [ChatwootRepository] and [ChatwootCable].
class ChatwootRealtimeRepository implements ChatwootRepository, ChatwootCable {
  ChatwootRealtimeRepository({
    required ChatwootSocket socket,
    required ChatwootClientApi api,
    required SessionStorage sessionStorage,
  }) : _socket = socket,
       _api = api,
       _sessionStorage = sessionStorage {
    _localEvents.stream.listen(
      _eventsBroadcast.add,
      onError: _eventsBroadcast.addError,
    );
    _socket.events.asyncMap(_mapSocketEvent).listen(
      (e) {
        if (e != null) {
          _eventsBroadcast.add(e);
        }
      },
      onError: _eventsBroadcast.addError,
    );
  }

  /// Server assigns positive ids; −1 marks optimistic rows until REST/socket supply a real id (merge on [echoId]).
  static const int _optimisticOutgoingMessageId = -1;

  static final Uuid _uuid = const Uuid();

  final ChatwootSocket _socket;
  final ChatwootClientApi _api;
  final SessionStorage _sessionStorage;

  final StreamController<ChatwootCableEvent> _localEvents = StreamController<ChatwootCableEvent>.broadcast(sync: true);

  final StreamController<ChatwootCableEvent> _eventsBroadcast = StreamController<ChatwootCableEvent>.broadcast(
    sync: true,
  );

  @override
  Stream<ChatwootConnectionState> get connectionState => _socket.connectionState;

  @override
  Stream<ChatwootCableEvent> get events => _eventsBroadcast.stream;

  @override
  Future<void> connect({
    required String sourceId,
    required String pubsubToken,
  }) async {
    await _socket.connect(
      sourceId: sourceId,
      pubsubToken: pubsubToken,
    );
  }

  @override
  Future<void> disconnect() => _socket.disconnect();

  @override
  Future<ChatwootSession> authorize(AuthorizationCreds creds) async {
    final dto = await _api.createContactSession(
      identifier: creds.identifier,
      identifierHash: creds.identifierHash,
      name: creds.name,
      email: creds.email,
      phoneNumber: creds.phoneNumber,
      customAttributes: Map<String, Object?>.from(creds.customAttributes),
    );
    await _sessionStorage.save(
      StoredChatwootSession(sourceId: dto.sourceId, identifier: creds.identifier),
    );
    return dto.toDomainSession(identifier: creds.identifier);
  }

  @override
  Future<ChatwootContact> updateContact({
    required String sourceId,
    String? identifier,
    String? name,
    String? email,
    String? phoneNumber,
    Map<String, Object?> customAttributes = const {},
  }) async {
    final result = await _api.updateContact(
      sourceId,
      identifier: identifier,
      name: name,
      email: email,
      phoneNumber: phoneNumber,
      customAttributes: customAttributes,
    );

    return ChatwootContact(
      id: result.id,
      identifier: identifier,
      name: name,
      email: email,
      phoneNumber: phoneNumber,
    );
  }

  @override
  Future<ChatwootSession?> currentSession() async {
    final stored = await _sessionStorage.read();
    if (stored == null) {
      return null;
    }
    try {
      final dto = await _api.getContactSession(stored.sourceId);
      return dto.toDomainSession(identifier: stored.identifier);
    } on ChatwootApiException catch (e) {
      if (e.statusCode == 404) {
        throw ChatwootSessionException$ContactNotFound(
          contactId: stored.sourceId,
          identifier: stored.identifier,
          cause: e,
        );
      }
      rethrow;
    }
  }

  @override
  Future<List<ChatwootConversation>> fetchConversations({
    required String sourceId,
  }) async {
    final items = await _api.listConversations(sourceId);

    return items.map((e) => e.toDomainConversation()).toList();
  }

  @override
  Future<ChatwootConversation> createConversation({
    required String sourceId,
    Map<String, Object?> customAttributes = const {},
  }) async {
    final dto = await _api.createConversation(
      sourceId,
      customAttributes: customAttributes,
    );

    return dto.toDomainConversation();
  }

  @override
  Future<ChatwootConversation> resolveConversation({
    required String sourceId,
    required ChatwootConversationId conversationId,
  }) => _withConversation404(
    conversationId: conversationId,
    action: () async {
      await _api.toggleConversationResolved(sourceId, conversationId.value);
      final dto = await _api.getConversation(sourceId, conversationId.value);
      return dto.toDomainConversation();
    },
  );

  @override
  Future<void> sendMessage({
    required String sourceId,
    required ChatwootConversationId conversationId,
    String? content,
    List<XFile> attachments = const [],
  }) async {
    final echoId = _uuid.v4();
    final fileAttachments = await Future.wait(attachments.map(Attachment$File.fromXFile));

    final optimistic = ChatwootMessage$Content$Outgoing(
      status: OutgoingMessageStatus.sending,
      id: _optimisticOutgoingMessageId,
      echoId: echoId,
      sentAt: DateTime.now(),
      content: content,
      attachments: fileAttachments,
      isDeleted: false,
    );

    _localEvents.add(
      ChatwootCableEvent$Message$Created(
        conversationId: conversationId,
        message: optimistic,
      ),
    );

    try {
      await _withConversation404(
        conversationId: conversationId,
        action: () => _api.createMessage(
          sourceId,
          conversationId.value,
          content: content,
          echoId: echoId,
          attachments: attachments,
        ),
      );
    } on ChatwootConversationException$NotFound {
      rethrow;
    } on Object {
      _localEvents.add(
        ChatwootCableEvent$Message$Updated(
          conversationId: conversationId,
          message: optimistic.copyWith(
            status: OutgoingMessageStatus.failed,
          ),
        ),
      );

      rethrow;
    }
  }

  @override
  Future<void> retryMessage({
    required String sourceId,
    required ChatwootConversationId conversationId,
    required ChatwootMessage$Content$Outgoing message,
  }) async {
    switch (message.status) {
      case OutgoingMessageStatus.delivered:
      case OutgoingMessageStatus.sending:
        return;
      case OutgoingMessageStatus.failed:
    }

    final sending = message.copyWith(
      status: OutgoingMessageStatus.sending,
      sentAt: DateTime.now(),
    );
    _localEvents.add(
      ChatwootCableEvent$Message$Updated(
        conversationId: conversationId,
        message: sending,
      ),
    );

    try {
      final files = message.attachments.whereType<Attachment$File>().map((a) => a.file).toList();
      await _withConversation404(
        conversationId: conversationId,
        action: () => _api.createMessage(
          sourceId,
          conversationId.value,
          content: message.content,
          echoId: message.echoId,
          attachments: files,
        ),
      );
    } on ChatwootConversationException$NotFound {
      rethrow;
    } on Object {
      _localEvents.add(
        ChatwootCableEvent$Message$Updated(
          conversationId: conversationId,
          message: sending.copyWith(
            status: OutgoingMessageStatus.failed,
          ),
        ),
      );

      rethrow;
    }
  }

  @override
  Future<void> markPresence({
    required String sourceId,
  }) async {
    await _socket.markPresence();
  }

  @override
  Future<void> markConversationRead({
    required String sourceId,
    required ChatwootConversationId conversationId,
  }) => _withConversation404(
    conversationId: conversationId,
    action: () => _api.updateConversationLastSeen(sourceId, conversationId.value),
  );

  @override
  Future<void> toggleTyping({
    required String sourceId,
    required ChatwootConversationId conversationId,
    required bool isTyping,
  }) => _withConversation404(
    conversationId: conversationId,
    action: () => _api.toggleConversationTyping(
      sourceId,
      conversationId.value,
      isTyping: isTyping,
    ),
  );

  Future<T> _withConversation404<T>({
    required ChatwootConversationId conversationId,
    required Future<T> Function() action,
  }) async {
    try {
      return await action();
    } on ChatwootApiException catch (error) {
      if (error.statusCode == 404) {
        throw ChatwootConversationException$NotFound(
          conversationId: conversationId,
          cause: error,
        );
      }
      rethrow;
    }
  }

  Future<ChatwootCableEvent?> _mapSocketEvent(ChatwootSocketEvent event) async {
    switch (event) {
      case ChatwootSocketEvent$Message$Created(:final message):
        final domainMessage = message.toDomainMessage();
        switch (domainMessage) {
          case ChatwootMessage$Activity():
          case ChatwootMessage$Content$Incoming():
            return ChatwootCableEvent$Message$Created(
              conversationId: ChatwootConversationId(message.conversationId),
              message: domainMessage,
            );
          case ChatwootMessage$Content$Outgoing():
            return ChatwootCableEvent$Message$Updated(
              conversationId: ChatwootConversationId(message.conversationId),
              message: domainMessage,
            );
        }
      case ChatwootSocketEvent$Message$Updated(:final message):
        return ChatwootCableEvent$Message$Updated(
          conversationId: ChatwootConversationId(message.conversationId),
          message: message.toDomainMessage(),
        );
      case ChatwootSocketEvent$Conversation$StatusChanged(:final conversation):
        return ChatwootCableEvent$ConversationStatusChanged(
          conversation: conversation.toDomainConversation(),
        );
      case ChatwootSocketEvent$Conversation$TypingOn(:final conversation):
        return ChatwootCableEvent$Typing(
          conversationId: ChatwootConversationId(conversation.id),
          isTyping: true,
        );
      case ChatwootSocketEvent$Conversation$TypingOff(:final conversation):
        return ChatwootCableEvent$Typing(
          conversationId: ChatwootConversationId(conversation.id),
          isTyping: false,
        );
    }
  }
}

extension ChatwootContactSessionDtoDomain on ChatwootContactSessionDto {
  ChatwootSession toDomainSession({required String? identifier}) {
    return ChatwootSession(
      id: ContactInboxId(sourceId),
      token: pubsubToken,
      contact: ChatwootContact(
        id: id,
        identifier: identifier,
        name: name,
        email: email,
        phoneNumber: phoneNumber,
      ),
    );
  }
}

extension ChatwootMessageDtoDomain on ChatwootMessageDto {
  ChatwootMessage toDomainMessage({
    OutgoingMessageStatus? outgoingStatus,
  }) {
    final deleted = contentAttributes['deleted'] == true;
    final sentAt = DateTime.fromMillisecondsSinceEpoch(createdAt * 1000, isUtc: true).toLocal();

    switch (messageType) {
      case 0:
        return ChatwootMessage$Content$Outgoing(
          status: outgoingStatus ?? OutgoingMessageStatus.delivered,
          id: id,
          echoId: echoId,
          sentAt: sentAt,
          content: content,
          attachments: attachments.map((a) => a.toDomainLink()).toList(),
          isDeleted: deleted,
        );
      case 1:
        return ChatwootMessage$Content$Incoming(
          id: id,
          echoId: echoId,
          sentAt: sentAt,
          sender: sender?.toDomainSender(),
          isDeleted: deleted,
          content: content,
          attachments: attachments.map((a) => a.toDomainLink()).toList(),
        );
      case 3:
        return ChatwootMessage$Activity(
          id: id,
          echoId: echoId,
          sentAt: sentAt,
          content: content,
          attachments: attachments.map((a) => a.toDomainLink()).toList(),
        );
      default:
        return ChatwootMessage$Activity(
          id: id,
          echoId: echoId,
          sentAt: sentAt,
          content: content,
          attachments: attachments.map((a) => a.toDomainLink()).toList(),
        );
    }
  }
}

extension on ChatwootPublicMessageSenderDto {
  ChatwootMessageSender toDomainSender() {
    return ChatwootMessageSender(
      id: id,
      name: name.isEmpty ? null : name,
      avatarUrl: Uri.tryParse(avatarUrl ?? ''),
      thumbnail: Uri.tryParse(thumbnail ?? ''),
    );
  }
}

extension ChatwootConversationDtoDomain on ChatwootConversationDto {
  ChatwootConversation toDomainConversation() {
    final domainMessages = messages.map((m) => m.toDomainMessage()).toList();

    return ChatwootConversation(
      id: ChatwootConversationId(id),
      status: ChatwootConversationStatus.fromString(status),
      messages: domainMessages,
      supportTyping: false,
      lastReadTime: DateTime.fromMillisecondsSinceEpoch(contactLastSeenAt * 1000, isUtc: true).toLocal(),
    );
  }
}

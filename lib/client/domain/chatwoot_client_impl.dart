import 'dart:async';

import 'package:chatwoot_sdk/client/data/api/http_chatwoot_client_api.dart';
import 'package:chatwoot_sdk/client/data/realtime_client/chatwoot_socket_impl.dart';
import 'package:chatwoot_sdk/client/data/realtime_client/chatwoot_socket_retry_policy.dart';
import 'package:chatwoot_sdk/client/data/session_storage/session_storage.dart';
import 'package:chatwoot_sdk/client/domain/chatwoot_client.dart';
import 'package:chatwoot_sdk/client/domain/chatwoot_realtime_repository.dart';
import 'package:chatwoot_sdk/client/domain/data/chatwoot_cable.dart';
import 'package:chatwoot_sdk/client/domain/data/chatwoot_repository.dart';
import 'package:chatwoot_sdk/client/domain/logger/chatwoot_logger.dart';
import 'package:chatwoot_sdk/client/domain/model/chatwoot_connection_state.dart';
import 'package:chatwoot_sdk/client/domain/model/chatwoot_state.dart';
import 'package:chatwoot_sdk/client/domain/model/conversation/chatwoot_conversation.dart';
import 'package:chatwoot_sdk/client/domain/model/conversation/chatwoot_conversation_exception.dart';
import 'package:chatwoot_sdk/client/domain/model/message/chatwoot_message.dart';
import 'package:chatwoot_sdk/client/domain/model/session/authorization_creds.dart';
import 'package:chatwoot_sdk/client/domain/model/session/chatwoot_contact.dart';
import 'package:chatwoot_sdk/client/domain/model/session/chatwoot_session.dart';
import 'package:cross_file/cross_file.dart';
import 'package:rxdart/rxdart.dart';

/// Stateful facade over [`ChatwootRepository`] + [`ChatwootCable`].
class ChatwootClientImpl implements ChatwootClient {
  /// Wires HTTP API, socket and [`ChatwootRealtimeRepository`] from inbox settings.
  factory ChatwootClientImpl.withHttpSocket({
    required Uri baseUrl,
    required String inboxIdentifier,
    required SessionStorage sessionStorage,
    ChatwootSocketRetryPolicy? retryPolicy,
    required AuthorizationCreds defaultCreds,
    ChatwootLogger? logger,
    Duration confirmSubscriptionGracePeriod = ChatwootSocketImpl.defaultConfirmSubscriptionGracePeriod,
    Duration confirmSubscriptionTimeout = ChatwootSocketImpl.defaultConfirmSubscriptionTimeout,
  }) {
    final api = HttpChatwootClientApi(
      baseUrl: baseUrl,
      inboxIdentifier: inboxIdentifier,
    );
    final socket = ChatwootSocketImpl(
      baseUrl: baseUrl,
      retryPolicy: retryPolicy,
      logger: logger,
      confirmSubscriptionGracePeriod: confirmSubscriptionGracePeriod,
      confirmSubscriptionTimeout: confirmSubscriptionTimeout,
    );
    final gateway = ChatwootRealtimeRepository(
      socket: socket,
      api: api,
      sessionStorage: sessionStorage,
    );
    return ChatwootClientImpl(
      repository: gateway,
      cable: gateway,
      defaultCreds: defaultCreds,
      logger: logger,
    );
  }

  ChatwootClientImpl({
    required ChatwootRepository repository,
    required ChatwootCable cable,
    required AuthorizationCreds defaultCreds,
    ChatwootLogger? logger,
  }) : _repository = repository,
       _cable = cable,
       _defaultCreds = defaultCreds,
       _logger = logger;

  final ChatwootRepository _repository;
  final ChatwootCable _cable;

  final AuthorizationCreds _defaultCreds;
  final ChatwootLogger? _logger;

  ChatwootSession? _session;

  bool _hasBootstrapped = false;
  bool _disposed = false;

  StreamSubscription<ChatwootCableEvent>? _cableSub;
  StreamSubscription<ChatwootConnectionState>? _connectionSub;

  Timer? _presenceTimer;
  String? _conversationsRefreshSourceId;
  Future<List<ChatwootConversation>>? _conversationsRefreshFuture;
  final Set<int> _emittedConversationDeletedIds = <int>{};

  final BehaviorSubject<ChatwootState> _stateSubject = BehaviorSubject<ChatwootState>.seeded(
    const ChatwootState$ConversationsLoaded(conversations: []),
  );

  @override
  ChatwootContact get contact => _requireSession.contact;

  @override
  ChatwootState get state => _stateSubject.value;

  @override
  Stream<ChatwootState> get statesStream => _stateSubject.stream;

  @override
  Stream<ChatwootConnectionState> get connectionState => _cable.connectionState;

  ChatwootSession get _requireSession {
    if (_disposed) {
      throw StateError('ChatwootClient.dispose() was called.');
    }
    if (!_hasBootstrapped) {
      throw StateError(
        'ChatwootClient.bootstrap() must be called before using ChatwootClient methods.',
      );
    }
    final s = _session;
    if (s == null) {
      throw StateError('ChatwootClient has no active session.');
    }
    return s;
  }

  @override
  Future<void> bootstrap() async {
    if (_disposed) {
      throw StateError('ChatwootClient.dispose() was called.');
    }
    await _stopCableAndPresence();
    await _cable.disconnect();

    ChatwootSession? session = await _repository.currentSession();
    session ??= await _repository.authorize(_defaultCreds);

    await _attachSession(session);
    _hasBootstrapped = true;
  }

  void _startPresence() {
    _presenceTimer?.cancel();
    final sid = _session?.id.value;
    if (sid == null) {
      return;
    }
    unawaited(
      _repository.markPresence(sourceId: sid).catchError((Object _) {}),
    );
    _presenceTimer = Timer.periodic(const Duration(seconds: 30), (_) {
      if (_disposed) {
        return;
      }
      if (_session case final session?) {
        unawaited(_repository.markPresence(sourceId: session.id).catchError((Object _) {}));
      }
    });
  }

  @override
  Future<void> authorize(AuthorizationCreds creds) async {
    _requireSession;
    await _stopCableAndPresence();
    await _cable.disconnect();
    final session = await _repository.authorize(creds);
    await _attachSession(session);
  }

  Future<void> _attachSession(ChatwootSession session) async {
    _resetConversationsRefresh();
    _emittedConversationDeletedIds.clear();
    _session = session;
    await _cable.connect(
      sourceId: session.id.value,
      pubsubToken: session.token,
    );
    final list = await _repository.fetchConversations(sourceId: session.id.value);
    _stateSubject.add(ChatwootState$ConversationsLoaded(conversations: list));
    await _cableSub?.cancel();
    _cableSub = _cable.events.listen(
      _onCableEvent,
      onError: (error, stackTrace) {
        _logger?.warning(
          'Chatwoot realtime events stream error.',
          error: error,
          stackTrace: stackTrace,
        );
      },
    );
    await _connectionSub?.cancel();
    _connectionSub = _cable.connectionState.listen(_onConnectionState);
    _startPresence();
  }

  void _onConnectionState(ChatwootConnectionState state) {
    switch (state) {
      case ChatwootConnectionState$Connected(isReconnected: true):
        unawaited(refreshConversations());
      case ChatwootConnectionState$Connected():
      case ChatwootConnectionState$Disconnected():
      case ChatwootConnectionState$Reconnecting():
        return;
    }
  }

  void _onCableEvent(ChatwootCableEvent event) async {
    try {
      switch (event) {
        case ChatwootCableEvent$Message$Created(:final conversationId, :final message):
          final list = List<ChatwootConversation>.from(_stateSubject.value.conversations);

          final (updatedList, updated) = list.updateConversation(
            conversationId,
            updater: (c) {
              return c.copyWith(messages: [...c.messages, message]);
            },
          );

          _stateSubject.add(
            ChatwootState$Message$New(
              conversations: updatedList,
              conversationId: conversationId,
              message: message,
            ),
          );
        case ChatwootCableEvent$Message$Updated(:final conversationId, :final message):
          final list = List<ChatwootConversation>.from(_stateSubject.value.conversations);

          switch (message) {
            case ChatwootMessage$Activity():
            case ChatwootMessage$Content$Incoming():
              final result = list.updateMessageById(
                conversationId,
                message.id,
                updater: (_) => message,
              );

              _stateSubject.add(
                ChatwootState$Message$Updated(
                  conversations: result.conversations,
                  conversationId: conversationId,
                  message: message,
                  messageIndex: result.messageIndex,
                ),
              );
            case ChatwootMessage$Content$Outgoing():
              if (message.echoId case final echoId?) {
                final result = list.updateMessageByEchoId(
                  conversationId,
                  echoId,
                  updater: (_) => message,
                );

                _stateSubject.add(
                  ChatwootState$Message$Updated(
                    conversations: result.conversations,
                    conversationId: conversationId,
                    message: message,
                    messageIndex: result.messageIndex,
                  ),
                );
              } else {
                final result = list.updateMessageById(
                  conversationId,
                  message.id,
                  updater: (_) => message,
                );

                _stateSubject.add(
                  ChatwootState$Message$Updated(
                    conversations: result.conversations,
                    conversationId: conversationId,
                    message: message,
                    messageIndex: result.messageIndex,
                  ),
                );
              }
          }
        case ChatwootCableEvent$ConversationStatusChanged(:final conversation):
          final list = List<ChatwootConversation>.from(_stateSubject.value.conversations);

          final (updatedList, updated) = list.updateConversation(
            conversation.id,
            updater: (c) {
              return c.copyWith(
                status: conversation.status,
              );
            },
          );

          _stateSubject.add(
            ChatwootState$Conversation$Updated(
              conversations: updatedList,
              conversation: updated,
            ),
          );
        case final ChatwootCableEvent$Typing typingEvent:
          final list = List<ChatwootConversation>.from(_stateSubject.value.conversations);

          final (updatedList, updated) = list.updateConversation(
            typingEvent.conversationId,
            updater: (c) {
              return c.copyWith(supportTyping: typingEvent.isTyping);
            },
          );

          _stateSubject.add(
            ChatwootState$Conversation$Updated(
              conversations: updatedList,
              conversation: updated,
            ),
          );
      }
    } on StateError {
      await refreshConversations();
    }
  }

  @override
  Future<void> logout() async {
    _requireSession;
    await _stopCableAndPresence();
    await _cable.disconnect();
    final session = await _repository.authorize(_defaultCreds);
    await _attachSession(session);
  }

  Future<void> _stopCableAndPresence() async {
    _presenceTimer?.cancel();
    _presenceTimer = null;
    await _cableSub?.cancel();
    _cableSub = null;
    await _connectionSub?.cancel();
    _connectionSub = null;
  }

  @override
  Future<void> refreshConversations() async {
    final id = _requireSession.id.value;
    final list = await _fetchConversations(sourceId: id);
    _stateSubject.add(ChatwootState$ConversationsLoaded(conversations: list));
  }

  @override
  Future<void> updateContact({
    String? name,
    String? email,
    String? phoneNumber,
    Map<String, Object?> customAttributes = const {},
  }) async {
    final updatedContact = await _repository.updateContact(
      sourceId: _requireSession.id.value,
      identifier: _requireSession.contact.identifier,
      name: name,
      email: email,
      phoneNumber: phoneNumber,
      customAttributes: customAttributes,
    );

    _session = _requireSession.copyWith(
      contact: updatedContact,
    );
  }

  @override
  Future<ChatwootConversation> createConversation() async {
    final created = await _repository.createConversation(sourceId: _requireSession.id.value);

    await refreshConversations();

    return created;
  }

  @override
  Future<void> resolveConversation({
    required ChatwootConversationId id,
  }) async {
    final sourceId = _requireSession.id.value;
    final result = await _handleConversationNotFound(
      conversationId: id,
      action: () => _repository.resolveConversation(
        sourceId: sourceId,
        conversationId: id,
      ),
    );

    final list = List<ChatwootConversation>.from(_stateSubject.value.conversations);
    try {
      final (updatedList, updated) = list.updateConversation(id, updater: (_) => result);
      _stateSubject.add(
        ChatwootState$Conversation$Updated(
          conversations: updatedList,
          conversation: updated,
        ),
      );
    } on StateError {
      await refreshConversations();
    }
  }

  @override
  Future<void> markConversationRead({
    required ChatwootConversationId id,
  }) async {
    final sourceId = _requireSession.id.value;
    await _handleConversationNotFound(
      conversationId: id,
      action: () => _repository.markConversationRead(
        sourceId: sourceId,
        conversationId: id,
      ),
    );

    final list = List<ChatwootConversation>.from(_stateSubject.value.conversations);

    try {
      final (updatedList, updated) = list.updateConversation(
        id,
        updater: (c) => c.copyWith(lastReadTime: DateTime.now()),
      );
      _stateSubject.add(
        ChatwootState$Conversation$Updated(
          conversations: updatedList,
          conversation: updated,
        ),
      );
    } on StateError {
      await refreshConversations();
    }
  }

  @override
  Future<void> toggleTyping({
    required ChatwootConversationId conversationId,
    required bool isTyping,
  }) async {
    await _handleConversationNotFound(
      conversationId: conversationId,
      action: () => _repository.toggleTyping(
        sourceId: _requireSession.id.value,
        conversationId: conversationId,
        isTyping: isTyping,
      ),
    );
  }

  @override
  Future<void> sendMessage({
    required ChatwootConversationId conversationId,
    String? content,
    List<XFile> attachments = const [],
  }) async {
    await _handleConversationNotFound(
      conversationId: conversationId,
      action: () => _repository.sendMessage(
        sourceId: _requireSession.id.value,
        conversationId: conversationId,
        content: content,
        attachments: attachments,
      ),
    );
  }

  @override
  Future<void> retryMessage({
    required ChatwootConversationId conversationId,
    required ChatwootMessage$Content$Outgoing message,
  }) async {
    await _handleConversationNotFound(
      conversationId: conversationId,
      action: () => _repository.retryMessage(
        sourceId: _requireSession.id.value,
        conversationId: conversationId,
        message: message,
      ),
    );
  }

  @override
  Future<void> dispose() async {
    if (_disposed) {
      return;
    }
    _disposed = true;
    await _stopCableAndPresence();
    await _cable.disconnect();
    await _stateSubject.close();
  }

  Future<T> _handleConversationNotFound<T>({
    required ChatwootConversationId conversationId,
    required Future<T> Function() action,
  }) async {
    try {
      return await action();
    } on ChatwootConversationException$NotFound {
      await _emitConversationDeletedIfPresent(conversationId);
      rethrow;
    }
  }

  Future<void> _emitConversationDeletedIfPresent(ChatwootConversationId conversationId) async {
    if (!_hasConversation(conversationId) || _emittedConversationDeletedIds.contains(conversationId.value)) {
      return;
    }

    List<ChatwootConversation> refreshed;
    try {
      refreshed = await _fetchConversations(sourceId: _requireSession.id.value);
    } on Object {
      refreshed = List<ChatwootConversation>.from(_stateSubject.value.conversations)
        ..removeWhere((c) => c.id == conversationId);
    }

    if (refreshed.any((c) => c.id == conversationId) || !_emittedConversationDeletedIds.add(conversationId.value)) {
      return;
    }

    _stateSubject.add(
      ChatwootState$Conversation$Deleted(
        conversations: refreshed,
        conversationId: conversationId,
      ),
    );
  }

  bool _hasConversation(ChatwootConversationId conversationId) {
    return _stateSubject.value.conversations.any((c) => c.id == conversationId);
  }

  Future<List<ChatwootConversation>> _fetchConversations({
    required String sourceId,
  }) {
    final current = _conversationsRefreshFuture;
    if (current != null && _conversationsRefreshSourceId == sourceId) {
      return current;
    }

    late final Future<List<ChatwootConversation>> refresh;
    refresh = _repository
        .fetchConversations(sourceId: sourceId)
        .then((list) {
          if (identical(_conversationsRefreshFuture, refresh)) {
            _emittedConversationDeletedIds.removeAll(list.map((c) => c.id.value));
          }
          return list;
        })
        .whenComplete(() {
          if (identical(_conversationsRefreshFuture, refresh)) {
            _resetConversationsRefresh();
          }
        });
    _conversationsRefreshSourceId = sourceId;
    _conversationsRefreshFuture = refresh;
    return refresh;
  }

  void _resetConversationsRefresh() {
    _conversationsRefreshSourceId = null;
    _conversationsRefreshFuture = null;
  }
}

extension on List<ChatwootConversation> {
  (List<ChatwootConversation>, ChatwootConversation) updateConversation(
    int id, {
    required ChatwootConversation Function(ChatwootConversation) updater,
  }) {
    final i = indexWhere((c) => c.id == id);
    if (i < 0) {
      throw StateError('Conversation not found: $id');
    }
    final updatedList = List<ChatwootConversation>.from(this);

    final updated = updater(updatedList[i]);

    updatedList[i] = updated;

    return (updatedList, updated);
  }

  UpdateMessageResult updateMessageById(
    int conversationId,
    int id, {
    required ChatwootMessage Function(ChatwootMessage) updater,
  }) {
    return _updateMessage(
      conversationId,
      (m) => m.id == id,
      updater: updater,
      error: StateError('Message not found: $id'),
    );
  }

  UpdateMessageResult updateMessageByEchoId(
    int conversationId,
    String echoId, {
    required ChatwootMessage Function(ChatwootMessage) updater,
  }) {
    return _updateMessage(
      conversationId,
      (m) => m.echoId == echoId,
      updater: updater,
      error: StateError('Message not found: $echoId'),
    );
  }

  UpdateMessageResult _updateMessage(
    int conversationId,
    bool Function(ChatwootMessage) messageChecker, {
    required ChatwootMessage Function(ChatwootMessage) updater,
    required Error error,
  }) {
    final conversationIndex = indexWhere((c) => c.id == conversationId);
    if (conversationIndex < 0) {
      throw StateError('Conversation not found: $conversationId');
    }
    final conversations = List<ChatwootConversation>.from(this);
    final conversation = conversations[conversationIndex];
    final messages = conversation.messages;

    final messageIndex = messages.indexWhere(messageChecker);
    if (messageIndex < 0) {
      throw error;
    }

    final newMessages = List<ChatwootMessage>.from(messages);
    final updatedMessage = updater(newMessages[messageIndex]);
    newMessages[messageIndex] = updatedMessage;

    final updatedConversation = conversation.copyWith(messages: newMessages);
    conversations[conversationIndex] = updatedConversation;

    return (
      conversations: conversations,
      conversationIndex: conversationIndex,
      messageIndex: messageIndex,
      updatedMessage: updatedMessage,
    );
  }
}

typedef UpdateMessageResult = ({
  List<ChatwootConversation> conversations,
  int conversationIndex,
  int messageIndex,
  ChatwootMessage updatedMessage,
});

import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:chatwoot_sdk/client/data/api/dto/chatwoot_conversation_dto.dart';
import 'package:chatwoot_sdk/client/data/api/dto/chatwoot_message_dto.dart';
import 'package:chatwoot_sdk/client/data/realtime_client/chatwoot_cable_uri.dart';
import 'package:chatwoot_sdk/client/data/realtime_client/chatwoot_socket.dart';
import 'package:chatwoot_sdk/client/data/realtime_client/chatwoot_socket_retry_policy.dart';
import 'package:chatwoot_sdk/client/domain/logger/chatwoot_logger.dart';
import 'package:chatwoot_sdk/client/domain/model/chatwoot_connection_state.dart';
import 'package:rxdart/rxdart.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

final class _SubscriptionNotConfirmed implements Exception {
  const _SubscriptionNotConfirmed(this.message);

  final String message;

  @override
  String toString() => message;
}

final class _SubscriptionConfirmation {
  _SubscriptionConfirmation({
    required ChatwootLogger? logger,
    required Duration confirmSubscriptionGracePeriod,
    required Duration confirmSubscriptionTimeout,
    required int attemptIndex,
    required bool isReconnected,
    required bool Function() isActive,
    required void Function() releaseConnectWait,
    required void Function() closeChannel,
  }) : _logger = logger,
       _confirmSubscriptionGracePeriod = confirmSubscriptionGracePeriod,
       _confirmSubscriptionTimeout = confirmSubscriptionTimeout,
       _attemptIndex = attemptIndex,
       _isReconnected = isReconnected,
       _isActive = isActive,
       _releaseConnectWait = releaseConnectWait,
       _closeChannel = closeChannel;

  final ChatwootLogger? _logger;
  final Duration _confirmSubscriptionGracePeriod;
  final Duration _confirmSubscriptionTimeout;
  final int _attemptIndex;
  final bool _isReconnected;
  final bool Function() _isActive;
  final void Function() _releaseConnectWait;
  final void Function() _closeChannel;

  final Completer<void> _completer = Completer<void>();
  Timer? _confirmSubscriptionTimer;
  Timer? _confirmAfterPingTimer;
  bool _pingBeforeConfirmSeen = false;

  Future<void> get future => _completer.future;

  bool get isCompleted => _completer.isCompleted;

  bool get _canComplete => !isCompleted && _isActive();

  void startTimeout() {
    _confirmSubscriptionTimer = Timer(_confirmSubscriptionTimeout, () {
      _failNoConfirmation(
        message: 'Chatwoot socket subscription was not confirmed in time.',
        extra: {
          'timeout_ms': _confirmSubscriptionTimeout.inMilliseconds,
          'attempt_index': _attemptIndex,
          'is_reconnect': _isReconnected,
          'ping_before_confirm_seen': _pingBeforeConfirmSeen,
        },
      );
    });
  }

  void handlePingBeforeConfirm() {
    if (isCompleted || _pingBeforeConfirmSeen) {
      return;
    }

    _pingBeforeConfirmSeen = true;
    _confirmAfterPingTimer = Timer(_confirmSubscriptionGracePeriod, () {
      _failNoConfirmation(
        message:
            'Chatwoot socket received ping before subscription confirmation, but confirm_subscription did not arrive in time.',
        extra: {
          'grace_period_ms': _confirmSubscriptionGracePeriod.inMilliseconds,
          'attempt_index': _attemptIndex,
          'is_reconnect': _isReconnected,
        },
      );
    });
  }

  void confirmIfNotCompleted() {
    if (isCompleted) {
      return;
    }

    dispose();
    _completer.complete();
  }

  void fail(Object error, [StackTrace? stackTrace]) {
    if (!_canComplete) {
      return;
    }

    _releaseConnectWait();
    _completer.completeError(error, stackTrace ?? StackTrace.current);
  }

  void dispose() {
    _confirmAfterPingTimer?.cancel();
    _confirmAfterPingTimer = null;
    _confirmSubscriptionTimer?.cancel();
    _confirmSubscriptionTimer = null;
  }

  void _failNoConfirmation({
    required String message,
    required Map<String, Object?> extra,
  }) {
    if (!_canComplete) {
      return;
    }

    final error = _SubscriptionNotConfirmed(message);
    final stackTrace = StackTrace.current;
    _logger?.warning(message, error: error, stackTrace: stackTrace, extra: extra);
    _releaseConnectWait();
    _completer.completeError(error, stackTrace);
    _closeChannel();
  }
}

class ChatwootSocketImpl implements ChatwootSocket {
  static const defaultConfirmSubscriptionGracePeriod = Duration(seconds: 3);
  static const defaultConfirmSubscriptionTimeout = Duration(seconds: 10);

  ChatwootSocketImpl({
    required Uri baseUrl,
    ChatwootSocketRetryPolicy? retryPolicy,
    ChatwootLogger? logger,
    Duration confirmSubscriptionGracePeriod = defaultConfirmSubscriptionGracePeriod,
    Duration confirmSubscriptionTimeout = defaultConfirmSubscriptionTimeout,
  }) : _baseUrl = baseUrl,
       _retryPolicy = retryPolicy ?? ChatwootSocketRetryPolicy.defaultPolicy,
       _logger = logger,
       _confirmSubscriptionGracePeriod = confirmSubscriptionGracePeriod,
       _confirmSubscriptionTimeout = confirmSubscriptionTimeout,
       _connectionState = BehaviorSubject<ChatwootConnectionState>.seeded(
         const ChatwootConnectionState$Disconnected(),
       ),
       _events = PublishSubject<ChatwootSocketEvent>();

  final Uri _baseUrl;
  final ChatwootSocketRetryPolicy _retryPolicy;
  final ChatwootLogger? _logger;
  final Duration _confirmSubscriptionGracePeriod;
  final Duration _confirmSubscriptionTimeout;

  final BehaviorSubject<ChatwootConnectionState> _connectionState;
  final PublishSubject<ChatwootSocketEvent> _events;

  int _generation = 0;
  bool _userDisconnected = false;

  WebSocketChannel? _channel;
  StreamSubscription<dynamic>? _subscription;
  String? _pubsubToken;

  Completer<void>? _connectCompleter;
  Completer<void>? _reconnectWaitCancel;

  @override
  Stream<ChatwootConnectionState> get connectionState => _connectionState.stream;

  @override
  Stream<ChatwootSocketEvent> get events => _events.stream;

  @override
  Future<void> connect({
    required String sourceId,
    required String pubsubToken,
  }) async {
    _userDisconnected = false;
    // [sourceId]: required by [ChatwootSocket]; not sent on `RoomChannel` wire (resolved via [pubsubToken]).
    final _ = sourceId;

    _generation++;
    final gen = _generation;
    _cancelReconnectWait();

    _releaseConnectWait();
    _connectCompleter = Completer<void>();
    final waitConnected = _connectCompleter!.future;

    await _disposeSocket();

    unawaited(
      Future<void>(() async {
        try {
          await _runLoop(gen, pubsubToken);
        } on Object catch (_) {}
      }),
    );

    return waitConnected;
  }

  @override
  Future<void> disconnect() async {
    _userDisconnected = true;
    _generation++;
    _cancelReconnectWait();

    await _disposeSocket();

    _releaseConnectWait();
    _connectCompleter = null;

    if (!_connectionState.isClosed) {
      _connectionState.add(const ChatwootConnectionState$Disconnected());
    }
  }

  @override
  Future<void> markPresence() async {
    final channel = _channel;
    final pubsubToken = _pubsubToken;
    if (channel == null || pubsubToken == null) {
      throw StateError('websocket is not connected');
    }

    channel.sink.add(_messageJson(pubsubToken, const {'action': 'update_presence'}));
  }

  Future<void> _runLoop(int gen, String pubsubToken) async {
    final rng = Random();
    var attempt = 0;
    var reconnectingAfterDrop = false;

    while (!_userDisconnected && gen == _generation) {
      if (reconnectingAfterDrop) {
        if (!_connectionState.isClosed) {
          _connectionState.add(const ChatwootConnectionState$Reconnecting());
        }
      }

      var sessionSucceeded = false;
      try {
        await _singleSocketLifetime(
          gen,
          pubsubToken,
          isReconnected: reconnectingAfterDrop,
          attemptIndex: attempt,
        );
        sessionSucceeded = true;
      } on Object catch (error, stackTrace) {
        // Expected on subscribe failure, abort, or wire errors before confirmation.
        if (!_userDisconnected && gen == _generation && error is! _SubscriptionNotConfirmed) {
          _logger?.warning(
            'Chatwoot socket connection failed.',
            error: error,
            stackTrace: stackTrace,
            extra: {
              'attempt_index': attempt,
              'is_reconnect': reconnectingAfterDrop,
            },
          );
        }
      }

      if (_userDisconnected || gen != _generation) {
        break;
      }

      if (sessionSucceeded) {
        attempt = 0;
        reconnectingAfterDrop = true;
      }

      final reconnectWaitCancel = Completer<void>();
      _reconnectWaitCancel = reconnectWaitCancel;
      try {
        await _retryPolicy.waitBeforeReconnect(
          ChatwootSocketRetryContext(
            attemptIndex: attempt,
            random: rng,
            cancelled: reconnectWaitCancel.future,
            previousSessionSucceeded: sessionSucceeded,
          ),
        );
      } finally {
        if (identical(_reconnectWaitCancel, reconnectWaitCancel)) {
          _reconnectWaitCancel = null;
        }
      }

      if (!sessionSucceeded) {
        attempt++;
      }

      if (_userDisconnected || gen != _generation) {
        break;
      }
    }
  }

  void _cancelReconnectWait() {
    final reconnectWaitCancel = _reconnectWaitCancel;
    _reconnectWaitCancel = null;
    if (reconnectWaitCancel case final completer? when !completer.isCompleted) {
      completer.complete();
    }
  }

  void _releaseConnectWait() {
    if (_connectCompleter case final completer? when !completer.isCompleted) {
      completer.complete();
    }
  }

  Future<void> _singleSocketLifetime(
    int gen,
    String pubsubToken, {
    required bool isReconnected,
    required int attemptIndex,
  }) async {
    try {
      if (pubsubToken.isEmpty) {
        throw StateError('pubsubToken empty');
      }

      final uri = chatwootCableUri(_baseUrl, pubsubToken);
      final channel = WebSocketChannel.connect(uri);
      _channel = channel;
      _pubsubToken = pubsubToken;

      final ended = Completer<void>();
      final confirmation = _SubscriptionConfirmation(
        logger: _logger,
        confirmSubscriptionGracePeriod: _confirmSubscriptionGracePeriod,
        confirmSubscriptionTimeout: _confirmSubscriptionTimeout,
        attemptIndex: attemptIndex,
        isReconnected: isReconnected,
        isActive: () => !_userDisconnected && gen == _generation,
        releaseConnectWait: _releaseConnectWait,
        closeChannel: () => unawaited(channel.sink.close()),
      );

      late final StreamSubscription<dynamic> sub;
      sub = channel.stream.listen(
        (dynamic data) {
          if (_userDisconnected || gen != _generation) {
            return;
          }
          _handleWireMessage(
            data,
            channel,
            confirmation,
          );
        },
        onError: (Object error, StackTrace stackTrace) {
          confirmation.fail(error, stackTrace);
          if (!ended.isCompleted) {
            ended.complete();
          }
        },
        onDone: () {
          confirmation.fail(StateError('socket closed before subscription'));
          if (!ended.isCompleted) {
            ended.complete();
          }
        },
        cancelOnError: false,
      );
      _subscription = sub;

      channel.sink.add(_subscribeJson(pubsubToken));
      confirmation.startTimeout();

      try {
        await confirmation.future;

        if (_userDisconnected || gen != _generation) {
          throw StateError('aborted');
        }

        if (!_connectionState.isClosed) {
          _connectionState.add(ChatwootConnectionState$Connected(isReconnected: isReconnected));
        }

        _releaseConnectWait();

        await ended.future;
      } finally {
        confirmation.dispose();
        await sub.cancel();
        if (identical(_subscription, sub)) {
          _subscription = null;
        }
        await channel.sink.close();
        if (identical(_channel, channel)) {
          _channel = null;
          _pubsubToken = null;
        }
      }
    } on Object {
      if (!_userDisconnected && gen == _generation) {
        _releaseConnectWait();
      }
      rethrow;
    }
  }

  static String _subscribeJson(String pubsubToken) {
    final identifier = jsonEncode({
      'channel': 'RoomChannel',
      'pubsub_token': pubsubToken,
    });
    return jsonEncode({'command': 'subscribe', 'identifier': identifier});
  }

  static String _messageJson(String pubsubToken, Map<String, Object?> data) {
    final identifier = jsonEncode({
      'channel': 'RoomChannel',
      'pubsub_token': pubsubToken,
    });
    return jsonEncode({
      'command': 'message',
      'identifier': identifier,
      'data': jsonEncode(data),
    });
  }

  void _handleWireMessage(
    dynamic data,
    WebSocketChannel channel,
    _SubscriptionConfirmation confirmation,
  ) {
    final String text;
    if (data is String) {
      text = data;
    } else if (data is List<int>) {
      text = utf8.decode(data);
    } else {
      return;
    }

    late final Map<String, dynamic> json;
    try {
      final decoded = jsonDecode(text);
      if (decoded is! Map) {
        return;
      }
      json = Map<String, dynamic>.from(decoded);
    } on Object catch (_) {
      return;
    }

    final typeRaw = json['type'];
    final typeStr = typeRaw is String ? typeRaw.toLowerCase() : '';

    switch (typeStr) {
      case 'welcome':
        return;
      case 'ping':
        confirmation.handlePingBeforeConfirm();
        final msg = json['message'];
        channel.sink.add(jsonEncode({'command': 'pong', 'message': msg}));
        return;
      case 'confirm_subscription':
        confirmation.confirmIfNotCompleted();
        return;
      case 'reject_subscription':
        confirmation.fail(StateError('reject_subscription'));
        return;
      default:
        break;
    }

    final payload = json['message'];
    if (payload is Map<String, dynamic>) {
      _handleChannelPayload(payload);
    } else if (payload is Map) {
      _handleChannelPayload(Map<String, dynamic>.from(payload));
    }
  }

  void _handleChannelPayload(Map<String, dynamic> message) {
    final event = message['event'] as String?;
    final data = message['data'];
    if (event == null || data is! Map) {
      return;
    }
    final map = Map<String, dynamic>.from(data);

    switch (event) {
      case 'message.created':
        _emit(() => ChatwootSocketEvent$Message$Created(message: ChatwootMessageDto.fromJson(map)));
        return;
      case 'message.updated':
        _emit(() => ChatwootSocketEvent$Message$Updated(message: ChatwootMessageDto.fromJson(map)));
        return;
      case 'conversation.status_changed':
        _emit(() {
          final conversation = ChatwootConversationDto.fromJson(map);
          return ChatwootSocketEvent$Conversation$StatusChanged(conversation: conversation);
        });
        return;
      case 'conversation.typing_on':
        if (map['is_private'] != true) {
          final onConversation = _conversationDtoFromTypingEnvelope(map);
          if (onConversation != null && !_events.isClosed) {
            _events.add(ChatwootSocketEvent$Conversation$TypingOn(conversation: onConversation));
          }
        }
        return;
      case 'conversation.typing_off':
        final offConversation = _conversationDtoFromTypingEnvelope(map);
        if (offConversation != null && !_events.isClosed) {
          _events.add(ChatwootSocketEvent$Conversation$TypingOff(conversation: offConversation));
        }
        return;
      default:
        return;
    }
  }

  void _emit(ChatwootSocketEvent Function() build) {
    if (_events.isClosed) {
      return;
    }
    try {
      _events.add(build());
    } on Object catch (_) {}
  }

  static ChatwootConversationDto? _conversationDtoFromTypingEnvelope(Map<String, dynamic> data) {
    final conv = data['conversation'];
    if (conv is! Map) {
      return null;
    }
    try {
      return ChatwootConversationDto.fromJson(Map<String, dynamic>.from(conv));
    } on Object catch (_) {
      return null;
    }
  }

  Future<void> _disposeSocket() async {
    await _subscription?.cancel();
    _subscription = null;
    await _channel?.sink.close();
    _channel = null;
    _pubsubToken = null;
  }
}

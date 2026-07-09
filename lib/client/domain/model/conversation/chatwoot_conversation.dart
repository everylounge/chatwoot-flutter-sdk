import 'package:meta/meta.dart';

import '../message/chatwoot_message.dart';

enum ChatwootConversationStatus {
  open,
  resolved,
  pending,
  snoozed;

  factory ChatwootConversationStatus.fromString(String raw) {
    switch (raw.toLowerCase()) {
      case 'open':
        return ChatwootConversationStatus.open;
      case 'resolved':
        return ChatwootConversationStatus.resolved;
      case 'pending':
        return ChatwootConversationStatus.pending;
      case 'snoozed':
        return ChatwootConversationStatus.snoozed;
      default:
        return ChatwootConversationStatus.open;
    }
  }
}

extension type ChatwootConversationId(int value) implements int {}

@immutable
class ChatwootConversation implements Comparable<ChatwootConversation> {
  /// Count of unread not outgoing messages.
  static int _unreadSupportMessageCount({
    required List<ChatwootMessage> messages,
    required DateTime lastReadTime,
  }) {
    var n = 0;
    for (var i = messages.length - 1; i >= 0; i--) {
      final m = messages[i];

      if (m is ChatwootMessage$Content$Outgoing) return n;

      if (m.sentAt.isBefore(lastReadTime)) return n;
      n++;
    }

    return n;
  }

  const ChatwootConversation._({
    required this.id,
    required this.status,
    this.messages = const [],
    this.supportTyping = false,
    this.unreadCount = 0,
    required this.lastReadTime,
  });

  factory ChatwootConversation({
    required ChatwootConversationId id,
    required ChatwootConversationStatus status,
    required DateTime lastReadTime,
    List<ChatwootMessage> messages = const [],
    bool supportTyping = false,
  }) {
    final unread = _unreadSupportMessageCount(messages: messages, lastReadTime: lastReadTime);

    return ChatwootConversation._(
      id: id,
      status: status,
      messages: messages,
      supportTyping: supportTyping,
      unreadCount: unread,
      lastReadTime: lastReadTime,
    );
  }

  final ChatwootConversationId id;
  final ChatwootConversationStatus status;
  final List<ChatwootMessage> messages;

  /// Support is typing in this conversation.
  final bool supportTyping;

  /// Count of unread not outgoing messages.
  final int unreadCount;
  final DateTime lastReadTime;

  ChatwootConversation copyWith({
    List<ChatwootMessage>? messages,
    ChatwootConversationStatus? status,
    bool? supportTyping,
    DateTime? lastReadTime,
  }) {
    return ChatwootConversation(
      id: id,
      status: status ?? this.status,
      messages: messages ?? this.messages,
      supportTyping: supportTyping ?? this.supportTyping,
      lastReadTime: lastReadTime ?? this.lastReadTime,
    );
  }
  
  @override
  int compareTo(ChatwootConversation other) {
    final lastMessage = messages.lastOrNull;
    final otherLastMessage = other.messages.lastOrNull;

    if (lastMessage == null && otherLastMessage == null) return id.compareTo(other.id);
    if (lastMessage == null) return 1;
    if (otherLastMessage == null) return -1;

    return otherLastMessage.sentAt.compareTo(lastMessage.sentAt);
  }
}

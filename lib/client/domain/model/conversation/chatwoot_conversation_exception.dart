import 'package:chatwoot_sdk/client/domain/model/conversation/chatwoot_conversation.dart';

sealed class ChatwootConversationException implements Exception {
  const ChatwootConversationException();
}

final class ChatwootConversationException$NotFound extends ChatwootConversationException {
  const ChatwootConversationException$NotFound({
    required this.conversationId,
    this.cause,
  });

  final ChatwootConversationId conversationId;
  final Object? cause;

  @override
  String toString() => 'ChatwootConversationException\$NotFound(conversationId: $conversationId)';
}

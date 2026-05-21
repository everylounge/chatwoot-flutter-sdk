import 'package:chatwoot_sdk/client/domain/model/conversation/chatwoot_conversation.dart';
import 'package:chatwoot_sdk/client/domain/model/message/chatwoot_message.dart';

sealed class ChatwootState {
  const ChatwootState({
    required this.conversations,
  });

  final List<ChatwootConversation> conversations;
}

class ChatwootState$ConversationsLoaded extends ChatwootState {
  const ChatwootState$ConversationsLoaded({
    required super.conversations,
  });
}

sealed class ChatwootState$Message extends ChatwootState {
  const ChatwootState$Message({
    required super.conversations,
    required this.conversationId,
    required this.message,
  });

  final ChatwootConversationId conversationId;
  final ChatwootMessage message;
}

class ChatwootState$Message$New extends ChatwootState$Message {
  const ChatwootState$Message$New({
    required super.conversations,
    required super.conversationId,
    required super.message,
  });
}

class ChatwootState$Message$Updated extends ChatwootState$Message {
  const ChatwootState$Message$Updated({
    required super.conversations,
    required super.conversationId,
    required super.message,
    required this.messageIndex,
  });

  final int messageIndex;
}

class ChatwootState$Conversation extends ChatwootState {
  const ChatwootState$Conversation({
    required super.conversations,
    required this.conversation,
  });

  final ChatwootConversation conversation;
}

class ChatwootState$Conversation$Updated extends ChatwootState$Conversation {
  const ChatwootState$Conversation$Updated({
    required super.conversations,
    required super.conversation,
  });
}

class ChatwootState$Conversation$Deleted extends ChatwootState {
  const ChatwootState$Conversation$Deleted({
    required super.conversations,
    required this.conversationId,
  });

  final ChatwootConversationId conversationId;
}

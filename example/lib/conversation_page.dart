import 'dart:async';

import 'package:cached_network_image/cached_network_image.dart';
import 'package:chatwoot_sdk/chatwoot_sdk.dart';
import 'package:example/chat.dart';
import 'package:example/local_file_image.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter_chat_core/flutter_chat_core.dart';
import 'package:flutter_chat_ui/flutter_chat_ui.dart';
import 'package:image_picker/image_picker.dart';

class ConversationChatPage extends StatefulWidget {
  const ConversationChatPage({
    super.key,
    required this.client,
    required this.initialConversation,
    required this.currentUserId,
  });

  final ChatwootClient client;
  final ChatwootConversation initialConversation;
  final String currentUserId;

  ChatwootConversationId get conversationId => initialConversation.id;

  @override
  State<ConversationChatPage> createState() => _ConversationChatPageState();
}

class _ConversationChatPageState extends State<ConversationChatPage> {
  late final InMemoryChatController _chatController;
  late final TextEditingController _composerController;

  StreamSubscription<ChatwootState>? _stateSub;

  Timer? _typingCooldown;
  bool _typingActive = false;
  bool _markingRead = false;

  ChatwootConversation? _latestConversation;

  @override
  void initState() {
    super.initState();
    _chatController = InMemoryChatController();
    _composerController = TextEditingController();
    _composerController.addListener(_onComposerChanged);
    _latestConversation = widget.initialConversation;

    unawaited(_syncMessages(widget.initialConversation));
    _stateSub = widget.client.statesStream.listen(_onClientState);
    unawaited(_markCurrentConversationRead());
  }

  void _onComposerChanged() {
    final hasText = _composerController.text.trim().isNotEmpty;
    if (hasText) {
      _signalTyping(true);
      _typingCooldown?.cancel();
      _typingCooldown = Timer(const Duration(seconds: 2), () => _signalTyping(false));
    } else {
      _typingCooldown?.cancel();
      unawaited(_signalTyping(false));
    }
  }

  Future<void> _signalTyping(bool typing) async {
    if (typing) {
      if (_typingActive) return;
      _typingActive = true;
      try {
        await widget.client.toggleTyping(
          conversationId: widget.conversationId,
          isTyping: true,
        );
      } on Object {
        // ignore
      }
      return;
    }

    _typingCooldown?.cancel();
    if (!_typingActive) return;
    _typingActive = false;
    try {
      await widget.client.toggleTyping(
        conversationId: widget.conversationId,
        isTyping: false,
      );
    } on Object {
      // ignore
    }
  }

  void _onClientState(ChatwootState state) {
    if (state case ChatwootState$Conversation$Deleted(:final conversationId)
        when conversationId == widget.conversationId) {
      if (mounted && Navigator.of(context).canPop()) {
        Navigator.of(context).pop();
      }
      return;
    }

    final conversation = state.conversations.where((c) => c.id == widget.conversationId).firstOrNull;
    if (conversation == null) {
      return;
    }
    _latestConversation = conversation;
    switch (state) {
      case ChatwootState$ConversationsLoaded(:final conversations):
        _syncConversationList(conversations);
      case ChatwootState$Conversation(:final conversation) when conversation.id == widget.conversationId:
        _syncConversation(conversation);
      case ChatwootState$Conversation$Deleted():
      case ChatwootState$Conversation():
      case ChatwootState$Conversation$Created():
        return;
      case ChatwootState$Message$New(:final conversationId, :final message)
          when conversationId == widget.conversationId:
        _chatController.insertAllMessages(message.uiMessages(widget.currentUserId).cast<Message>().toList());
        return;
      case ChatwootState$Message$Updated(:final conversationId, :final messageIndex, :final message)
          when conversationId == widget.conversationId:
        final oldMessage = _chatController.messages[messageIndex];
        final messages = message.uiMessages(widget.currentUserId);
        _chatController.updateMessage(
          oldMessage,
          messages.first,
        );
        return;
      case ChatwootState$Message$New():
      case ChatwootState$Message$Updated():
        return;
    }
  }

  void _syncConversationList(List<ChatwootConversation> conversations) {
    for (final c in conversations) {
      if (c.id == widget.conversationId) {
        _syncConversation(c);
        return;
      }
    }
  }

  void _syncConversation(ChatwootConversation conversation) {
    _latestConversation = conversation;
    unawaited(_syncMessages(conversation));
    if (conversation.unreadCount > 0) {
      unawaited(_markCurrentConversationRead());
    }
    if (mounted) {
      setState(() {});
    }
  }

  Future<void> _syncMessages(ChatwootConversation conversation) {
    final ui = conversation.messages.uiMessages(widget.currentUserId);
    return _chatController.setMessages(ui);
  }

  Future<void> _markCurrentConversationRead() async {
    if (_markingRead) {
      return;
    }
    _markingRead = true;
    try {
      await widget.client.markConversationRead(id: widget.conversationId);
    } on Object {
      // New state or manual refresh will try again if unread messages remain.
    } finally {
      _markingRead = false;
    }
  }

  Future<User?> _resolveUser(UserID id) async {
    if (id == widget.currentUserId) {
      final c = widget.client.contact;
      return User(id: id, name: c.name ?? 'You');
    }
    if (id == kSupportUserId) {
      return const User(id: kSupportUserId, name: 'Support');
    }
    if (id == kSystemUserId) {
      return const User(id: kSystemUserId, name: 'System');
    }
    return User(id: id, name: id);
  }

  Future<void> _onRefresh() async {
    await widget.client.refreshConversations();
  }

  Future<void> _onResolve() async {
    try {
      await widget.client.resolveConversation(id: widget.conversationId);
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('Conversation resolved')));
      }
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text('resolve: $e')));
      }
    }
  }

  Future<void> _pickAndSendAttachments() async {
    final picker = ImagePicker();
    final XFile? file;
    try {
      file = await picker.pickMedia();
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text('Pick attachments: $e')));
      }
      return;
    }
    if (file == null || !mounted) return;

    await _signalTyping(false);
    try {
      await widget.client.sendMessage(
        conversationId: widget.conversationId,
        attachments: [file],
      );
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text('Attachments: $e')));
      }
    }
  }

  Future<void> _sendTextMessage(String text) async {
    unawaited(_signalTyping(false));
    final trimmed = text.trim();
    if (trimmed.isEmpty) return;

    try {
      await widget.client.sendMessage(
        conversationId: widget.conversationId,
        content: trimmed,
      );
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text('Message: $e')));
      }
    }
  }

  Future<void> _retryFailed(Message uiMessage) async {
    final conv = _latestConversation;
    if (conv == null) return;

    final echoId = uiMessage.metadata?['chatwootEchoId'] as String?;
    final mid = uiMessage.metadata?['chatwootMessageId'] as int?;

    ChatwootMessage$Content$Outgoing? failed;
    for (final m in conv.messages) {
      switch (m) {
        case ChatwootMessage$Content$Outgoing(:final status) when status == OutgoingMessageStatus.failed:
          if (echoId != null && echoId.isNotEmpty && m.echoId == echoId) {
            failed = m;
            break;
          }
          if ((echoId == null || echoId.isEmpty) && m.id == mid) {
            failed = m;
            break;
          }
        case ChatwootMessage$Content$Outgoing():
        case ChatwootMessage$Content$Incoming():
        case ChatwootMessage$Activity():
          break;
      }
      if (failed != null) break;
    }

    if (failed == null) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('Original message for retry was not found')),
        );
      }
      return;
    }

    try {
      await widget.client.retryMessage(
        conversationId: widget.conversationId,
        message: failed,
      );
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text('retry: $e')));
      }
    }
  }

  void _onMessageTap(
    BuildContext context,
    Message message, {
    required int index,
    required TapUpDetails details,
  }) {
    if (message.metadata?['failedOutgoing'] == true) {
      unawaited(_retryFailed(message));
    }
  }

  @override
  void dispose() {
    unawaited(_signalTyping(false));
    _typingCooldown?.cancel();
    unawaited(_stateSub?.cancel());
    _composerController.removeListener(_onComposerChanged);
    _composerController.dispose();
    _chatController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final titleId = widget.conversationId;
    final conv = _latestConversation;

    return Scaffold(
      appBar: AppBar(
        title: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          mainAxisSize: MainAxisSize.min,
          children: [
            Text('Chat #$titleId'),
            if (conv?.supportTyping == true)
              Text(
                'Teammate is typing...',
                style: Theme.of(context).textTheme.bodySmall,
              ),
          ],
        ),
        actions: [
          IconButton(
            tooltip: 'Refresh conversation',
            onPressed: () async {
              await _onRefresh();
            },
            icon: const Icon(Icons.refresh),
          ),
          IconButton(
            tooltip: 'Resolve conversation',
            onPressed: _onResolve,
            icon: const Icon(Icons.check_circle_outline),
          ),
        ],
      ),
      body: Chat(
        currentUserId: widget.currentUserId,
        resolveUser: _resolveUser,
        chatController: _chatController,
        theme: ChatTheme.light(),
        onMessageSend: (text) {
          unawaited(_sendTextMessage(text));
        },
        onAttachmentTap: _pickAndSendAttachments,
        onMessageTap: _onMessageTap,
        builders: Builders(
          composerBuilder: (context) => Composer(
            textEditingController: _composerController,
            hintText: 'Message...',
            sendButtonVisibilityMode: SendButtonVisibilityMode.hidden,
            sendOnEnter: false,
            minLines: 1,
            maxLines: 6,
          ),
          textMessageBuilder: (context, message, index, {required isSentByMe, groupStatus}) {
            final failed = message.metadata?['failedOutgoing'] == true;
            return SimpleTextMessage(
              message: message,
              index: index,
              topWidget: failed
                  ? Padding(
                      padding: const EdgeInsets.only(bottom: 6),
                      child: Text(
                        'Not delivered - tap to retry',
                        style: TextStyle(
                          fontSize: 11,
                          color: Theme.of(context).colorScheme.error,
                        ),
                      ),
                    )
                  : null,
            );
          },
          imageMessageBuilder: (context, message, index, {required isSentByMe, groupStatus}) {
            return _ExampleNetworkOrFileImage(message: message);
          },
          fileMessageBuilder: (context, message, index, {required isSentByMe, groupStatus}) {
            return _ExampleFileTile(message: message);
          },
          systemMessageBuilder: (context, message, index, {required isSentByMe, groupStatus}) {
            return Padding(
              padding: const EdgeInsets.symmetric(vertical: 6),
              child: Center(
                child: DecoratedBox(
                  decoration: BoxDecoration(
                    color: Theme.of(context).colorScheme.surfaceContainerHighest,
                    borderRadius: BorderRadius.circular(12),
                  ),
                  child: Padding(
                    padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 6),
                    child: Text(
                      message.text,
                      style: Theme.of(context).textTheme.labelMedium,
                      textAlign: TextAlign.center,
                    ),
                  ),
                ),
              ),
            );
          },
        ),
      ),
    );
  }
}

class _ExampleNetworkOrFileImage extends StatelessWidget {
  const _ExampleNetworkOrFileImage({required this.message});

  final ImageMessage message;

  @override
  Widget build(BuildContext context) {
    final src = message.source;
    final borderRadius = BorderRadius.circular(12);
    Widget image;
    if (src.startsWith('http://') || src.startsWith('https://')) {
      image = kIsWeb
          ? Image.network(
              src,
              width: 260,
              height: 260,
              fit: BoxFit.cover,
              webHtmlElementStrategy: WebHtmlElementStrategy.prefer,
              loadingBuilder: (_, child, progress) {
                if (progress == null) {
                  return child;
                }
                return const Center(child: CircularProgressIndicator());
              },
              errorBuilder: (_, error, stackTrace) => _ExampleImageError(
                source: src,
                error: error,
              ),
            )
          : CachedNetworkImage(
              imageUrl: src,
              width: 260,
              height: 260,
              fit: BoxFit.cover,
              progressIndicatorBuilder: (_, __, progress) => Center(
                child: CircularProgressIndicator(value: progress.progress),
              ),
              errorWidget: (_, __, error) => _ExampleImageError(
                source: src,
                error: error,
              ),
            );
    } else {
      image = SizedBox(
        width: 260,
        height: 260,
        child: localFileImage(src, fit: BoxFit.cover),
      );
    }

    return ClipRRect(
      borderRadius: borderRadius,
      child: image,
    );
  }
}

class _ExampleImageError extends StatelessWidget {
  const _ExampleImageError({
    required this.source,
    required this.error,
  });

  final String source;
  final Object error;

  @override
  Widget build(BuildContext context) {
    return Container(
      width: 260,
      height: 260,
      padding: const EdgeInsets.all(12),
      color: Theme.of(context).colorScheme.errorContainer,
      child: DefaultTextStyle(
        style: Theme.of(context).textTheme.bodySmall!.copyWith(
              color: Theme.of(context).colorScheme.onErrorContainer,
            ),
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Icon(Icons.broken_image_outlined),
            const SizedBox(height: 8),
            const Text('Image load failed'),
            const SizedBox(height: 4),
            Text(
              error.toString(),
              maxLines: 4,
              overflow: TextOverflow.ellipsis,
            ),
            const SizedBox(height: 4),
            Text(
              source,
              maxLines: 2,
              overflow: TextOverflow.ellipsis,
            ),
          ],
        ),
      ),
    );
  }
}

class _ExampleFileTile extends StatelessWidget {
  const _ExampleFileTile({required this.message});

  final FileMessage message;

  @override
  Widget build(BuildContext context) {
    final fileSize = message.metadata?['attachmentFileSize'] as int?;

    return Align(
      alignment: Alignment.centerLeft,
      child: ConstrainedBox(
        constraints: const BoxConstraints(maxWidth: 280),
        child: Material(
          color: Theme.of(context).colorScheme.surfaceContainerHighest,
          borderRadius: BorderRadius.circular(12),
          child: ListTile(
            leading: const Icon(Icons.attach_file),
            title: Text(message.name),
            subtitle: fileSize != null ? null : Text(fileSize!.formattedBytes),
          ),
        ),
      ),
    );
  }
}

extension on int {
  String get formattedBytes {
    if (this < 1024) {
      return '$this B';
    }
    final kb = this / 1024;
    if (kb < 1024) {
      return '${kb.toStringAsFixed(kb < 10 ? 1 : 0)} KB';
    }
    final mb = kb / 1024;
    if (mb < 1024) {
      return '${mb.toStringAsFixed(mb < 10 ? 1 : 0)} MB';
    }
    final gb = mb / 1024;
    return '${gb.toStringAsFixed(gb < 10 ? 1 : 0)} GB';
  }
}

extension on List<ChatwootMessage> {
  List<Message> uiMessages(String currentUserId) {
    return expand((message) => message.uiMessages(currentUserId)).toList();
  }
}

extension on ChatwootMessage {
  String get uiKey {
    return switch (this) {
      ChatwootMessage$Content$Outgoing(:final echoId) when echoId != null && echoId.isNotEmpty => 'cw_echo_$echoId',
      _ => 'cw_id_$id',
    };
  }

  Iterable<Message> uiMessages(String currentUserId) sync* {
    Iterable<Message> contentMessages(
      ChatwootMessage$Content message, {
      required String authorId,
      required MessageStatus? status,
      required Map<String, dynamic> metadata,
    }) sync* {
      final trimmed = message.content?.trim();
      if (trimmed != null && trimmed.isNotEmpty) {
        yield Message.text(
          id: '${message.uiKey}_t',
          authorId: authorId,
          createdAt: message.sentAt.toUtc(),
          text: trimmed,
          status: status,
          metadata: metadata,
        );
      }

      var attachmentIndex = 0;
      for (final att in message.attachments) {
        final suffix = '${message.uiKey}_a${attachmentIndex++}';
        switch (att) {
          case final Attachment$File att:
            yield att.uiFileMessage(
              id: suffix,
              authorId: authorId,
              sentAt: message.sentAt,
              status: status,
              metadata: metadata,
            );
          case final Attachment$Link att:
            if (att.fileType == AttachmentFileType.image) {
              final uri = att.thumbnail ?? att.url;
              yield Message.image(
                id: suffix,
                authorId: authorId,
                createdAt: message.sentAt.toUtc(),
                source: uri.toString(),
                status: status,
                metadata: <String, dynamic>{
                  ...metadata,
                  'attachmentUrl': att.url.toString(),
                  if (att.thumbnail case final thumbnail?) 'attachmentThumbnail': thumbnail.toString(),
                  'attachmentFileName': att.fileName,
                  if (att.fileName.extension case final extension?) 'attachmentExtension': extension,
                  'attachmentFileSize': att.fileSize,
                },
              );
            } else {
              yield att.uiFileMessage(
                id: suffix,
                authorId: authorId,
                sentAt: message.sentAt,
                status: status,
                metadata: metadata,
              );
            }
        }
      }

      if ((trimmed == null || trimmed.isEmpty) && message.attachments.isEmpty) {
        yield Message.text(
          id: message.uiKey,
          authorId: authorId,
          createdAt: message.sentAt.toUtc(),
          text: '(empty message)',
          status: status,
          metadata: metadata,
        );
      }
    }

    switch (this) {
      case ChatwootMessage$Activity():
        yield Message.system(
          id: uiKey,
          authorId: kSystemUserId,
          createdAt: sentAt.toUtc(),
          text: content ?? '',
        );
      case final ChatwootMessage$Content$Incoming incoming:
        yield* contentMessages(
          incoming,
          authorId: kSupportUserId,
          status: null,
          metadata: <String, dynamic>{
            'chatwootMessageId': incoming.id,
          },
        );
      case final ChatwootMessage$Content$Outgoing outgoing:
        yield* contentMessages(
          outgoing,
          authorId: currentUserId,
          status: outgoing.status.uiStatus,
          metadata: <String, dynamic>{
            'chatwootMessageId': outgoing.id,
            if (outgoing.echoId != null) 'chatwootEchoId': outgoing.echoId,
            if (outgoing.status == OutgoingMessageStatus.failed) 'failedOutgoing': true,
          },
        );
    }
  }
}

extension on OutgoingMessageStatus {
  MessageStatus get uiStatus {
    return switch (this) {
      OutgoingMessageStatus.sending => MessageStatus.sending,
      OutgoingMessageStatus.delivered => MessageStatus.delivered,
      OutgoingMessageStatus.failed => MessageStatus.error,
    };
  }
}

extension on Attachment$File {
  Message uiFileMessage({
    required String id,
    required String authorId,
    required DateTime sentAt,
    MessageStatus? status,
    required Map<String, dynamic> metadata,
  }) {
    return Message.file(
      id: id,
      authorId: authorId,
      createdAt: sentAt.toUtc(),
      source: file.path,
      name: fileName ?? file.name,
      status: status,
      metadata: <String, dynamic>{
        ...metadata,
        if (fileSize case final fileSize?) 'attachmentFileSize': fileSize,
      },
    );
  }
}

extension on Attachment$Link {
  Message uiFileMessage({
    required String id,
    required String authorId,
    required DateTime sentAt,
    MessageStatus? status,
    required Map<String, dynamic> metadata,
  }) {
    return Message.file(
      id: id,
      authorId: authorId,
      createdAt: sentAt.toUtc(),
      source: url.toString(),
      name: fileName,
      status: status,
      metadata: <String, dynamic>{
        ...metadata,
        if (fileName.extension case final extension?) 'attachmentExtension': extension,
        'attachmentFileSize': fileSize,
      },
    );
  }
}

extension on String {
  String? get extension {
    final dot = lastIndexOf('.');
    if (dot < 0 || dot == length - 1) {
      return null;
    }
    final extension = substring(dot + 1).trim().toLowerCase();
    return extension.isEmpty ? null : extension;
  }
}

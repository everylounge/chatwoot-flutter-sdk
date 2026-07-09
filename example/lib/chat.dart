import 'dart:async';

import 'package:chatwoot_sdk/chatwoot_sdk.dart';
import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:example/conversation_page.dart';
import 'package:example/session_storage.dart';
import 'package:flutter/material.dart';
import 'package:shared_preferences/shared_preferences.dart';

const String kSupportUserId = 'chatwoot_support';
const String kSystemUserId = 'chatwoot_system';

String contactUserId(ChatwootContact c) => 'contact_${c.id}';

String? _nonEmpty(String? s) {
  if (s == null || s.trim().isEmpty) return null;
  return s;
}

AuthorizationCreds authorizationCredsFromEnvironment() {
  const identifier = 'CHATWOOT_IDENTIFIER';
  const identifierHash = String.fromEnvironment('CHATWOOT_IDENTIFIER_HASH');
  const name = String.fromEnvironment('CHATWOOT_NAME');
  const email = String.fromEnvironment('CHATWOOT_EMAIL');
  const phone = String.fromEnvironment('CHATWOOT_PHONE_NUMBER');

  return AuthorizationCreds(
    identifier: _nonEmpty(identifier),
    identifierHash: _nonEmpty(identifierHash),
    name: _nonEmpty(name),
    email: _nonEmpty(email),
    phoneNumber: _nonEmpty(phone),
  );
}

bool _hasNetworkConnectivity(List<ConnectivityResult> results) {
  return results.any((result) => result != ConnectivityResult.none);
}

class ChatwootExampleRoot extends StatefulWidget {
  const ChatwootExampleRoot({super.key});

  @override
  State<ChatwootExampleRoot> createState() => _ChatwootExampleRootState();
}

class _ChatwootExampleRootState extends State<ChatwootExampleRoot> {
  late final ChatwootClient _client;

  bool _ready = false;
  Object? _error;

  @override
  void initState() {
    super.initState();
    const inboxIdentifier = String.fromEnvironment('CHATWOOT_INBOX_IDENTIFIER');
    const baseUrlString = String.fromEnvironment('CHATWOOT_BASE_URL');

    final baseUri = baseUrlString.trim().isEmpty ? Uri.parse('https://app.chatwoot.com') : Uri.parse(baseUrlString);
    final connectivity = Connectivity();

    _client = ChatwootClientImpl.withHttpSocket(
      inboxIdentifier: inboxIdentifier,
      baseUrl: baseUri,
      sessionStorage: SharedPreferencesSessionStorage(preferences: SharedPreferencesAsync()),
      defaultCreds: authorizationCredsFromEnvironment(),
      retryPolicy: ChatwootSocketConnectivityRetryPolicy(
        connectivity: connectivity.onConnectivityChanged.map(_hasNetworkConnectivity).distinct(),
        checkConnectivity: () async => _hasNetworkConnectivity(await connectivity.checkConnectivity()),
      ),
    );
    _bootstrap();
  }

  Future<void> _bootstrap() async {
    try {
      await _client.bootstrap();
      if (mounted) {
        setState(() => _ready = true);
      }
    } catch (e) {
      if (mounted) {
        setState(() => _error = e);
      }
    }
  }

  @override
  void dispose() {
    unawaited(_client.dispose());
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    if (_error != null) {
      return Scaffold(
        appBar: AppBar(title: const Text('Error')),
        body: Center(
          child: Padding(
            padding: const EdgeInsets.all(24),
            child: Text('$_error', textAlign: TextAlign.center),
          ),
        ),
      );
    }
    if (!_ready) {
      return const Scaffold(
        body: Center(child: CircularProgressIndicator()),
      );
    }
    return ConversationListPage(client: _client);
  }
}

// ---------------------------------------------------------------------------
// Conversation list
// ---------------------------------------------------------------------------

class ConversationListPage extends StatefulWidget {
  const ConversationListPage({super.key, required this.client});

  final ChatwootClient client;

  @override
  State<ConversationListPage> createState() => _ConversationListPageState();
}

class _ConversationListPageState extends State<ConversationListPage> {
  StreamSubscription<ChatwootState>? _statesSub;
  StreamSubscription<ChatwootConnectionState>? _connectionSub;
  ChatwootConnectionState _connection = const ChatwootConnectionState$Disconnected();

  @override
  void initState() {
    super.initState();
    _statesSub = widget.client.statesStream.listen(_onClientState);
    _connectionSub = widget.client.connectionState.listen((s) {
      if (mounted) setState(() => _connection = s);
    });
  }

  void _onClientState(ChatwootState event) {
    if (!mounted) return;
    switch (event) {
      case ChatwootState$Message$New(:final message):
        switch (message) {
          case ChatwootMessage$Content$Incoming():
            break;
          case ChatwootMessage$Activity():
          case ChatwootMessage$Content$Outgoing():
            return;
        }
        final preview = message.content?.trim();
        final text = preview == null || preview.isEmpty ? 'New message' : preview;
        ScaffoldMessenger.maybeOf(context)?.showSnackBar(
          SnackBar(content: Text(text), duration: const Duration(seconds: 3)),
        );
      case ChatwootState$ConversationsLoaded():
      case ChatwootState$Message$Updated():
      case ChatwootState$Conversation():
      case ChatwootState$Conversation$Deleted():
      case ChatwootState$Conversation$Created():
        return;
    }
  }

  @override
  void dispose() {
    unawaited(_statesSub?.cancel());
    unawaited(_connectionSub?.cancel());
    super.dispose();
  }

  String _statusLabel(ChatwootConversationStatus status) {
    return switch (status) {
      ChatwootConversationStatus.open => 'Open',
      ChatwootConversationStatus.resolved => 'Resolved',
      ChatwootConversationStatus.pending => 'Pending',
      ChatwootConversationStatus.snoozed => 'Snoozed',
    };
  }

  String _connectionLabel() {
    return switch (_connection) {
      ChatwootConnectionState$Connected() => 'Online',
      ChatwootConnectionState$Disconnected() => 'Disconnected',
      ChatwootConnectionState$Reconnecting() => 'Reconnecting...',
    };
  }

  IconData _connectionIcon() {
    return switch (_connection) {
      ChatwootConnectionState$Connected() => Icons.cloud_done_outlined,
      ChatwootConnectionState$Disconnected() => Icons.cloud_off_outlined,
      ChatwootConnectionState$Reconnecting() => Icons.sync,
    };
  }

  String? _lastMessagePreview(ChatwootConversation c) {
    final msgs = c.messages;
    if (msgs.isEmpty) return null;
    final last = msgs.last;
    final t = last.content?.trim();
    if (t != null && t.isNotEmpty) return t;
    if (last.attachments.isNotEmpty) return 'Attachment';
    return null;
  }

  ChatwootConversation? _conversationFromState(ChatwootConversationId id) {
    for (final c in widget.client.state.conversations) {
      if (c.id == id) return c;
    }
    return null;
  }

  Future<void> _openChat(BuildContext context, ChatwootConversation conversation) async {
    await widget.client.markConversationRead(id: conversation.id);
    if (!context.mounted) return;
    final uid = contactUserId(widget.client.contact);
    await Navigator.of(context).push<void>(
      MaterialPageRoute<void>(
        builder: (context) => ConversationChatPage(
          client: widget.client,
          initialConversation: _conversationFromState(conversation.id) ?? conversation,
          currentUserId: uid,
        ),
      ),
    );
    await widget.client.refreshConversations();
  }

  Future<void> _startNewConversation(BuildContext context) async {
    final messenger = ScaffoldMessenger.of(context);
    try {
      final created = await widget.client.createConversation();
      if (!context.mounted) {
        return;
      }
      await _openChat(context, created);
    } catch (e) {
      messenger.showSnackBar(SnackBar(content: Text('Could not create conversation: $e')));
    }
  }

  Future<void> _showProfile(BuildContext context) async {
    final nameCtrl = TextEditingController(text: widget.client.contact.name ?? '');
    final emailCtrl = TextEditingController(text: widget.client.contact.email ?? '');
    final phoneCtrl = TextEditingController(text: widget.client.contact.phoneNumber ?? '');

    final idCtrl = TextEditingController(text: widget.client.contact.identifier ?? '');
    final idHashCtrl = TextEditingController(text: '');
    final switchNameCtrl = TextEditingController(text: '');
    final switchEmailCtrl = TextEditingController(text: '');

    await showModalBottomSheet<void>(
      context: context,
      isScrollControlled: true,
      showDragHandle: true,
      builder: (ctx) {
        return Padding(
          padding: EdgeInsets.only(left: 16, right: 16, bottom: MediaQuery.paddingOf(ctx).bottom + 16),
          child: SingleChildScrollView(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.stretch,
              mainAxisSize: MainAxisSize.min,
              children: [
                Text('Contact', style: Theme.of(ctx).textTheme.titleLarge),
                const SizedBox(height: 8),
                Text('id: ${widget.client.contact.id}', style: Theme.of(ctx).textTheme.bodySmall),
                TextField(controller: nameCtrl, decoration: const InputDecoration(labelText: 'Name')),
                TextField(controller: emailCtrl, decoration: const InputDecoration(labelText: 'Email')),
                TextField(controller: phoneCtrl, decoration: const InputDecoration(labelText: 'Phone')),
                const SizedBox(height: 12),
                FilledButton(
                  onPressed: () async {
                    try {
                      await widget.client.updateContact(
                        name: _nonEmpty(nameCtrl.text),
                        email: _nonEmpty(emailCtrl.text),
                        phoneNumber: _nonEmpty(phoneCtrl.text),
                      );
                      if (ctx.mounted) Navigator.pop(ctx);
                      if (context.mounted) {
                        ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('Contact updated')));
                        setState(() {});
                      }
                    } catch (e) {
                      if (context.mounted) {
                        ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text('Error: $e')));
                      }
                    }
                  },
                  child: const Text('Save contact'),
                ),
                const Divider(height: 32),
                Text('Switch identity (authorize)', style: Theme.of(ctx).textTheme.titleMedium),
                TextField(controller: idCtrl, decoration: const InputDecoration(labelText: 'identifier')),
                TextField(controller: idHashCtrl, decoration: const InputDecoration(labelText: 'identifierHash')),
                TextField(controller: switchNameCtrl, decoration: const InputDecoration(labelText: 'Name (optional)')),
                TextField(
                    controller: switchEmailCtrl, decoration: const InputDecoration(labelText: 'Email (optional)')),
                const SizedBox(height: 8),
                FilledButton.tonal(
                  onPressed: () async {
                    try {
                      await widget.client.authorize(
                        AuthorizationCreds(
                          identifier: _nonEmpty(idCtrl.text),
                          identifierHash: _nonEmpty(idHashCtrl.text),
                          name: _nonEmpty(switchNameCtrl.text),
                          email: _nonEmpty(switchEmailCtrl.text),
                          phoneNumber: null,
                        ),
                      );
                      if (ctx.mounted) Navigator.pop(ctx);
                      if (context.mounted) {
                        ScaffoldMessenger.of(context)
                            .showSnackBar(const SnackBar(content: Text('Authorization completed')));
                        setState(() {});
                      }
                    } catch (e) {
                      if (context.mounted) {
                        ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text('authorize: $e')));
                      }
                    }
                  },
                  child: const Text('authorize(...)'),
                ),
                const SizedBox(height: 8),
                OutlinedButton(
                  onPressed: () async {
                    try {
                      await widget.client.logout();
                      if (ctx.mounted) Navigator.pop(ctx);
                      if (context.mounted) {
                        ScaffoldMessenger.of(context)
                            .showSnackBar(const SnackBar(content: Text('logout(): new session from defaultCreds')));
                        setState(() {});
                      }
                    } catch (e) {
                      if (context.mounted) {
                        ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text('logout: $e')));
                      }
                    }
                  },
                  child: const Text('logout()'),
                ),
              ],
            ),
          ),
        );
      },
    );

    nameCtrl.dispose();
    emailCtrl.dispose();
    phoneCtrl.dispose();
    idCtrl.dispose();
    idHashCtrl.dispose();
    switchNameCtrl.dispose();
    switchEmailCtrl.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final contact = widget.client.contact;

    return Scaffold(
      appBar: AppBar(
        title: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          mainAxisSize: MainAxisSize.min,
          children: [
            const Text('Conversations'),
            Text(
              '${contact.name ?? contact.identifier ?? 'Contact'} · ${_connectionLabel()}',
              style: Theme.of(context).textTheme.labelSmall,
            ),
          ],
        ),
        actions: [
          Icon(_connectionIcon(), color: Theme.of(context).colorScheme.onSurfaceVariant),
          IconButton(
            tooltip: 'Profile and session',
            onPressed: () => _showProfile(context),
            icon: const Icon(Icons.person_outline),
          ),
          IconButton(
            tooltip: 'Refresh',
            onPressed: () async {
              try {
                await widget.client.refreshConversations();
              } catch (e) {
                if (context.mounted) {
                  ScaffoldMessenger.of(context).showSnackBar(
                    SnackBar(content: Text('Refresh failed: $e')),
                  );
                }
              }
            },
            icon: const Icon(Icons.refresh),
          ),
        ],
      ),
      body: StreamBuilder<ChatwootState>(
        stream: widget.client.statesStream,
        initialData: widget.client.state,
        builder: (context, snapshot) {
          final state = snapshot.data;
          if (state == null) {
            return const Center(child: CircularProgressIndicator());
          }
          final list = state.conversations;
          if (list.isEmpty) {
            return const Center(
              child: Padding(
                padding: EdgeInsets.all(24),
                child: Text(
                  'No conversations yet.\nTap "New conversation" below.',
                  textAlign: TextAlign.center,
                ),
              ),
            );
          }
          return ListView.separated(
            itemCount: list.length,
            separatorBuilder: (_, __) => const Divider(height: 1),
            itemBuilder: (context, index) {
              final c = list[index];
              final preview = _lastMessagePreview(c);
              final subtitle = StringBuffer(_statusLabel(c.status));
              if (preview != null) {
                subtitle.write(' · ');
                subtitle.write(preview);
              }
              return ListTile(
                title: Text('Conversation #${c.id}'),
                subtitle: Text(subtitle.toString()),
                trailing: Row(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    if (c.unreadCount > 0)
                      Padding(
                        padding: const EdgeInsets.only(right: 8),
                        child: CircleAvatar(
                          radius: 12,
                          child: Text(
                            '${c.unreadCount}',
                            style: const TextStyle(fontSize: 11),
                          ),
                        ),
                      ),
                    const Icon(Icons.chevron_right),
                  ],
                ),
                onTap: () => _openChat(context, c),
              );
            },
          );
        },
      ),
      floatingActionButton: FloatingActionButton.extended(
        onPressed: () => _startNewConversation(context),
        icon: const Icon(Icons.add_comment_outlined),
        label: const Text('New conversation'),
      ),
    );
  }
}

// ---------------------------------------------------------------------------
// Chat page (flutter_chat_ui)
// ---------------------------------------------------------------------------

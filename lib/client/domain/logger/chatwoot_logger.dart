abstract interface class ChatwootLogger {
  void debug(
    String message, {
    Object? error,
    StackTrace? stackTrace,
    Map<String, Object?> extra = const {},
  });

  void info(
    String message, {
    Object? error,
    StackTrace? stackTrace,
    Map<String, Object?> extra = const {},
  });

  void warning(
    String message, {
    Object? error,
    StackTrace? stackTrace,
    Map<String, Object?> extra = const {},
  });

  void error(
    String message, {
    Object? error,
    StackTrace? stackTrace,
    Map<String, Object?> extra = const {},
  });
}

"""Command handlers for ``valkey-fuzzer``.

Each module registers its subcommands on the main parser, with the handler as
the ``handler`` default. A handler takes the parsed namespace, calls
:class:`~valkey_fuzzer.service.RunService`, prints, and returns an exit code;
compiling, executing, and writing reports happen in the service, and an error
a handler raises is reported once, by :func:`valkey_fuzzer.cli.main`. The exit
codes, and how a run's outcome prints, are in :mod:`valkey_fuzzer.outcomes`.
"""

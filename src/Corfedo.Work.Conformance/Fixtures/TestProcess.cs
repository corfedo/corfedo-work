using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Corfedo.Work.Conformance;

/// <summary>One simulated process: its own container, owner id and hosted work, stopped gracefully or abandoned.</summary>
public sealed class TestProcess : IAsyncDisposable
{
    private readonly ServiceProvider services;
    private readonly IHostedService[] hosted;
    private bool started;

    internal TestProcess(ServiceProvider services, CapturedLogs logs)
    {
        this.services = services;
        Logs = logs;
        hosted = services.GetServices<IHostedService>().ToArray();
    }

    public IServiceProvider Services => services;

    public LeaseOperations Operations => services.GetRequiredService<LeaseOperations>();

    public OwnerId Owner => services.GetRequiredService<WorkProcess>().Owner;

    public CapturedLogs Logs { get; }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var service in hosted)
        {
            await service.StartAsync(cancellationToken);
        }
        started = true;
    }

    /// <summary>Stops gracefully: dispatchers release what they still hold after the grace period.</summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (!started)
        {
            return;
        }
        started = false;
        foreach (var service in hosted.Reverse())
        {
            await service.StopAsync(cancellationToken);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None);
        await services.DisposeAsync();
    }
}

/// <summary>One framework or handler log entry: its event name, formatted message and exception.</summary>
public sealed record CapturedLog(string? Event, LogLevel Level, string Message, Exception? Exception);

/// <summary>Log entries of a process, for scenarios that check what it reports.</summary>
public sealed class CapturedLogs : ILoggerFactory, ILoggerProvider
{
    private readonly ConcurrentQueue<CapturedLog> entries = new();

    /// <summary>Event names of the entries that have one, in order.</summary>
    public IReadOnlyCollection<string> Events => [.. entries.Where(e => e.Event is not null).Select(e => e.Event!)];

    public IReadOnlyCollection<CapturedLog> Entries => entries;

    public void AddProvider(ILoggerProvider provider)
    {
    }

    public ILogger CreateLogger(string categoryName) => new Logger(this);

    public void Dispose()
    {
    }

    private sealed class Logger(CapturedLogs logs) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            logs.entries.Enqueue(new CapturedLog(eventId.Name, logLevel, formatter(state, exception), exception));
    }
}

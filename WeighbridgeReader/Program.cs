// Program.cs
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Hosting.WindowsServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.IO;
using System.IO.Pipes;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.IO.Pipelines;
namespace PontBasculeSystem
{
    // ======================= Options =======================
    public sealed class SimulationSettings { public bool Simulate { get; init; } }
    public sealed class TcpOptions
    {
        public string DeviceIp { get; init; } = GetEnv("PONT_DEVICE_IP", "192.168.0.6");
        public int DevicePort { get; init; } = GetEnv("PONT_DEVICE_PORT", 4001);
        public int ReceiveBufferSize { get; init; } = GetEnv("PONT_RECV_BUF", 8192);
        public int IdleReconnectSeconds { get; init; } = GetEnv("PONT_IDLE_RECONN_SEC", 30);
        public bool EnableTcpKeepAlive { get; init; } = GetEnv("PONT_TCP_KEEPALIVE", true);
        public int KeepAliveTimeMs { get; init; } = GetEnv("PONT_KEEPALIVE_TIME_MS", 15000);
        public int KeepAliveIntervalMs { get; init; } = GetEnv("PONT_KEEPALIVE_INT_MS", 15000);
        public int BackoffMinMs { get; init; } = GetEnv("PONT_BACKOFF_MIN_MS", 1000);
        public int BackoffMaxMs { get; init; } = GetEnv("PONT_BACKOFF_MAX_MS", 5000);
        static string GetEnv(string k, string d) => Environment.GetEnvironmentVariable(k) ?? d;
        static int GetEnv(string k, int d) => int.TryParse(Environment.GetEnvironmentVariable(k), out var v) ? v : d;
        static bool GetEnv(string k, bool d) => bool.TryParse(Environment.GetEnvironmentVariable(k), out var v) ? v : d;
    }
    public sealed class FramingOptions
    {
        public bool UseStxEtx { get; init; } = GetEnv("PONT_USE_STXETX", true);
        public byte Stx { get; init; } = (byte)GetEnv("PONT_STX", 0x02);
        public byte Etx { get; init; } = (byte)GetEnv("PONT_ETX", 0x03);
        public bool UseCr { get; init; } = GetEnv("PONT_USE_CR", true);
        public bool UseLf { get; init; } = GetEnv("PONT_USE_LF", false);
        public int MaxFrameBytes { get; init; } = GetEnv("PONT_MAX_FRAME", 256);
        static bool GetEnv(string k, bool d) => bool.TryParse(Environment.GetEnvironmentVariable(k), out var v) ? v : d;
        static int GetEnv(string k, int d) => int.TryParse(Environment.GetEnvironmentVariable(k), out var v) ? v : d;
    }
    public sealed class OutputOptions
    {
        public string PontId { get; init; } = GetEnv("PONT_ID", "PAB-01");
        public string PipeName { get; init; } = GetEnv("PONT_PIPE", "WeightPipe");
        public int MaxQueued { get; init; } = GetEnv("PONT_MAX_QUEUED", 4096);
        public bool EchoToConsole { get; init; } = GetEnv("PONT_ECHO_CONSOLE", true);
        static string GetEnv(string k, string d) => Environment.GetEnvironmentVariable(k) ?? d;
        static int GetEnv(string k, int d) => int.TryParse(Environment.GetEnvironmentVariable(k), out var v) ? v : d;
        static bool GetEnv(string k, bool d) => bool.TryParse(Environment.GetEnvironmentVariable(k), out var v) ? v : d;
    }
    public sealed class ParserOptions
    {
        public bool ClampRange { get; init; } = GetEnv("PONT_CLAMP", false);
        public decimal MinKg { get; init; } = GetEnv("PONT_MIN_KG", 0m);
        public decimal MaxKg { get; init; } = GetEnv("PONT_MAX_KG", 100_000m);
        static bool GetEnv(string k, bool d) => bool.TryParse(Environment.GetEnvironmentVariable(k), out var v) ? v : d;
        static decimal GetEnv(string k, decimal d) => decimal.TryParse(Environment.GetEnvironmentVariable(k), NumberStyles.Number, CultureInfo.InvariantCulture, out var v) ? v : d;
    }
    // ======================= Entry =======================
    public class Program
    {
        public static async Task Main(string[] args)
        {
            bool simulate = false;
            if (Environment.UserInteractive)
            {
                Console.Write("Use real Ethernet (Moxa) connection? (y/n): ");
                var key = Console.ReadKey().KeyChar;
                Console.WriteLine();
                simulate = !(key == 'y' || key == 'Y');
                Console.WriteLine(simulate
                    ? "Simulation mode: generating dummy weights."
                    : "Real mode: reading from Ethernet (TCP) via Moxa.");
            }
            using var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((ctx, services) =>
                {
                    services.AddSingleton(new SimulationSettings { Simulate = simulate });
                    services.Configure<TcpOptions>(ctx.Configuration.GetSection("Tcp"));
                    services.Configure<FramingOptions>(ctx.Configuration.GetSection("Framing"));
                    services.Configure<OutputOptions>(ctx.Configuration.GetSection("Output"));
                    services.Configure<ParserOptions>(ctx.Configuration.GetSection("Parser"));
                    services.AddHostedService<TcpScaleWorker>();
                })
                .UseWindowsService()
                .UseConsoleLifetime()
                .Build();
            if (Environment.UserInteractive)
            {
                Console.WriteLine("Starting. Press ENTER to stop...");
                await host.StartAsync();
                Console.ReadLine();
                await host.StopAsync();
            }
            else
            {
                await host.RunAsync();
            }
        }
    }
    // ======================= Worker =======================
    public sealed class TcpScaleWorker : BackgroundService
    {
        private readonly ILogger<TcpScaleWorker> _log;
        private readonly SimulationSettings _sim;
        private readonly TcpOptions _tcpOpt;
        private readonly FramingOptions _framing;
        private readonly OutputOptions _outOpt;
        private readonly ParserOptions _parserOpt;
        private readonly Channel<string> _outQueue;
        private readonly Random _rng = new();
        private readonly Meter _meter = new("PontBasculeSystem");
        private readonly Counter<long> _framesRx;
        private readonly Counter<long> _framesParsed;
        private readonly Counter<long> _framesFailed;
        private readonly Counter<long> _bytesRx;
        private readonly Counter<long> _reconnects;
        private readonly Counter<long> _published;
        // Telemetry totals for snapshot logging
        private long _bytesRxTotal, _framesRxTotal, _framesParsedTotal, _framesFailedTotal, _reconnectsTotal, _publishedTotal;
        // Zero-suppression state: suppress only consecutive zeros
        private int _lastSentWasZero = 0; // 0=false, 1=true
        public TcpScaleWorker(
            ILogger<TcpScaleWorker> log,
            IOptions<SimulationSettings> sim,
            IOptions<TcpOptions> tcpOpt,
            IOptions<FramingOptions> framing,
            IOptions<OutputOptions> outOpt,
            IOptions<ParserOptions> parserOpt)
        {
            _log = log;
            _sim = sim.Value;
            _tcpOpt = tcpOpt.Value;
            _framing = framing.Value;
            _outOpt = outOpt.Value;
            _parserOpt = parserOpt.Value;
            _outQueue = Channel.CreateBounded<string>(new BoundedChannelOptions(Math.Max(128, _outOpt.MaxQueued))
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = false,
                SingleWriter = false
            });
            _framesRx = _meter.CreateCounter<long>("frames.rx");
            _framesParsed = _meter.CreateCounter<long>("frames.parsed");
            _framesFailed = _meter.CreateCounter<long>("frames.failed");
            _bytesRx = _meter.CreateCounter<long>("bytes.rx");
            _reconnects = _meter.CreateCounter<long>("tcp.reconnects");
            _published = _meter.CreateCounter<long>("frames.published");
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _ = Task.Run(() => PipeBroadcastLoop(stoppingToken), stoppingToken);
            _ = Task.Run(() => TelemetryLoop(stoppingToken), stoppingToken);
            if (_sim.Simulate)
            {
                _log.LogInformation("Simulation enabled: producing weights every 1s");
                await SimulationLoop(stoppingToken);
                return;
            }
            await TcpLoop(stoppingToken);
        }
        // ------------------- Simulation -------------------
        private async Task SimulationLoop(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                var zero = _rng.NextDouble() < 0.1;
                var w = zero ? 0m : 1250m + (decimal)(_rng.NextDouble() * 20 - 10);
                ProcessReading(w, "[sim]");
                await Task.Delay(1000, ct);
            }
        }
        // ------------------- TCP + Pipelines -------------------
        private async Task TcpLoop(CancellationToken ct)
        {
            int attempt = 0;
            while (!ct.IsCancellationRequested)
            {
                TcpClient? tcp = null;
                NetworkStream? stream = null;
                PipeReader? reader = null;
                try
                {
                    tcp = new TcpClient()
                    {
                        NoDelay = true,
                        ReceiveBufferSize = _tcpOpt.ReceiveBufferSize,
                        SendBufferSize = 1024
                    };
                    if (_tcpOpt.EnableTcpKeepAlive)
                        TryConfigureKeepAlive(tcp, _tcpOpt.KeepAliveTimeMs, _tcpOpt.KeepAliveIntervalMs);
                    _log.LogInformation("Connecting to {ip}:{port} ...", _tcpOpt.DeviceIp, _tcpOpt.DevicePort);
                    using (ct.Register(() => SafeClose(tcp)))
                    {
                        await tcp.ConnectAsync(_tcpOpt.DeviceIp, _tcpOpt.DevicePort);
                    }
                    attempt = 0;
                    _log.LogInformation("Connected to {ip}:{port}", _tcpOpt.DeviceIp, _tcpOpt.DevicePort);
                    stream = tcp.GetStream();
                    reader = PipeReader.Create(stream, new StreamPipeReaderOptions(bufferSize: _tcpOpt.ReceiveBufferSize));
                    var lastData = Stopwatch.StartNew();
                    while (!ct.IsCancellationRequested)
                    {
                        var result = await reader.ReadAsync(ct);
                        var buffer = result.Buffer;
                        if (buffer.Length > 0) lastData.Restart();
                        _bytesRx.Add((long)buffer.Length);
                        Interlocked.Add(ref _bytesRxTotal, (long)buffer.Length);
                        while (TryReadFrame(ref buffer, out ReadOnlySequence<byte> frame))
                        {
                            _framesRx.Add(1);
                            Interlocked.Increment(ref _framesRxTotal);
                            if (TryParseWeight(frame, out var weight))
                            {
                                _framesParsed.Add(1);
                                Interlocked.Increment(ref _framesParsedTotal);
                                ProcessReading(weight, "[tcp]");
                            }
                            else
                            {
                                _framesFailed.Add(1);
                                var failed = Interlocked.Increment(ref _framesFailedTotal);
                                if ((failed % 100) == 0)
                                    _log.LogDebug("Parse failed for frame (len {len})", frame.Length);
                            }
                        }
                        reader.AdvanceTo(buffer.Start, buffer.End);
                        if (result.IsCompleted || result.IsCanceled) break;
                        if (lastData.Elapsed.TotalSeconds > _tcpOpt.IdleReconnectSeconds)
                        {
                            _log.LogWarning("No data for {secs}s — reconnecting.", _tcpOpt.IdleReconnectSeconds);
                            break;
                        }
                    }
                }
                catch (OperationCanceledException) { /* shutting down */ }
                catch (Exception ex)
                {
                    _log.LogError(ex, "TCP loop error");
                }
                finally
                {
                    if (reader is not null) await reader.CompleteAsync();
                    stream?.Dispose();
                    SafeClose(tcp);
                    _reconnects.Add(1);
                    Interlocked.Increment(ref _reconnectsTotal);
                }
                if (!ct.IsCancellationRequested)
                {
                    attempt++;
                    var delay = BackoffWithJitter(attempt, _tcpOpt.BackoffMinMs, _tcpOpt.BackoffMaxMs, _rng);
                    _log.LogInformation("Reconnecting in {ms} ms ...", delay);
                    await Task.Delay(delay, ct);
                }
            }
        }
        private static void SafeClose(TcpClient? tcp)
        {
            try { tcp?.Close(); tcp?.Dispose(); } catch { /* ignore */ }
        }
        private static int BackoffWithJitter(int attempt, int minMs, int maxMs, Random rng)
        {
            var exp = Math.Min(maxMs, minMs * (int)Math.Pow(2, Math.Min(10, attempt)));
            var jitter = rng.Next(minMs, exp + 1);
            return Math.Clamp(jitter, minMs, maxMs);
        }
        // ------------------- Framing -------------------
        private bool TryReadFrame(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> frame)
        {
            var reader = new SequenceReader<byte>(buffer);
            // STX/ETX framing
            if (_framing.UseStxEtx)
            {
                if (reader.TryAdvanceTo(_framing.Stx, advancePastDelimiter: true))
                {
                    var payloadStart = reader.Position;
                    if (reader.TryAdvanceTo(_framing.Etx, advancePastDelimiter: false))
                    {
                        var payloadEnd = reader.Position;
                        var len = buffer.Slice(payloadStart, payloadEnd).Length;
                        reader.Advance(1); // past ETX
                        if (len > 0 && len <= _framing.MaxFrameBytes)
                        {
                            frame = buffer.Slice(payloadStart, payloadEnd);
                            buffer = buffer.Slice(reader.Position);
                            return true;
                        }
                        buffer = buffer.Slice(reader.Position);
                        frame = default;
                        return false;
                    }
                    frame = default;
                    return false; // wait for more data
                }
            }
            // CR/LF terminator
            var termFound = false;
            while (!reader.End)
            {
                byte b = reader.CurrentSpan[reader.CurrentSpanIndex];
                if ((_framing.UseCr && b == (byte)'\r') || (_framing.UseLf && b == (byte)'\n'))
                {
                    termFound = true;
                    break;
                }
                reader.Advance(1);
            }
            if (termFound)
            {
                var payload = buffer.Slice(0, reader.Position);
                reader.Advance(1); // skip terminator
                if (_framing.UseCr && _framing.UseLf && !reader.End)
                {
                    if (reader.TryPeek(out byte next) && next == (byte)'\n')
                        reader.Advance(1);
                }
                if (payload.Length > 0 && payload.Length <= _framing.MaxFrameBytes)
                {
                    frame = payload;
                    buffer = buffer.Slice(reader.Position);
                    return true;
                }
                buffer = buffer.Slice(reader.Position);
                frame = default;
                return false;
            }
            frame = default;
            return false;
        }
        // ------------------- Parsing -------------------
        private bool TryParseWeight(in ReadOnlySequence<byte> frame, out decimal weight)
        {
            Span<int> starts = stackalloc int[16];
            Span<int> ends = stackalloc int[16];
            int tokCount = 0;
            var seq = frame;
            var reader = new SequenceReader<byte>(seq);
            int offset = 0;
            bool inTok = false, seenSep = false;
            int tokStart = 0;
            while (!reader.End && tokCount < starts.Length)
            {
                byte b = reader.CurrentSpan[reader.CurrentSpanIndex];
                bool isDigit = b >= (byte)'0' && b <= (byte)'9';
                bool isSign = (b == (byte)'+' || b == (byte)'-');
                bool isSep = (b == (byte)'.' || b == (byte)',');
                if (!inTok)
                {
                    if (isDigit || isSign)
                    {
                        inTok = true;
                        seenSep = false;
                        tokStart = offset;
                    }
                }
                else
                {
                    if (isDigit) { /* keep */ }
                    else if (isSep && !seenSep) { seenSep = true; }
                    else
                    {
                        starts[tokCount] = tokStart;
                        ends[tokCount] = offset;
                        tokCount++;
                        inTok = false;
                        seenSep = false;
                    }
                }
                reader.Advance(1);
                offset++;
            }
            if (inTok && tokCount < starts.Length)
            {
                starts[tokCount] = tokStart;
                ends[tokCount] = offset;
                tokCount++;
            }
            if (tokCount == 0)
            {
                weight = 0m;
                return false;
            }
            var ascii = Encoding.ASCII;
            string[] tokens = new string[tokCount];
            int[] digits = new int[tokCount];
            for (int i = 0; i < tokCount; i++)
            {
                var slice = frame.Slice(starts[i], ends[i] - starts[i]);
                tokens[i] = ascii.GetString(slice.ToArray());
                digits[i] = CountDigits(tokens[i]);
            }
            // Rule 1: 3 tokens, first & last short (<=2 digits) => middle token is weight
            if (tokCount == 3 && digits[0] <= 2 && digits[2] <= 2)
            {
                if (TryParseDecimal(tokens[1], out weight))
                    return Clamp(weight);
            }
            // Rule 2: token with most digits (ties -> larger |value|)
            decimal bestVal = 0m; int bestDigits = -1; bool haveBest = false;
            for (int i = 0; i < tokCount; i++)
            {
                if (!TryParseDecimal(tokens[i], out var val)) continue;
                int d = digits[i];
                if (!haveBest || d > bestDigits || (d == bestDigits && Math.Abs(val) > Math.Abs(bestVal)))
                {
                    bestVal = val; bestDigits = d; haveBest = true;
                }
            }
            if (haveBest)
            {
                weight = bestVal;
                return Clamp(weight);
            }
            weight = 0m;
            return false;
            bool Clamp(decimal w)
            {
                if (!_parserOpt.ClampRange) return true;
                return (w >= _parserOpt.MinKg && w <= _parserOpt.MaxKg);
            }
            static int CountDigits(string s)
            {
                int c = 0;
                foreach (var ch in s)
                    if (ch >= '0' && ch <= '9') c++;
                return c;
            }
            static bool TryParseDecimal(string s, out decimal d)
                => decimal.TryParse(s.Replace(',', '.'), NumberStyles.Number, CultureInfo.InvariantCulture, out d);
        }
        // ------------------- Process & Publish -------------------
        // Suppress only consecutive zeros
        private void ProcessReading(decimal weight, string rawHint)
        {
            _log.LogInformation("Reading: {Weight:F2} kg | raw: {raw}", weight, rawHint);
            bool isZero = (weight == 0m);
            if (isZero && Interlocked.CompareExchange(ref _lastSentWasZero, 1, 1) == 1)
                return; // suppress duplicate zero
            Volatile.Write(ref _lastSentWasZero, isZero ? 1 : 0);
            PublishWeight(weight);
        }
        private void PublishWeight(decimal weight)
        {
            var s = weight.ToString("F2", CultureInfo.InvariantCulture);
            if (_outOpt.EchoToConsole && Environment.UserInteractive)
                Console.WriteLine($"{_outOpt.PontId}:{s}");
            _outQueue.Writer.TryWrite(s);
            _published.Add(1);
            Interlocked.Increment(ref _publishedTotal);
        }
        // ------------------- Named Pipe Broadcast -------------------
        private async Task PipeBroadcastLoop(CancellationToken ct)
        {
            var clients = new ConcurrentDictionary<int, (NamedPipeServerStream pipe, StreamWriter writer)>();
            int nextId = 0;
            // Accept loop
            _ = Task.Run(async () =>
            {
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var pipe = new NamedPipeServerStream(
                            _outOpt.PipeName,
                            PipeDirection.Out,
                            NamedPipeServerStream.MaxAllowedServerInstances,
                            PipeTransmissionMode.Byte,
                            System.IO.Pipes.PipeOptions.Asynchronous  // disambiguated
                        );
                        await pipe.WaitForConnectionAsync(ct);
                        var writer = new StreamWriter(pipe) { AutoFlush = true };
                        int id = Interlocked.Increment(ref nextId);
                        clients[id] = (pipe, writer);
                        _log.LogInformation("Pipe client connected (#{id}). Total: {count}", id, clients.Count);
                        _ = Task.Run(async () =>
                        {
                            try
                            {
                                while (!ct.IsCancellationRequested && pipe.IsConnected)
                                    await Task.Delay(1000, ct);
                            }
                            catch { /* ignore */ }
                            finally
                            {
                                if (clients.TryRemove(id, out var p))
                                {
                                    try { p.writer.Dispose(); } catch { }
                                    try { p.pipe.Dispose(); } catch { }
                                    _log.LogInformation("Pipe client disconnected (#{id}). Total: {count}", id, clients.Count);
                                }
                            }
                        }, ct);
                    }
                    catch (OperationCanceledException) { break; }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Pipe accept error");
                        await Task.Delay(500, ct);
                    }
                }
            }, ct);
            // Broadcast loop
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    var msg = await _outQueue.Reader.ReadAsync(ct);
                    foreach (var kv in clients)
                    {
                        try
                        {
                            if (kv.Value.pipe.IsConnected)
                                await kv.Value.writer.WriteLineAsync(msg);
                        }
                        catch
                        {
                            // Drop on write errors; cleanup task will remove it
                        }
                    }
                }
            }
            catch (OperationCanceledException) { /* shutting down */ }
        }
        // ------------------- Telemetry -------------------
        private async Task TelemetryLoop(CancellationToken ct)
        {
            long lastBytes = 0, lastParsed = 0, lastPub = 0, lastRx = 0;
            var sw = Stopwatch.StartNew();
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(5000, ct);
                    var curBytes = Interlocked.Read(ref _bytesRxTotal);
                    var curParsed = Interlocked.Read(ref _framesParsedTotal);
                    var curPub = Interlocked.Read(ref _publishedTotal);
                    var curRx = Interlocked.Read(ref _framesRxTotal);
                    var dt = Math.Max(0.001, sw.Elapsed.TotalSeconds);
                    sw.Restart();
                    var bps = (curBytes - lastBytes) / dt;
                    var fps = (curParsed - lastParsed) / dt;
                    var rps = (curRx - lastRx) / dt;
                    var pps = (curPub - lastPub) / dt;
                    lastBytes = curBytes;
                    lastParsed = curParsed;
                    lastPub = curPub;
                    lastRx = curRx;
                    _log.LogInformation("Traffic: {rps:F0} frames/s in, {fps:F0} parsed/s, {pps:F0} published/s, {kbps:F1} KB/s",
                        rps, fps, pps, bps / 1024.0);
                }
                catch (OperationCanceledException) { break; }
            }
        }
        // ------------------- TCP KeepAlive -------------------
        private static void TryConfigureKeepAlive(TcpClient tcp, int timeMs, int intervalMs)
        {
            try
            {
                tcp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    const int SIO_KEEPALIVE_VALS = unchecked((int)0x98000004);
                    var inOptionValues = new byte[12];
                    BitConverter.GetBytes((uint)1).CopyTo(inOptionValues, 0);
                    BitConverter.GetBytes((uint)timeMs).CopyTo(inOptionValues, 4);
                    BitConverter.GetBytes((uint)intervalMs).CopyTo(inOptionValues, 8);
                    tcp.Client.IOControl(SIO_KEEPALIVE_VALS, inOptionValues, null);
                }
            }
            catch
            {
                // Non-fatal
            }
        }
    }
}
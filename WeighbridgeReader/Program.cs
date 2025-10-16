// Program.cs
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace PontBasculeSystem
{
    /// <summary>
    /// Windows service / console app that reads weighbridge data over TCP
    /// and logs raw + converted representations. No SignalR, no serial.
    /// </summary>
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var builder = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    services.AddHostedService<TcpReaderWorker>();
                })
                .UseWindowsService()
                .UseConsoleLifetime();

            var host = builder.Build();

            if (Environment.UserInteractive)
            {
                Console.WriteLine("TCP Weighbridge reader starting. Press ENTER to stop…");
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

    /// <summary>
    /// Connects to the weighbridge over TCP and logs everything received.
    /// Frames are detected using STX..ETX or CR/LF when present.
    /// </summary>
    public class TcpReaderWorker : BackgroundService
    {
        private readonly ILogger<TcpReaderWorker> _logger;

        // ==== CONFIG ====
        // You can also override via env vars TCP_HOST / TCP_PORT if needed.
        private readonly string _host =
            Environment.GetEnvironmentVariable("TCP_HOST") ?? "10.116.136.22";
        private readonly int _port =
            int.TryParse(Environment.GetEnvironmentVariable("TCP_PORT"), out var p) ? p : 4001;

        private readonly string _logFilePath;

        // Buffer for frame extraction across chunk boundaries
        private readonly List<byte> _accum = new(capacity: 8192);

        public TcpReaderWorker(ILogger<TcpReaderWorker> logger)
        {
            _logger = logger;
            _logFilePath = Path.Combine(AppContext.BaseDirectory, "TcpReader.log");
            Directory.CreateDirectory(Path.GetDirectoryName(_logFilePath)!);
        }

        protected override async Task ExecuteAsync(CancellationToken token)
        {
            _logger.LogInformation("Target endpoint: {Host}:{Port}", _host, _port);
            LogMessage($"=== START {DateTime.Now:yyyy-MM-dd HH:mm:ss} ({_host}:{_port}) ===");

            var backoff = TimeSpan.FromSeconds(1);

            while (!token.IsCancellationRequested)
            {
                using var client = new TcpClient();
                try
                {
                    var ip = IPAddress.TryParse(_host, out var parsed) ? parsed : null;
                    if (ip == null)
                    {
                        var ips = await Dns.GetHostAddressesAsync(_host);
                        ip = ips.FirstOrDefault(a => a.AddressFamily == AddressFamily.InterNetwork) ?? ips.First();
                    }

                    _logger.LogInformation("Connecting to {IP}:{Port}…", ip, _port);
                    using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
                    cts.CancelAfter(TimeSpan.FromSeconds(10));
                    await client.ConnectAsync(ip!, _port, cts.Token);
                    client.NoDelay = true;

                    _logger.LogInformation("Connected.");
                    LogMessage($"Connected to {ip}:{_port}");

                    backoff = TimeSpan.FromSeconds(1); // reset backoff
                    await ReadLoop(client, token);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    // shutting down
                }
                catch (Exception ex)
                {
                    LogError("Socket error / connect/read failed", ex);
                    _logger.LogWarning("Disconnected. Reconnecting in {Delay}s…", backoff.TotalSeconds);
                    await Task.Delay(backoff, token);
                    // Exponential backoff up to ~30s
                    backoff = TimeSpan.FromSeconds(Math.Min(backoff.TotalSeconds * 2, 30));
                }
            }

            LogMessage($"=== STOP {DateTime.Now:yyyy-MM-dd HH:mm:ss} ===");
        }

        private async Task ReadLoop(TcpClient client, CancellationToken token)
        {
            using var stream = client.GetStream();
            var buf = new byte[4096];

            while (!token.IsCancellationRequested)
            {
                int n;
                try
                {
                    n = await stream.ReadAsync(buf.AsMemory(0, buf.Length), token);
                }
                catch (IOException ioEx)
                {
                    LogError("Read failed (IO)", ioEx);
                    break;
                }
                catch (ObjectDisposedException)
                {
                    break;
                }

                if (n <= 0)
                {
                    _logger.LogInformation("Remote closed the connection.");
                    break;
                }

                var chunk = buf.AsSpan(0, n).ToArray();

                // Log the received chunk as-is in multiple representations
                LogConversions(chunk, source: "CHUNK");

                // Accumulate for frame extraction (STX..ETX or line-based)
                _accum.AddRange(chunk);
                TryExtractAndLogFrames();
            }
        }

        // ===== Frame extraction =====
        private void TryExtractAndLogFrames()
        {
            // Repeatedly pop frames from _accum if we can find a delimiter
            while (true)
            {
                if (_accum.Count == 0) return;

                // Prefer STX..ETX framing if present
                int stx = _accum.IndexOf((byte)0x02);
                if (stx >= 0)
                {
                    int etx = _accum.IndexOf((byte)0x03, stx + 1);
                    if (etx < 0) break; // wait for more data

                    var frameLen = etx - stx - 1;
                    if (frameLen < 0)
                    {
                        // corrupt, drop up to etx
                        _accum.RemoveRange(0, etx + 1);
                        continue;
                    }

                    var frame = _accum.GetRange(stx + 1, frameLen).ToArray();
                    // remove everything up to and including ETX
                    _accum.RemoveRange(0, etx + 1);
                    LogConversions(frame, source: "FRAME STX..ETX");
                    continue;
                }

                // Otherwise, try line-based (CR or LF)
                int cr = _accum.IndexOf((byte)0x0D);
                int lf = _accum.IndexOf((byte)0x0A);
                int term = (cr >= 0 && lf >= 0) ? Math.Min(cr, lf) : (cr >= 0 ? cr : lf);

                if (term < 0) break; // no full line yet

                // Extract up to terminator
                var line = _accum.GetRange(0, term).ToArray();

                // Drop the terminator and also swallow CRLF or LFCR when present
                int drop = term + 1;
                if (term + 1 < _accum.Count)
                {
                    byte a = _accum[term];
                    byte b = _accum[term + 1];
                    if ((a == 0x0D && b == 0x0A) || (a == 0x0A && b == 0x0D))
                        drop++;
                }
                _accum.RemoveRange(0, drop);

                LogConversions(line, source: "FRAME LINE");
            }
        }

        // ===== Logging helpers =====
        private void LogConversions(byte[] data, string source)
        {
            // HEX (space-separated)
            string hex = data.Length == 0 ? "" : BitConverter.ToString(data).Replace("-", " ");

            // DEC (space-separated, zero-padded 3 digits)
            string dec = data.Length == 0 ? "" : string.Join(" ", data.Select(b => b.ToString("D3")));

            // ASCII printables (replace control with '.')
            string ascii = new string(data.Select(b => b >= 32 && b <= 126 ? (char)b : '.').ToArray());

            // UTF8 best-effort (may contain replacement chars if invalid)
            string utf8 = Encoding.UTF8.GetString(data);

            // Mixed escaped: printables as-is, control bytes escaped / tagged
            var sbEsc = new StringBuilder(data.Length * 4);
            foreach (var b in data)
            {
                switch (b)
                {
                    case 0x02: sbEsc.Append("<STX>"); break;
                    case 0x03: sbEsc.Append("<ETX>"); break;
                    case 0x0D: sbEsc.Append("<CR>"); break;
                    case 0x0A: sbEsc.Append("<LF>"); break;
                    default:
                        if (b >= 32 && b <= 126) sbEsc.Append((char)b);
                        else sbEsc.Append($"\\x{b:X2}");
                        break;
                }
            }
            string mixed = sbEsc.ToString();

            var ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            var log =
$@"[{ts}] {source}: {data.Length} byte(s)
HEX   : {hex}
DEC   : {dec}
ASCII : {ascii}
UTF8  : {utf8}
MIXED : {mixed}
";
            LogMessage(log);
            _logger.LogInformation("{Source} {Count}B | ASCII Preview: {Preview}",
                source, data.Length, ascii.Length > 120 ? ascii[..120] + "…" : ascii);
        }

        private void LogError(string msg, Exception ex)
        {
            var ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            File.AppendAllText(_logFilePath, $"[{ts}] ERROR: {msg}\n{ex}\n");
            _logger.LogError(ex, msg);
        }

        private void LogMessage(string msg)
        {
            var ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            File.AppendAllText(_logFilePath, $"[{ts}] {msg}\n");
        }
    }
}

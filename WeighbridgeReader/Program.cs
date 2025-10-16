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
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var builder = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    services.AddHostedService<WeighbridgeWorker>();
                })
                .UseWindowsService()
                .UseConsoleLifetime();

            var host = builder.Build();
            if (Environment.UserInteractive)
            {
                Console.WriteLine("Weighbridge reader starting. Press Ctrl+C to stop.");
                await host.RunAsync();
            }
            else
            {
                await host.RunAsync();
            }
        }
    }

    /// <summary>
    /// Minimal, no-noise reader:
    /// - Connects (or listens) TCP.
    /// - Optionally sends a poll command periodically.
    /// - Parses and PRINTS weights only, like the bridge display.
    /// - Logs parsed weights to Weight.log.
    /// - (Optional) mirrors raw bytes to Raw.log if LOG_RAW=true.
    /// </summary>
    public class WeighbridgeWorker : BackgroundService
    {
        private readonly ILogger<WeighbridgeWorker> _logger;

        // Mode
        private readonly string _mode = (Environment.GetEnvironmentVariable("TCP_MODE") ?? "client")
            .Trim().ToLowerInvariant();

        // Client
        private readonly string _host = Environment.GetEnvironmentVariable("TCP_HOST") ?? "10.116.136.22";
        private readonly int _port = int.TryParse(Environment.GetEnvironmentVariable("TCP_PORT"), out var p) ? p : 4001;

        // Server
        private readonly string _listenAddr = Environment.GetEnvironmentVariable("TCP_LISTEN_ADDR") ?? "0.0.0.0";
        private readonly int _listenPort = int.TryParse(Environment.GetEnvironmentVariable("TCP_LISTEN_PORT"), out var lp) ? lp : 4001;

        // Poll command
        private readonly string _pollCmd = Environment.GetEnvironmentVariable("POLL_CMD") ?? "/r t";
        private readonly int _pollIntervalMs = int.TryParse(Environment.GetEnvironmentVariable("POLL_INTERVAL_MS"), out var ms) ? ms : 1000;

        // Hex frame decode
        private readonly int _wordIndex = int.TryParse(Environment.GetEnvironmentVariable("HEX_WEIGHT_WORD_INDEX"), out var wi) ? wi : 1; // 0=hi,1=lo
        private readonly int _divisor = Math.Max(1, int.TryParse(Environment.GetEnvironmentVariable("HEX_WEIGHT_DIVISOR"), out var dv) ? dv : 1);

        // Raw mirror
        private readonly bool _logRaw = string.Equals(Environment.GetEnvironmentVariable("LOG_RAW"), "true", StringComparison.OrdinalIgnoreCase);

        private readonly string _weightLog = Path.Combine(AppContext.BaseDirectory, "Weight.log");
        private readonly string _rawLog = Path.Combine(AppContext.BaseDirectory, "Raw.log");

        // Simple parsers
        private readonly HexFrameParser _hex = new();
        private readonly LineAsciiParser _line = new();

        // Connection
        private readonly object _connLock = new();
        private TcpClient? _client;
        private NetworkStream? _stream;

        // ASCII selection config
        private readonly int _asciiMin = int.TryParse(Environment.GetEnvironmentVariable("ASCII_MIN_DIGITS"), out var amin) ? amin : 5;
        private readonly int _asciiMax = int.TryParse(Environment.GetEnvironmentVariable("ASCII_MAX_DIGITS"), out var amax) ? amax : 6;
        private readonly string _asciiPick = (Environment.GetEnvironmentVariable("ASCII_PICK") ?? "last").Trim().ToLowerInvariant();
        private readonly int _asciiIndex = int.TryParse(Environment.GetEnvironmentVariable("ASCII_INDEX"), out var aidx) ? aidx : 0;

        // Scaling
        private readonly decimal _scaleNum = decimal.TryParse(Environment.GetEnvironmentVariable("SCALE_NUM"), out var sn) ? sn : 1m;
        private readonly decimal _scaleDen = decimal.TryParse(Environment.GetEnvironmentVariable("SCALE_DEN"), out var sd) ? (sd == 0 ? 1m : sd) : 1m;
        private readonly decimal _offset = decimal.TryParse(Environment.GetEnvironmentVariable("OFFSET"), out var of) ? of : 0m;
        private readonly string _hexMode = (Environment.GetEnvironmentVariable("HEX_MODE") ?? "lo").Trim().ToLowerInvariant();

        public WeighbridgeWorker(ILogger<WeighbridgeWorker> logger)
        {
            _logger = logger;
            Directory.CreateDirectory(AppContext.BaseDirectory);
        }

        protected override async Task ExecuteAsync(CancellationToken token)
        {
            if (_mode == "server")
                await RunServerAsync(token);
            else
                await RunClientAsync(token);
        }

        // ============ Client ============
        private async Task RunClientAsync(CancellationToken token)
        {
            _logger.LogInformation("Client → {Host}:{Port}", _host, _port);

            var backoff = TimeSpan.FromSeconds(1);

            while (!token.IsCancellationRequested)
            {
                using var client = new TcpClient();
                try
                {
                    var ip = IPAddress.TryParse(_host, out var parsed) ? parsed : (await Dns.GetHostAddressesAsync(_host)).First();
                    await client.ConnectAsync(ip, _port, token);
                    client.NoDelay = true;
                    client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);

                    lock (_connLock)
                    {
                        _client = client;
                        _stream = client.GetStream();
                    }

                    _logger.LogInformation("Connected.");
                    var pollCts = new CancellationTokenSource();
                    var pollTask = StartPollingAsync(pollCts.Token);

                    await ReadStreamAsync(client.GetStream(), token);

                    pollCts.Cancel();
                    await Task.WhenAny(pollTask, Task.Delay(100));
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested) { }
                catch (Exception ex)
                {
                    _logger.LogWarning("Disconnected: {Msg}", ex.Message);
                    await Task.Delay(backoff, token);
                    backoff = TimeSpan.FromSeconds(Math.Min(backoff.TotalSeconds * 2, 30));
                }
                finally
                {
                    lock (_connLock)
                    {
                        try { _stream?.Dispose(); } catch { }
                        try { _client?.Dispose(); } catch { }
                        _stream = null;
                        _client = null;
                    }
                }
            }
        }

        // ============ Server ============
        private async Task RunServerAsync(CancellationToken token)
        {
            IPAddress listenIp = _listenAddr == "0.0.0.0" ? IPAddress.Any :
                                 (IPAddress.TryParse(_listenAddr, out var ip) ? ip : IPAddress.Any);
            var listener = new TcpListener(listenIp, _listenPort);
            listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            listener.Start();
            _logger.LogInformation("Server listening on {Addr}:{Port}", listenIp, _listenPort);

            try
            {
                while (!token.IsCancellationRequested)
                {
                    var client = await listener.AcceptTcpClientAsync(token);
                    client.NoDelay = true;
                    client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                    var remote = client.Client.RemoteEndPoint?.ToString();
                    _logger.LogInformation("Accepted {Remote}", remote);

                    lock (_connLock)
                    {
                        _client = client;
                        _stream = client.GetStream();
                    }

                    var pollCts = new CancellationTokenSource();
                    var pollTask = StartPollingAsync(pollCts.Token);

                    try { await ReadStreamAsync(client.GetStream(), token); }
                    finally
                    {
                        pollCts.Cancel();
                        await Task.WhenAny(pollTask, Task.Delay(100));
                        lock (_connLock)
                        {
                            try { _stream?.Dispose(); } catch { }
                            try { _client?.Dispose(); } catch { }
                            _stream = null;
                            _client = null;
                        }
                        _logger.LogInformation("Closed {Remote}", remote);
                    }
                }
            }
            finally
            {
                try { listener.Stop(); } catch { }
            }
        }

        // ============ Read + Parse ============
        private async Task ReadStreamAsync(NetworkStream stream, CancellationToken token)
        {
            var buf = new byte[4096];
            while (!token.IsCancellationRequested)
            {
                int n;
                try { n = await stream.ReadAsync(buf.AsMemory(0, buf.Length), token); }
                catch (IOException) { break; }
                catch (ObjectDisposedException) { break; }
                if (n <= 0) break;

                ProcessBuffer(buf, n);
            }
        }
        private void ProcessBuffer(byte[] buffer, int count)
        {
            if (_logRaw) AppendRaw(new ReadOnlySpan<byte>(buffer, 0, count));

            for (int i = 0; i < count; i++)
            {
                byte b = buffer[i];

                // STX..ETX hex path (kept)
                if (_hex.Feed(b, out string? hexPayload))
                {
                    if (TryDecodeHexWeight(hexPayload, out decimal h))
                        PrintWeight(ApplyScale(h));
                    continue;
                }

                // ASCII line path
                /*if (_line.Feed(b, out string? line))
                {
                    if (TryParseAsciiWeight(line, out decimal a))
                        PrintWeight(ApplyScale(a));
                }*/
            }
        }
        private void PrintWeight(decimal weight)
        {
            var s = weight.ToString("0.###", System.Globalization.CultureInfo.InvariantCulture);
            Console.WriteLine(s);
            File.AppendAllText(_weightLog, $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} {s}\n");
        }

        //private bool TryDecodeHexWeight(string hexPayload, out decimal weight)
        //{
        //    weight = 0m;

        //    // Sanitize to 8 hex chars (4 bytes)
        //    string hex = new string(hexPayload.Where(Uri.IsHexDigit).ToArray());
        //    if (hex.Length < 8) return false;
        //    hex = hex.Substring(0, 8);

        //    // Bytes: b0 b1 b2 b3
        //    byte b0 = Convert.ToByte(hex.Substring(0, 2), 16);
        //    byte b1 = Convert.ToByte(hex.Substring(2, 2), 16);
        //    byte b2 = Convert.ToByte(hex.Substring(4, 2), 16);
        //    byte b3 = Convert.ToByte(hex.Substring(6, 2), 16);

        //    // Fast path: frames like F2 C? ?? E2
        //    // Weight digits are the nibbles of b1 (hi,lo) and b2 (hi,lo)
        //    int n1 = (b1 >> 4) & 0xF; // thousands
        //    int n2 = (b1 >> 0) & 0xF; // hundreds
        //    int n3 = (b2 >> 4) & 0xF; // tens
        //    int n4 = (b2 >> 0) & 0xF; // ones-of-tens (we multiply by 10 at the end)

        //    // Position-specific nibble→digit maps (derived from your samples)
        //    int d1 = n1 == 0xC ? 1 : -1; // 'C' → 1 (thousands)
        //    int d2 = n2 switch          // hundreds
        //    {
        //        0xB => 4,
        //        0xF => 5,
        //        0xC => 3,
        //        _ => -1
        //    };
        //    int d3 = n3 switch          // tens
        //    {
        //        0xA => 2,
        //        0xF => 4,
        //        0x5 => 7,
        //        _ => -1
        //    };
        //    int d4 = n4 switch          // ones-of-tens
        //    {
        //        0xB => 8,
        //        0x9 => 4,
        //        0x8 => 8,
        //        _ => -1
        //    };

        //    // If we can’t map, bail (so we don’t print garbage)
        //    if (d1 < 0 || d2 < 0 || d3 < 0 || d4 < 0)
        //        return false;

        //    // Build the 4-digit number and scale to kg (all your examples end with 0)
        //    int value = (d1 * 1000) + (d2 * 100) + (d3 * 10) + d4; // e.g., 1428
        //    weight = value * 10m;                                   // 14280
        //    _logger.LogInformation("Weight decoded via Hex Function is: {weight}", weight);
        //    return true;
        //}
        private static readonly Dictionary<int, int> MAP_D1 = new() // thousands (from high nibble of b1)
        {
            { 0xC, 1 }, // seen in F2 CB/CF/CC/C1 …
            { 0xD, 0 }, // seen in F2 D1 …
            { 0x5, 1 }, // seen in 8D 5A …
        };

                private static readonly Dictionary<int, int> MAP_D2_DEFAULT = new() // hundreds (low nibble of b1)
        {
            { 0xB, 4 },
            { 0xF, 5 },
            { 0xC, 3 },
        };
                private static readonly Dictionary<int, int> MAP_D2_IF_N1_C = new()
        {
            { 0xB, 4 }, { 0xF, 5 }, { 0xC, 3 }, { 0x1, 5 },
        };
                private static readonly Dictionary<int, int> MAP_D2_IF_N1_D = new()
        {
            { 0x1, 6 },
        };
                private static readonly Dictionary<int, int> MAP_D2_IF_N1_5 = new()
        {
            { 0xA, 5 },
        };

                private static readonly Dictionary<int, int> MAP_D3 = new() // tens (high nibble of b2)
        {
            { 0xA, 2 },
            { 0xF, 4 },
            { 0x5, 7 },
            { 0x3, 6 },
            { 0x7, 3 },
            { 0xB, 3 },
        };

                private static readonly Dictionary<int, int> MAP_D4 = new() // ones-of-tens (low nibble of b2)
        {
            { 0xB, 8 },
            { 0x9, 4 },
            { 0x8, 8 },
            { 0x2, 2 },
            { 0x4, 2 },
            { 0x3, 6 },
        };

        private bool TryDecodeHexWeight(string hexPayload, out decimal weight)
        {
            weight = 0m;

            // Sanitize to 8 hex chars (4 bytes)
            string hex = new string(hexPayload.Where(Uri.IsHexDigit).ToArray());
            if (hex.Length < 8) return false;
            hex = hex.Substring(0, 8);

            // Bytes: b0 b1 b2 b3
            byte b0 = Convert.ToByte(hex.Substring(0, 2), 16);
            byte b1 = Convert.ToByte(hex.Substring(2, 2), 16);
            byte b2 = Convert.ToByte(hex.Substring(4, 2), 16);
            byte b3 = Convert.ToByte(hex.Substring(6, 2), 16);

            // Nibbles from the middle two bytes carry the digits
            int n1 = (b1 >> 4) & 0xF; // thousands code
            int n2 = b1 & 0xF; // hundreds code
            int n3 = (b2 >> 4) & 0xF; // tens code
            int n4 = b2 & 0xF; // ones-of-tens code (final value ends with 0)

            // Frame "family" — 8D..3A behaves slightly differently from F2..E2/92/22/12
            bool is8D = (b0 == 0x8D && b3 == 0x3A);
            bool isF2 = (b0 == 0xF2 && (b3 == 0xE2 || b3 == 0x92 || b3 == 0x22 || b3 == 0x12));

            // --- d1 (thousands): default 1, except 0xD -> 0
            int d1 = (n1 == 0xD) ? 0 : 1;

            // --- d2 (hundreds) ---
            int d2 = -1;
            if (is8D)
            {
                // 8D-family
                d2 = n2 switch
                {
                    0xA => 5, // e.g. 8D 5A ..
                    0xD => 3, // e.g. 8D 6D ..
                    0x1 => 5, // observed in some C1/B1 cases
                    _ => -1
                };
            }
            else // F2-family (default)
            {
                d2 = n2 switch
                {
                    0xB => 4, // CB -> 4
                    0xF => 5, // CF -> 5
                    0xC => 3, // CC -> 3
                    0x1 => 5, // C1 -> 5 (15360 sample)
                    _ => -1
                };
            }
            _logger.LogInformation("The D2 value is: {d2}", d2);

            if (d2 < 0) return false;

            // --- d3 (tens) ---
            int d3 = -1;
            if (is8D)
            {
                // 8D-family tweak: A -> 8 (to satisfy 8D6DA83A -> 13820)
                d3 = n3 switch
                {
                    0x3 => 6, // ..33/32 -> 6
                    0xA => 8, // ..A8 -> 8
                    0x5 => 7, // ..58 -> 7
                    0x7 => 3, // ..74 -> 3 (6320 sample)
                    0xB => 3, // ..B3 -> 3 (15360 sample seen in F2 but harmless here)
                    0xF => 4, // keep common mapping
                    _ => -1
                };
            }
            else // F2-family
            {
                d3 = n3 switch
                {
                    0xA => 2, // ..A? -> 2
                    0xF => 4, // ..F? -> 4
                    0x5 => 7, // ..5? -> 7
                    0x3 => 6, // ..3? -> 6
                    0x7 => 3, // ..7? -> 3
                    0xB => 3, // ..B? -> 3 (15360)
                    _ => -1
                };
            }
            _logger.LogInformation("The D3 value is: {d3}", d3);

            if (d3 < 0) return false;

            // --- d4 (ones-of-tens) — common to both families (from your pairs)
            int d4 = n4 switch
            {
                0xB => 8, // ..B
                0x9 => 4, // ..9
                0x8 => 8, // ..8
                0x2 => 2, // ..2
                0x4 => 2, // ..4
                0x3 => 6, // ..3
                _ => -1
            };
            _logger.LogInformation("The D4 value is: {d4}", d4);

            if (d4 < 0) return false;

            // Build number and scale to kg (your values always end with 0)
            int rawDigits = d1 * 1000 + d2 * 100 + d3 * 10 + d4;
            weight = rawDigits * 10m;
            _logger.LogInformation("Weight decoded via Hex Function is: {weight}", weight);
            return true;
        }
        private bool TryParseAsciiWeight(string line, out decimal weight)
        {
            weight = 0m;

            // Grab all numeric tokens (integers and decimals)
            var matches = System.Text.RegularExpressions.Regex.Matches(
                line, @"[-+]?\d+(?:[.,]\d+)?");

            if (matches.Count == 0) return false;

            // Convert to tokens with digit-count
            var tokens = matches
                .Select(m => new {
                    Raw = m.Value,
                    Digits = m.Value.Replace(".", "").Replace(",", "")
                                    .TrimStart('+', '-')
                                    .Count(char.IsDigit),
                    Value = m.Value
                })
                .ToList();

            // Filter by digit window if any token fits it
            var window = tokens.Where(t => t.Digits >= _asciiMin && t.Digits <= _asciiMax).ToList();
            var pool = window.Count > 0 ? window : tokens; // fall back if none match window

            string chosen;

            switch (_asciiPick)
            {
                case "first":
                    chosen = pool.First().Value;
                    break;
                case "maxlen":
                    chosen = pool.OrderByDescending(t => t.Digits)
                                 .ThenByDescending(t => t.Value.Length)
                                 .First().Value;
                    break;
                case "index":
                    chosen = pool.ElementAtOrDefault(_asciiIndex)?.Value ?? pool.Last().Value;
                    break;
                case "last":
                default:
                    chosen = pool.Last().Value;
                    break;
            }

            if (decimal.TryParse(
                    chosen.Replace(',', '.'),
                    System.Globalization.NumberStyles.Number,
                    System.Globalization.CultureInfo.InvariantCulture,
                    out var v))
            {
                weight = v;
                return true;
            }
                _logger.LogInformation("Weight decoded via ASCII Function is: {weight}", weight);

            return false;
        }

        private decimal ApplyScale(decimal v) => (_scaleNum * v) / _scaleDen + _offset;


        private void AppendRaw(ReadOnlySpan<byte> data)
        {
            // Mirror raw as ASCII with '.' for controls
            var ascii = new string(data.ToArray().Select(b => (b >= 32 && b <= 126) ? (char)b : '.').ToArray());
            File.AppendAllText(_rawLog, ascii);
        }

        // ============ Polling ============
        private async Task StartPollingAsync(CancellationToken token)
        {
            if (string.IsNullOrWhiteSpace(_pollCmd) || _pollIntervalMs <= 0) return;

            byte[] payload = Encoding.ASCII.GetBytes(_pollCmd + "\r\n");

            while (!token.IsCancellationRequested)
            {
                try
                {
                    NetworkStream? s; lock (_connLock) s = _stream;
                    if (s != null && s.CanWrite)
                    {
                        await s.WriteAsync(payload.AsMemory(0, payload.Length), token);
                        await s.FlushAsync(token);
                    }
                }
                catch { /* swallow; try again next tick */ }

                try { await Task.Delay(_pollIntervalMs, token); } catch (TaskCanceledException) { break; }
            }
        }
    }

    // ===== Minimal state parsers =====

    /// <summary> STX .. ETX ASCII-hex capture (no logging). </summary>
    internal sealed class HexFrameParser
    {
        private bool _in;
        private readonly StringBuilder _sb = new();

        public bool Feed(byte b, out string? payload)
        {
            payload = null;

            if (b == 0x02) // STX
            {
                _in = true;
                _sb.Clear();
                return false;
            }
            if (!_in) return false;

            if (b == 0x03) // ETX
            {
                _in = false;
                payload = _sb.ToString();
                _sb.Clear();
                return true;
            }

            // keep only ASCII hex and separators (just in case)
            if ((b >= '0' && b <= '9') || (b >= 'A' && b <= 'F') || (b >= 'a' && b <= 'f'))
                _sb.Append((char)b);
            else if (b == ' ' || b == '-' || b == ':')
                _sb.Append((char)b);

            return false;
        }
    }

    /// <summary> ASCII line collector (CR/LF delimited). </summary>
    internal sealed class LineAsciiParser
    {
        private readonly StringBuilder _sb = new();

        public bool Feed(byte b, out string? line)
        {
            line = null;
            if (b == 0x0D) return false; // ignore CR, wait for LF
            if (b == 0x0A)
            {
                line = _sb.ToString();
                _sb.Clear();
                return line.Length > 0;
            }

            // printable + spaces only
            if (b >= 32 && b <= 126) _sb.Append((char)b);
            else if (b == 0x09) _sb.Append(' '); // TAB -> space

            return false;
        }
    }
}

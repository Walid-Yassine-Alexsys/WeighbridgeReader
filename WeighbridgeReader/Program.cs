// Program.cs — .NET 8
// Lecteur passif MOXA/IND560 : n'envoie rien, lit les lignes et n'affiche QUE le poids.
// Si la valeur brute est en grammes (ex: "43560" => 43.560 kg), laisse SCALE_DIVISOR = 1000.
// Si c'est déjà en kg (ex: "43.560"), mets SCALE_DIVISOR = 1.

using System;
using System.Globalization;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

internal class Program
{
    // ========= CONFIG =========
    private const string HOST = "10.116.136.29"; // IP MOXA/IND560
    private const int PORT = 4001;               // Port MOXA
    private const decimal SCALE_DIVISOR = 1m; // 1000 si valeur brute en g, 1 si déjà en kg
    private const int MIN_DIGITS = 3;            // ignorer "00", etc.
    private static readonly TimeSpan RECONNECT_DELAY = TimeSpan.FromSeconds(2);
    // ==========================

    // Regex pour "<nombre><espace><unité>" (kg|g|t|lb|oz), on prend la DERNIÈRE occurrence
    private static readonly Regex ValueWithUnit = new(
        @"(?<!\S)(?<num>[+-]?\d+(?:[.,]\d+)?)[ ]*(?<unit>kg|g|t|lb|oz)\b",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    // Regex pour n'importe quel nombre (avec . ou ,)
    private static readonly Regex AnyNumber = new(
        @"[+-]?\d+(?:[.,]\d+)?",
        RegexOptions.Compiled);

    public static async Task Main(string[] args)
    {
        using var appCts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; appCts.Cancel(); };

        while (!appCts.IsCancellationRequested)
        {
            try
            {
                using var tcp = new TcpClient();
                await tcp.ConnectAsync(HOST, PORT, appCts.Token);
                using var stream = tcp.GetStream();
                using var reader = new StreamReader(stream, Encoding.ASCII, detectEncodingFromByteOrderMarks: false, bufferSize: 8192, leaveOpen: true);

                while (!appCts.IsCancellationRequested)
                {
                    string? line = await reader.ReadLineAsync(appCts.Token);
                    if (line is null) throw new IOException("Remote closed.");

                    string clean = StripControlChars(line).Trim(); // enlève STX/ETX etc. (ton '☻')
                    if (clean.Length == 0) continue;

                    if (TryParseWeight(clean, out var rawValue))
                    {
                        var scaled = rawValue / SCALE_DIVISOR;
                        // On n'affiche QUE le nombre (point décimal invariant)
                        Console.WriteLine(scaled.ToString("0.###", CultureInfo.InvariantCulture));
                    }
                    // sinon: on ignore la ligne (ex: RFID, texte, etc.)
                }
            }
            catch (OperationCanceledException)
            {
                break; // sortie propre
            }
            catch
            {
                try { await Task.Delay(RECONNECT_DELAY, appCts.Token); } catch { }
            }
        }
    }

    private static string StripControlChars(string s)
    {
        var sb = new StringBuilder(s.Length);
        foreach (var ch in s)
            if (!char.IsControl(ch)) sb.Append(ch);
        return sb.ToString();
    }

    // Stratégie:
    //  1) Chercher la dernière occurrence "<nombre> <unité>" (kg/g/t/lb/oz).
    //  2) Sinon, prendre le plus long token numérique (>= MIN_DIGITS).
    private static bool TryParseWeight(string line, out decimal value)
    {
        // 1) Avec unité
        var unitMatches = ValueWithUnit.Matches(line);
        if (unitMatches.Count > 0)
        {
            var m = unitMatches[^1];
            var raw = m.Groups["num"].Value;
            if (TryParseDecimalFlexible(raw, out value)) return true;
        }

        // 2) Sans unité
        Match? best = null;
        foreach (Match m in AnyNumber.Matches(line))
        {
            var digits = CountDigits(m.Value);
            if (digits >= MIN_DIGITS && (best is null || m.Value.Length > best.Value.Length))
                best = m;
        }

        if (best is not null && TryParseDecimalFlexible(best.Value, out value))
            return true;

        value = default;
        return false;
    }

    private static int CountDigits(string s)
    {
        int c = 0;
        foreach (var ch in s) if (char.IsDigit(ch)) c++;
        return c;
    }

    private static bool TryParseDecimalFlexible(string s, out decimal v)
    {
        if (decimal.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out v)) return true;
        if (decimal.TryParse(s, NumberStyles.Float, CultureInfo.GetCultureInfo("fr-FR"), out v)) return true;
        return false;
    }
}

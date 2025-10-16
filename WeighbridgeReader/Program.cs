using System;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace WeighbridgeReader
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string ip = "192.168.0.100"; // Moxa IP
            int port = 4001;             // Moxa TCP port

            Console.WriteLine($"Connecting to weighbridge at {ip}:{port}...");
            try
            {
                using var client = new TcpClient();
                await client.ConnectAsync(ip, port);
                Console.WriteLine("Connected!");

                using NetworkStream stream = client.GetStream();
                byte[] buffer = new byte[1024];

                while (true)
                {
                    if (stream.DataAvailable)
                    {
                        int bytesRead = await stream.ReadAsync(buffer);
                        string message = Encoding.ASCII.GetString(buffer, 0, bytesRead).Trim();

                        if (!string.IsNullOrWhiteSpace(message))
                        {
                            string weight = ParseWeight(message);
                            Console.WriteLine($"Weight: {weight}");
                        }
                    }

                    await Task.Delay(200);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }

        static string ParseWeight(string data)
        {
            // Example: "W=12340kg" → "12340"
            // Adjust this based on your weighbridge output
            string cleaned = data.Replace("W=", "")
                                 .Replace("kg", "")
                                 .Replace("\r", "")
                                 .Replace("\n", "")
                                 .Trim();
            return cleaned;
        }
    }
}

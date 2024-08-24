using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;

class SimpleTcpClient
{
    static void Main()
    {
        try
        {
            Console.Write("Enter server IP address: ");
            string serverIp = Console.ReadLine();

            Int32 port = 13000;

            using (TcpClient client = new TcpClient(serverIp, port))
            {
                Console.WriteLine("Connected to server...");

                // Thread para receber mensagens do servidor
                Thread receiveThread = new Thread(() =>
                {
                    ReceiveMessages(client);
                });
                receiveThread.Start();

                // Thread para enviar mensagens para o servidor
                Thread sendThread = new Thread(() =>
                {
                    SendMessages(client);
                });
                sendThread.Start();

                // Aguarda o término das threads
                receiveThread.Join();
                sendThread.Join();
            }
        }
        catch (SocketException e)
        {
            Console.WriteLine("SocketException: {0}", e);
        }

        Console.WriteLine("\nPress Enter to exit...");
        Console.ReadLine();
    }

    // Método para receber mensagens do servidor
    static void ReceiveMessages(TcpClient client)
    {
        try
        {
            using (NetworkStream stream = client.GetStream())
            {
                byte[] buffer = new byte[1024];
                int bytesRead;

                // Loop para receber mensagens continuamente
                while ((bytesRead = stream.Read(buffer, 0, buffer.Length)) != 0)
                {
                    string message = Encoding.ASCII.GetString(buffer, 0, bytesRead);
                    Console.WriteLine("Received: " + message);
                }
            }
        }
        catch (IOException)
        {
            Console.WriteLine("Server disconnected.");
        }
    }

    // Método para enviar mensagens para o servidor
    static void SendMessages(TcpClient client)
    {
        try
        {
            using (NetworkStream stream = client.GetStream())
            {
                string message;
                do
                {
                    // Lê a entrada do usuário e envia para o servidor
                    message = Console.ReadLine();
                    byte[] data = Encoding.ASCII.GetBytes(message);
                    stream.Write(data, 0, data.Length);
                } while (message.ToUpper() != "QUIT");
            }
        }
        catch (IOException)
        {
            Console.WriteLine("Server disconnected.");
        }
    }
}


using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

class Task
{
    public int TarefaID { get; set; }
    public string Descricao { get; set; }
    public string Estado { get; set; }
    public int ClienteID { get; set; }

    public Task(int id, string description, string status, int clientId)
    {
        TarefaID = id;
        Descricao = description;
        Estado = status;
        ClienteID = clientId;
    }
}

class TaskManager
{
    private List<Task> tasksA;
    private List<Task> tasksB;
    private List<Task> tasksC;
    private List<Task> tasksD;
    private Mutex mutexA;
    private Mutex mutexB;
    private Mutex mutexC;
    private Mutex mutexD;
    private string csvFilePathA;
    private string csvFilePathB;
    private string csvFilePathC;
    private string csvFilePathD;

    public TaskManager(string csvFilePathA, string csvFilePathB, string csvFilePathC, string csvFilePathD)
    {
        this.csvFilePathA = csvFilePathA;
        this.csvFilePathB = csvFilePathB;
        this.csvFilePathC = csvFilePathC;
        this.csvFilePathD = csvFilePathD;
        tasksA = new List<Task>();
        tasksB = new List<Task>();
        tasksC = new List<Task>();
        tasksD = new List<Task>();
        mutexA = new Mutex();
        mutexB = new Mutex();
        mutexC = new Mutex();
        mutexD = new Mutex();
        LoadTasksFromCSV(csvFilePathA, tasksA, mutexA);
        LoadTasksFromCSV(csvFilePathB, tasksB, mutexB);
        LoadTasksFromCSV(csvFilePathC, tasksC, mutexC);
        LoadTasksFromCSV(csvFilePathD, tasksD, mutexD);
    }

    private void LoadTasksFromCSV(string csvFilePath, List<Task> tasks, Mutex mutex)
    {
        mutex.WaitOne();
        try
        {
            tasks.Clear();

            using (StreamReader sr = new StreamReader(csvFilePath))
            {
                sr.ReadLine();

                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    string[] parts = line.Split(',');

                    if (parts.Length >= 4)
                    {
                        int TarefaID = int.Parse(parts[0]);
                        string Descricao = parts[1];
                        string Estado = parts[2];

                        int ClienteID;
                        if (int.TryParse(parts[3], out ClienteID))
                        {
                            tasks.Add(new Task(TarefaID, Descricao, Estado, ClienteID));
                        }
                        else
                        {
                            Console.WriteLine($"ClienteID invalido na linha: {line}");
                        }
                    }
                    else
                    {
                        Console.WriteLine($"Linha invalida no arquivo CSV: {line}");
                    }
                }
            }
        }
        catch (IOException e)
        {
            Console.WriteLine("Erro ao carregar tarefas do arquivo CSV: " + e.Message);
        }
        catch (Exception e)
        {
            Console.WriteLine("Erro inesperado ao carregar tarefas do arquivo CSV: " + e.Message);
        }
        finally
        {
            mutex.ReleaseMutex();
        }
    }


    public void MarkTaskAsCompleted(int taskId, string csvFilePathA, string csvFilePathB, string csvFilePathC, string csvFilePathD)
    {
        try
        {
            if (taskId >= 1 && taskId <= 5)
            {
                mutexA.WaitOne();
                MarkTaskAsCompletedInFile(tasksA, taskId, csvFilePathA);
            }
            else if (taskId >= 6 && taskId <= 10)
            {
                mutexB.WaitOne();
                MarkTaskAsCompletedInFile(tasksB, taskId, csvFilePathB);
            }
            else if (taskId >= 11 && taskId <= 15) // Handle service C
            {
                mutexC.WaitOne();
                MarkTaskAsCompletedInFile(tasksC, taskId, csvFilePathC);
            }
            else if (taskId >= 16 && taskId <= 20) // Handle service D
            {
                mutexD.WaitOne();
                MarkTaskAsCompletedInFile(tasksD, taskId, csvFilePathD);
            }
            else
            {
                Console.WriteLine("ID de tarefa invalido.");
            }
        }
        finally
        {
            if (taskId >= 1 && taskId <= 5)
            {
                mutexA.ReleaseMutex();
            }
            else if (taskId >= 6 && taskId <= 10)
            {
                mutexB.ReleaseMutex();
            }
            else if (taskId >= 11 && taskId <= 15)
            {
                mutexC.ReleaseMutex();
            }
            else if (taskId >= 16 && taskId <= 20)
            {
                mutexD.ReleaseMutex();
            }
        }
    }

    private void MarkTaskAsCompletedInFile(List<Task> tasks, int taskId, string csvFilePath)
    {
        Task task = tasks.FirstOrDefault(t => t.TarefaID == taskId);

        if (task != null)
        {
            task.Estado = "Concluido";
            task.ClienteID = -1;
            SaveTasksToCSV(csvFilePath, tasks);
            Console.WriteLine($"Tarefa {taskId} marcada como concluida.");
        }
        else
        {
            Console.WriteLine($"Tarefa com ID {taskId} nao encontrada.");
        }
    }

    private void SaveTasksToCSV(string csvFilePath, List<Task> tasksToSave)
    {
        try
        {
            List<string> lines = new List<string>();
            lines.Add("TarefaID,Descricao,Estado,ClienteID");

            foreach (Task task in tasksToSave)
            {
                string line = $"{task.TarefaID},{task.Descricao},{task.Estado},{task.ClienteID}";
                lines.Add(line);
            }

            File.WriteAllLines(csvFilePath, lines);
        }
        catch (Exception e)
        {
            Console.WriteLine("Erro ao salvar tarefas no arquivo CSV: " + e.Message);
        }
    }

    public string AssignNewTask(int clientId)
    {
        if (tasksA.Any(t => t.ClienteID == clientId) || tasksB.Any(t => t.ClienteID == clientId))
        {
            return "Voce ja possui uma tarefa atribuida.";
        }

        Task taskA = tasksA.FirstOrDefault(t => t.Estado == "Nao alocado");
        if (taskA != null)
        {
            taskA.Estado = "Em curso";
            taskA.ClienteID = clientId;
            SaveTasksToCSV(csvFilePathA, tasksA);
            return $"Nova tarefa atribuida com sucesso: {taskA.Descricao}";
        }

        Task taskB = tasksB.FirstOrDefault(t => t.Estado == "Nao alocado");
        if (taskB != null)
        {
            taskB.Estado = "Em curso";
            taskB.ClienteID = clientId;
            SaveTasksToCSV(csvFilePathB, tasksB);
            return $"Nova tarefa atribuida com sucesso: {taskB.Descricao}";
        }

        Task taskC = tasksC.FirstOrDefault(t => t.Estado == "Nao alocado");
        if (taskC != null)
        {
            taskC.Estado = "Em curso";
            taskC.ClienteID = clientId;
            SaveTasksToCSV(csvFilePathC, tasksC);
            return $"Nova tarefa atribuida com sucesso: {taskC.Descricao}";
        }

        Task taskD = tasksD.FirstOrDefault(t => t.Estado == "Nao alocado");
        if (taskD != null)
        {
            taskD.Estado = "Em curso";
            taskD.ClienteID = clientId;
            SaveTasksToCSV(csvFilePathD, tasksD);
            return $"Nova tarefa atribuida com sucesso: {taskD.Descricao}";
        }

        return "Nao ha tarefas disponiveis para atribuicao.";
    }

}

class SimpleTcpServer
{
    private static int nextClientId = 1;
    private static readonly object clientLock = new object();
    private static readonly object fileLock = new object();

    private static int GenerateClientId()
    {
        lock (clientLock)
        {
            return nextClientId++;
        }
    }

    static void Main()
    {
        string csvFilePath1 = "C:/Users/Moisés/Desktop/Universidade/3º ano/2º Semestre/Sistemas Distribuídos/Ficheiros de exemplo/Servico_A.csv";
        string csvFilePath2 = "C:/Users/Moisés/Desktop/Universidade/3º ano/2º Semestre/Sistemas Distribuídos/Ficheiros de exemplo/Servico_B.csv";
        string csvFilePath3 = "C:/Users/Moisés/Desktop/Universidade/3º ano/2º Semestre/Sistemas Distribuídos/Ficheiros de exemplo/Servico_C.csv";
        string csvFilePath4 = "C:/Users/Moisés/Desktop/Universidade/3º ano/2º Semestre/Sistemas Distribuídos/Ficheiros de exemplo/Servico_D.csv";
        TaskManager taskManager = new TaskManager(csvFilePath1, csvFilePath2, csvFilePath3, csvFilePath4);

        TcpListener server = null;
        try
        {
            Int32 port = 13000;
            IPAddress localAddr = IPAddress.Parse("127.0.0.1");
            server = new TcpListener(localAddr, port);
            server.Start();
            Console.WriteLine("Server started...");

            while (true)
            {
                TcpClient client = server.AcceptTcpClient();
                Console.WriteLine("Client connected...");

                int clientId = GenerateClientId();

                Thread clientThread = new Thread(() => HandleClient(client, clientId, taskManager, csvFilePath1, csvFilePath2, csvFilePath3, csvFilePath4));
                clientThread.Start();
            }
        }
        catch (SocketException e)
        {
            Console.WriteLine("SocketException: {0}", e);
        }
        finally
        {
            server?.Stop();
        }

        Console.WriteLine("\nPress Enter to exit...");
        Console.ReadLine();
    }

    static void HandleClient(TcpClient client, int clientId, TaskManager taskManager, string csvFilePathA, string csvFilePathB, string csvFilePathC, string csvFilePathD)
    {
        try
        {
            NetworkStream stream = client.GetStream();

            byte[] initialResponse = Encoding.ASCII.GetBytes("100 OK");
            stream.Write(initialResponse, 0, initialResponse.Length);
            Console.WriteLine("Sent: 100 OK");

            string idResponse = $"ID:{clientId} \n";
            byte[] idResponseBytes = Encoding.ASCII.GetBytes(idResponse);
            stream.Write(idResponseBytes, 0, idResponseBytes.Length);
            Console.WriteLine("Sent: " + idResponse);

            byte[] buffer = new byte[1024];
            int bytesRead;

            while ((bytesRead = stream.Read(buffer, 0, buffer.Length)) != 0)
            {
                string message = Encoding.ASCII.GetString(buffer, 0, bytesRead);
                Console.WriteLine("Received: " + message);

                if (message.Trim().ToUpper() == "QUIT")
                {
                    byte[] responseMessage = Encoding.ASCII.GetBytes("400 BYE");
                    stream.Write(responseMessage, 0, responseMessage.Length);
                    Console.WriteLine("Sent: 400 BYE");
                    break;
                }
                else if (message.StartsWith("CONCLUIDA"))
                {
                    int taskId = int.Parse(message.Substring(10));
                    string csvFilePath = GetCsvFilePathForClient(clientId);
                    if (taskId >= 1 && taskId <= 5)
                    {
                        taskManager.MarkTaskAsCompleted(taskId, csvFilePath, csvFilePathA, csvFilePathB, csvFilePathC);
                    }
                    else if (taskId >= 6 && taskId <= 10)
                    {
                        taskManager.MarkTaskAsCompleted(taskId, csvFilePath, csvFilePathA, csvFilePathB, csvFilePathC);
                    }
                    else if (taskId >= 11 && taskId <= 15)
                    {
                        taskManager.MarkTaskAsCompleted(taskId, csvFilePathC, csvFilePathA, csvFilePathB, csvFilePathD);
                    }
                    else if (taskId >= 16 && taskId <= 20)
                    {
                        taskManager.MarkTaskAsCompleted(taskId, csvFilePathD, csvFilePathA, csvFilePathB, csvFilePathC);
                    }

                    byte[] responseMessage = Encoding.ASCII.GetBytes("Tarefa concluída com sucesso.");
                    stream.Write(responseMessage, 0, responseMessage.Length);
                    Console.WriteLine("Sent: Tarefa concluida com sucesso.");
                }

                else if (message == "NOVA_TAREFA")
                {
                    string response = taskManager.AssignNewTask(clientId);
                    if (!string.IsNullOrEmpty(response))
                    {
                        byte[] responseMessage = Encoding.ASCII.GetBytes(response);
                        stream.Write(responseMessage, 0, responseMessage.Length);
                        Console.WriteLine("Sent: " + response);
                    }
                }

                Array.Clear(buffer, 0, buffer.Length);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred with client {clientId}: {ex.Message}");
        }
        finally
        {
            client.Close();
            Console.WriteLine($"Client {clientId} disconnected.");
        }
    }

    private static string GetCsvFilePathForClient(int clientId)
    {
        if (clientId % 4 == 0)
        {
            return "C:/Users/Moisés/Desktop/Universidade/3º ano/2º Semestre/Sistemas Distribuídos/Ficheiros de exemplo/Servico_A.csv";
        }
        else if (clientId % 4 == 1)
        {
            return "C:/Users/Moisés/Desktop/Universidade/3º ano/2º Semestre/Sistemas Distribuídos/Ficheiros de exemplo/Servico_B.csv";
        }
        else if (clientId % 4 == 2)
        {
            return "C:/Users/Moisés/Desktop/Universidade/3º ano/2º Semestre/Sistemas Distribuídos/Ficheiros de exemplo/Servico_C.csv";
        }
        else // clientId % 4 == 3
        {
            return "C:/Users/Moisés/Desktop/Universidade/3º ano/2º Semestre/Sistemas Distribuídos/Ficheiros de exemplo/Servico_D.csv";
        }
    }
}

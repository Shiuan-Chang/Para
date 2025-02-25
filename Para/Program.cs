using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Para
{
    class Program  
    {
        private static async Task ParallelProcessingAsync()
        {
            Console.OutputEncoding = Encoding.UTF8;
            Stopwatch totalStopwatch = Stopwatch.StartNew();
            double maxReadTime = 0;
            double maxWriteTime = 0;

            string readFilePath = @"C:\CSharp練習\data read\MOCK_DATA (10).csv";
            string baseFolderPath = @"C:\Users\icewi\OneDrive\桌面\DataTest";
            string outputPath = Path.Combine(baseFolderPath, "output.csv");

            int totalRecords = 10_000_000; // 總資料筆數
            int recordNum = 3_000_000; // 每個批次處理的數據量
            int batchNum = (totalRecords / recordNum) + 1; // 計算批次數量

            // 限制最多同時 5 個 Task 寫入檔案，避免過多 I/O 操作
            SemaphoreSlim writeSemaphore = new SemaphoreSlim(1, 1);


            Console.WriteLine($"開始並行讀取 CSV (批次數: {batchNum})");

            await Parallel.ForAsync(0, batchNum, new ParallelOptions { MaxDegreeOfParallelism = 5 }, async (count, _) =>
            {
                Stopwatch readStopwatch = Stopwatch.StartNew();

                // 計算讀取範圍
                int startLine = count * recordNum;
                int linesToRead = Math.Min(recordNum, totalRecords - startLine);

                Console.WriteLine($"執行緒 {count + 1}/{batchNum} 開始讀取 {startLine} ~ {startLine + linesToRead}");

                // 讀取 CSV
                List<CsvRow> result = await Task.Run(() => CSVHelper.CSV.ReadCSV<CsvRow>(readFilePath, startLine, linesToRead));

                readStopwatch.Stop();
                double batchReadTime = readStopwatch.Elapsed.TotalSeconds;
                Console.WriteLine($"執行緒 {count + 1}/{batchNum} 結束讀取，資料筆數: {result.Count}，讀取耗時: {batchReadTime:F4} 秒");


                Stopwatch writeStopwatch = Stopwatch.StartNew();
                await writeSemaphore.WaitAsync(); // 限制同時寫入的 Task
                try
                {
                    await Task.Run(() => CSVHelper.CSV.WriteCSV<CsvRow>(outputPath, result, true));
                }
                finally
                {
                    writeSemaphore.Release();
                }
                writeStopwatch.Stop();

                double batchWriteTime = writeStopwatch.Elapsed.TotalSeconds;

                writeStopwatch.Stop();
                maxWriteTime = writeStopwatch.Elapsed.TotalSeconds;

            });

            Console.WriteLine("所有讀取作業完成，開始寫入 CSV...");

           
            

            totalStopwatch.Stop();

            Console.WriteLine($"完成寫入，寫入耗時: {maxWriteTime:F4} 秒");
            Console.WriteLine("程式執行結束");
            Console.WriteLine($"最大讀取耗時: {maxReadTime:F2} 秒");
            Console.WriteLine($"最大寫入耗時: {maxWriteTime:F2} 秒");
            Console.WriteLine($"總共耗時: {totalStopwatch.Elapsed.TotalSeconds:F2} 秒");
        }

        static async Task Main(string[] args) 
        {
            await ParallelProcessingAsync();
        }
    }
}

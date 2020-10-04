
nameclass NexoTestApp
{
    partial class Program
    {
        public static string checklastrun()
        {
            /// This code is used to calculate time in minutes between last runned program and now. 
            /// This is only run on the first use of programm. 
            DateTime logTime = DateTime.Parse(File.ReadAllText("lastsync.txt"));
            DateTime now = DateTime.Now;
            TimeSpan span = now.Subtract(logTime);
            var temp = span.Minutes + (span.Hours*60) + (span.Days*3600);
            Console.WriteLine(temp);
            string a = temp.ToString();
            return a;
        }
    }   
}

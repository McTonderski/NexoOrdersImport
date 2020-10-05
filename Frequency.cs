using InsERT.Moria.Asortymenty;
using InsERT.Moria.Dokumenty.Logistyka;
using InsERT.Moria.Klienci;
using InsERT.Moria.ModelDanych;
using InsERT.Moria.ModelOrganizacyjny;
using InsERT.Moria.Sfera;
using InsERT.Mox.Product;
using InsERT.Mox.Validation;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace NexoTestApp
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

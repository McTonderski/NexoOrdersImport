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
        public static Uchwyt UruchomSfere()
        {
            DanePolaczenia danePolaczenia = DanePolaczenia.Jawne("SERWER", "DB", true);
            try
            {
                MenedzerPolaczen mp = new MenedzerPolaczen();
                Uchwyt sfera = mp.Polacz(danePolaczenia, ProductId.Subiekt);
                if (!sfera.ZalogujOperatora("USER", "PASSWORD"))
                    throw new ArgumentException("Nie udało się zalogować");
                return sfera;
            }catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
            return null;
        }
    }
}
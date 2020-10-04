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
    public class PairZamowienieOpakowanie
    {
        public string ZamowienieHashKey { get; set; }
        public string OpakowanieHashKey { get; set; }

    }

    partial class Program
    {
        static int last_ETL_runId = 3479461;
        static string time_in_minutes = "15";

        static void Main(string[] args)
        {   // Here program opens file containing last run time and checks how many minutes elapsed since last run. 
            // Program than is going to collect orders that were placed since the last check.
            time_in_minutes = checklastrun();
            // Infinite loop for program to check orders and migrate them from dinar to insert
            while (true)
            {
                // Debugging and testing purposes
                Console.WriteLine("Start check " + DateTime.Now);
                // getting needed data from Dinar
                Dictionary<string, Dictionary<string, List<string>>> pozycje = getListPozycje();
                Dictionary<string, string> LinkiZamowienieKlient = PolaczZBazaLinkZamowienieKlient();
                Dictionary<string, Dictionary<string, string>> ZamowienieDane = PolaczZBazaSatZamowienieDane();
                Dictionary<string, Dictionary<string, string>> KlientAdres = PolaczZBazaSatZamowienieKlientAdres();
                Dictionary<string, Dictionary<string, string>> KlientDane = PolaczZBazaSatZamowienieKlientDane();
                Dictionary<string, Dictionary<string, string>> KlientFirma = PolaczZBazaSatZamowienieKlientDaneFirmy();
                string NIP = default(string);
                string nazwa_firma = default(string);
                string uwagi = default(string);
                string dostawa = default(string);
                string source = default(string);
                string kosztDostawy = default(string);
                string platnosc = default(string);
                Dictionary<string, string> Firma = new Dictionary<string, string>();
                Dictionary<string, string> Adres = new Dictionary<string, string>();
                Dictionary<string, string> Dane = new Dictionary<string, string>();
                Dictionary<string, List<string>> Pozycje = new Dictionary<string, List<string>>();

                // Loops through the order variables and gathers data from all tables
                foreach (KeyValuePair<string, Dictionary<string, List<string>>> entry in pozycje)
                {   

                    Pozycje = entry.Value;
                    Console.WriteLine(entry.Key);

                    foreach(KeyValuePair<string, List<string>> entryin in entry.Value)
                    {
                    
                        Console.WriteLine("\t" + entryin.Key);
                        foreach(var val in entryin.Value)
                        {
                            Console.WriteLine("\t\t" + val);
                        }
                    }
                    try
                    {
                        string KlientId = LinkiZamowienieKlient[entry.Key.ToString()];
                        Console.WriteLine("\tAdres:");
                        Adres = KlientAdres[entry.Key];
                        foreach(KeyValuePair<string, string> adres in KlientAdres[entry.Key])
                        {
                            Console.WriteLine("\t\t" + adres.Key + ": " + adres.Value);
                        }
                    }catch(Exception ex)
                    {
                        Console.WriteLine("Adres exception");
                        Console.WriteLine(ex);
                    }

                    Console.WriteLine("\n");
                    try
                    {
                    
                        Console.WriteLine("\tDane:");
                        Dane = KlientDane[entry.Key];
                        foreach (KeyValuePair<string, string> dane in KlientDane[entry.Key])
                        {
                            Console.WriteLine("\t\t" + dane.Key + ": " + dane.Value);
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);
                    }
                    if (KlientFirma.ContainsKey(entry.Key) && KlientFirma[entry.Key].ContainsKey("NIP"))// && KlientFirma[entry.Key]["NIP"] != "" && KlientFirma[entry.Key]["NIP"].Length > 5)
                    {   
                        if(KlientFirma[entry.Key]["NIP"] != "")
                        {
                            try
                            {
                                Console.WriteLine("\n");
                                Firma = KlientFirma[entry.Key];
                                NIP = KlientFirma[entry.Key]["NIP"].Replace("-", "").Replace(" ", "");
                                nazwa_firma = KlientFirma[entry.Key]["firma_nazwa"];
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex);
                            }
                            try
                            {
                                Console.WriteLine("\tFirma Dane:");
                                foreach (KeyValuePair<string, string> dane in KlientFirma[entry.Key])
                                {
                                    Console.WriteLine("\t\t" + dane.Key + ": " + dane.Value);
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("Firma exception");
                                Console.WriteLine(ex);
                            }
                        }
                            
                    }
                    else
                    {

                    }

                    Console.WriteLine("\tUwagi: ");
                    if (ZamowienieDane.ContainsKey(entry.Key))
                    {
                        Console.WriteLine("no uwagi");
                        if (ZamowienieDane[entry.Key].ContainsKey("uwagi"))
                        {
                            Console.WriteLine(ZamowienieDane[entry.Key]["uwagi"]);
                            uwagi = ZamowienieDane[entry.Key]["uwagi"];
                            
                        }
                        if (ZamowienieDane[entry.Key].ContainsKey("dostawa"))
                        {
                            dostawa = ZamowienieDane[entry.Key]["dostawa"];
                        }
                        if (ZamowienieDane[entry.Key].ContainsKey("koszt_przesylki"))
                        {
                            kosztDostawy = ZamowienieDane[entry.Key]["koszt_przesylki"];
                        }
                        if (ZamowienieDane[entry.Key].ContainsKey("RecordSource"))
                        {
                            source = ZamowienieDane[entry.Key]["RecordSource"];
                        }
                        if (ZamowienieDane[entry.Key].ContainsKey("platnosc"))
                        {
                            platnosc = ZamowienieDane[entry.Key]["platnosc"];
                            uwagi += "\n" + platnosc;
                        }
                    }

                    // Console.WriteLine(ZamowienieDane[entry.Key]["uwagi"]);
                    try
                    {
                        if (KlientAdres.Count > 0)
                        {
                            uwagi +=  "Źródło: " + getZamowienieSourceId(entry.Key);
                            if (KlientFirma.ContainsKey(entry.Key) && KlientFirma[entry.Key]["NIP"] != "")
                                DodajZamowienie(Pozycje: Pozycje, Adres: Adres, Dane: Dane, uwagi:uwagi, NIP: NIP, firma_nazwa: nazwa_firma, Firma: Firma, dostawa: dostawa, kosztDostawy: kosztDostawy, source: source);
                            else
                                DodajZamowienie(Pozycje: Pozycje, Adres: Adres, Dane: Dane, uwagi: uwagi, dostawa: dostawa, kosztDostawy: kosztDostawy, source: source);
                        }
                    }catch(Exception ex)
                    {
                        Console.WriteLine("Uwagi exception");
                        Console.WriteLine(ex);
                    }
                }
                DateTime dateVal = DateTime.Now;
                Console.WriteLine(dateVal + " Następny sync o: " + dateVal.AddMinutes(15));
                StreamWriter sw = new StreamWriter("lastsync.txt");
                sw.WriteLine(dateVal);
                sw.Flush();
                sw.Close();
                time_in_minutes = "15";
                //Console.WriteLine(time_in_minutes);
                Thread.Sleep(900000);
            }
        }

        public static void EdytujKlienta()
        {
            try
            {
                using (var sfera = UruchomSfere())
                {
                    // TODO dodawnie klientow
                    IZamowieniaOdKlientow mgr = sfera.PodajObiektTypu<IZamowieniaOdKlientow>();

                    IKonfiguracje mgrKonf = sfera.PodajObiektTypu<IKonfiguracje>();
                    var konf = mgrKonf.Dane.WszystkieOTypieDokumentu(TypDokumentu.ZamowienieOdKlienta).FirstOrDefault();

                    using (var dokument = mgr.Utworz(konf))
                    {
                        if (dokument.Zapisz())
                        {
                            Console.WriteLine("Edytowano dane");
                        }
                        else
                        {
                            Console.WriteLine("Dane NIE nie zostaly edytowane.");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Wystapil blad" + ex.Message);
            }
            Console.WriteLine("Ending .... ");
            Console.Read();
        }
    }
}
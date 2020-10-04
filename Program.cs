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

    internal class BladInfo
    {
        public string Tresc { get; private set; }
        public DataErrorSeverity Waznosc { get; private set; }
        public BladInfo(string tresc, DataErrorSeverity waznosc)
        {
            Tresc = tresc;
            Waznosc = waznosc;
        }
    }

    //klasa z metodą rozszerzającą
    internal static class Rozszerzenia
    {
        //metoda rozszerzająca Uchwyt
        internal static BladInfo[] PodajBledy(this Uchwyt sfera,
            InsERT.Mox.ObiektyBiznesowe.IObiektBiznesowy obiektBiznesowy)
        {
            var lista = new List<BladInfo>();
            var store = sfera.PodajObiektTypu<InsERT.Mox.Validation.IValidationMetadataStore>();
            PodajBledy((InsERT.Mox.BusinessObjects.IBusinessObject)obiektBiznesowy, store, lista);
            var uow = ((InsERT.Mox.BusinessObjects.IGetUnitOfWork)obiektBiznesowy).UnitOfWork;
            foreach (var innyObiektBiznesowy in uow.Participants.OfType<InsERT.Mox.BusinessObjects.IBusinessObject>().Where(bo => bo != obiektBiznesowy))
            {
                PodajBledy(innyObiektBiznesowy, store, lista);
            }
            return lista.ToArray();
        }

        //metoda pomocnicza sprawdzająca występowanie błędów
        private static bool HasAnyErrors(InsERT.Mox.Validation.IValidationMetadataStore store, ITypedDataErrorInfo errorInfo)
        {
            return
                errorInfo != null &&
                (
                    errorInfo.Errors.Any() ||
                    errorInfo.MemberErrors.Any()
                );
        }

        //metoda pomocnicza zbierająca informacje o błędach
        private static void PodajBledy(this InsERT.Mox.BusinessObjects.IBusinessObject obiektBiznesowy,
            InsERT.Mox.Validation.IValidationMetadataStore store,
            List<BladInfo> bledy)
        {
            HashSet<ITypedDataErrorInfo> invalidData = new HashSet<ITypedDataErrorInfo>();
            ((InsERT.Mox.DataAccess.IGetDataDomain)obiektBiznesowy).DataDomain.TraverseData(
                obiektBiznesowy.Data, (o) =>
                {
                    ITypedDataErrorInfo dataErrorInfoEx = o as ITypedDataErrorInfo;
                    if (HasAnyErrors(store, dataErrorInfoEx))
                    {
                        invalidData.Add(dataErrorInfoEx);
                    }
                    return true;
                });

            foreach (var encjaZBledami in invalidData)
            {
                foreach (var bladNaCalejEncji in encjaZBledami.Errors)
                {
                    StringBuilder sb = new StringBuilder(3);
                    sb.AppendLine(bladNaCalejEncji.ToString());
                    sb.Append(" na encjach:" + encjaZBledami.GetType().Name);
                    DataErrorType errorType = store.GetEntryForClrType(bladNaCalejEncji.GetType());
                    bledy.Add(new BladInfo(sb.ToString(), errorType.Severity));
                }
                foreach (var bladNaKonkretnychPolach in encjaZBledami.MemberErrors)
                {
                    StringBuilder sb = new StringBuilder(3);
                    sb.AppendLine(bladNaKonkretnychPolach.Key.ToString());
                    sb.AppendLine(" na polach:");
                    sb.AppendLine(string.Join(", ", bladNaKonkretnychPolach.Select(b => encjaZBledami.GetType().Name + "." + b)));
                    DataErrorType errorType = store.GetEntryForClrType(bladNaKonkretnychPolach.Key.GetType());
                    bledy.Add(new BladInfo(sb.ToString(), errorType.Severity));
                }
            }
        }
    }

    class Program
    {
        static int last_ETL_runId = 3479461;
        static string conn_string = @"Server=SERWER; Database=DB; USER=USER; password=PASSWORD; MultipleActiveResultSets=true;";
        static string query_get_orders = "SELECT [ZamowienieKlientHashKey]," +
            " [ZamowienieIdHashKey]," +
            " [KlientIdHashKey]," +
            " [ETL_RunId]," +
            " [RecordSource]," +
            " [ServerId]," +
            " [Id]," +
            " [id_klienta]," +
            " [mail]" +
            " FROM [Dinar].[raw].[LinkZamowienieKlient]" +
            " WHERE ETL_RunId > " + last_ETL_runId +
            " group by ZamowienieIdHashKey, ZamowienieKlientHashKey, KlientIdHashKey, LoadDate, ETL_RunId, RecordSource, ServerId, Id, id_klienta, mail" +
            " order by ETL_RunId desc";

        static string query_get_pozycja_zamowienie_opakowanie = "SELECT [PozycjaZamowienieOpakowanieHashKey]," +
            " [PozycjaIdHashKey]," +
            " [ZamowienieIdHashKey]," +
            " [OpakowanieIdHashKey]," +
            " [LoadDate]," +
            " [ETL_RunId]," +
            " [RecordSource]," +
            " [ServerId]," +
            " [Id]," +
            " [id_zamowienia]," +
            " [id_opakowania]" +
            " FROM [Dinar].[raw].[LinkPozycjaZamowienieOpakowanie]" +
            " WHERE ETL_RunID > " + last_ETL_runId +
            " ORDER BY ETL_RunId asc";

        static string query_SatZamowienieDane = "SELECT [ZamowienieIdHashKey]," +
            " [ETL_RunId]," +
            " [LoadDate]," +
            " [LoadEndDate]," +
            " [RecordSource]," +
            " [data]," +
            " [czy_zrealizowano]," +
            " [platnosc]," +
            " [uwagi]," +
            " [potwierdz]," +
            " [suma]," +
            " [karta]," +
            " [koszt_przesylki]," +
            " [dostawa]," +
            " [aktywny]," +
            " [status]," +
            " [koszt_przesylki_netto]," +
            " [koszt_przesylki_vat]," +
            " [ZamowienieDaneHashDiff]" +
            " FROM [Dinar].[raw].[SatZamowienieDane]" +
            " WHERE ETL_RunId > " + last_ETL_runId + 
            " Order By ETL_RunID desc";

        static string query_adresy = "SELECT [ZamowienieIdHashKey]," +
            " [ETL_RunId]," +
            " [LoadDate]," +
            " [LoadEndDate]," +
            " [RecordSource]," +
            " [adres]," +
            " [adres2]," +
            " [kod]," +
            " [miasto]," +
            " [kraj]," +
            " [imie]," +
            " [nazwisko]," +
            " [firma]," +
            " [ZamowienieKlientAdresHashDiff]" +
            " FROM [Dinar].[raw].[SatZamowienieKlientAdres]" +
            " WHERE ETL_RunId > " + last_ETL_runId +
            " ORDER BY ETL_RunId desc";

        static string query_zamowienie_klient_dane = "SELECT [ZamowienieIdHashKey]," +
            " [ETL_RunId]," +
            " [LoadDate]," +
            " [LoadEndDate]," +
            " [RecordSource]," +
            " [imie]," +
            " [nazwisko]," +
            " [zam_osoba]," +
            " [telefon]," +
            " [fax]," +
            " [mail]," +
            " [data]," +
            " [anonymized]," +
            " [ZamowienieKlientDaneHashDiff]" +
            " FROM [Dinar].[raw].[SatZamowienieKlientDane]" +
            " WHERE ETL_RunId > " + last_ETL_runId +
            " ORDER BY ETL_RunId desc";

        static string query_zamowienie_klient_dane_firmy = "SELECT [ZamowienieIdHashKey]," +
            " [ETL_RunId]," +
            " [LoadDate]," +
            " [LoadEndDate]," +
            " [RecordSource]," +
            " [nip]," +
            " [firma_nazwa]," +
            " [firma_imie]," +
            " [firma_nazwisko]," +
            " [firma_adres]," +
            " [firma_adres2]," +
            " [firma_kod]," +
            " [firma_miasto]," +
            " [firma_kraj]," +
            " [firma_telefon]," +
            " [firma_email]," +
            " [ZamowienieKlientDaneFirmyHashDiff]" +
            " FROM [Dinar].[raw].[SatZamowienieKlientDaneFirmy]" +
            " WHERE ETL_RunId > " + last_ETL_runId +
            " ORDER BY ETL_RunId desc";

       static string query_pozycja = "" +
            "SELECT [PozycjaIdHashKey]," +
            " [ETL_RunId]," +
            " [LoadDate]," +
            " [LoadEndDate]," +
            " [RecordSource]," +
            " [liczba]," +
            " [tytul]," +
            " [cena]," +
            " [vat]," +
            " [cena_netto]," +
            " [PozycjaHashDiff]," +
            " [wartosc_vat] " +
            "FROM [Dinar].[raw].[SatPozycja] " +
            "WHERE ETL_RunID > " + last_ETL_runId +
            "ORDER BY ETL_RunId desc";

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
        }

    }
}
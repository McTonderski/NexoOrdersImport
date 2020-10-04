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

        public static string getZamowienieSourceId(string zamowienieIdHashKey)
        {
            string recordSource = ""; string id = ""; string server_feedback = "";
            string connString = conn_string;
            try
            {
                using (SqlConnection conn = new SqlConnection(connString))
                {
                    SqlCommand cmd = new SqlCommand(query_id_zamowienia + zamowienieIdHashKey+"'", conn);
                    conn.Open();
                    SqlDataReader dr = cmd.ExecuteReader();

                    

                    if (dr.HasRows)
                    {
                        while (dr.Read())
                        {
                            recordSource = dr[0] as string ?? default(string);
                            id = (dr[1] as int? ?? default(int)).ToString();
                        }
                    }

                    server_feedback = recordSource + " " + id;
                }
            }
            catch (Exception ex)
            {
                //display error message
                Console.WriteLine("Exception: " + ex.Message);
            }
            return server_feedback;
        }

        public static Dictionary<string, Dictionary<string, List<string>>> getListPozycje()
        {
            string connString = conn_string;
            try
            {
                //sql connection object
                using (SqlConnection conn = new SqlConnection(connString))
                {
                    Console.WriteLine(time_in_minutes);
                    
                    //define the SqlCommand object
                    string query = "SELECT distinct a.ZamowienieIdHashKey, " +
                                " b.PozycjaIdHashKey, " +
		                        " c.OpakowanieIdHashKey, " +
		                        " b.tytul, " +
		                        " Last_value(c.symbol) over(PARTITION BY symbol order by symbol RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as symbol, " +
		                        " c.RecordSource, " +
		                        " b.liczba, " +
		                        " b.cena, " +
		                        " a.LoadDate " +
                                " FROM[Dinar].[raw].[LinkPozycjaZamowienieOpakowanie] as a, [Dinar].[raw].[SatPozycja] as b, [Dinar].[raw].[SatOpakowanie] as c" +
                                " WHERE a.OpakowanieIdHashKey = c.OpakowanieIdHashKey and a.PozycjaIdHashKey = b.PozycjaIdHashKey and a.LoadDate > (Select DATEADD(mi, -" + time_in_minutes + ", GETDATE()))" +
                                " group by a.ZamowienieIdHashKey, b.PozycjaIdHashKey, c.OpakowanieIdHashKey, b.tytul, c.symbol, c.RecordSource, b.liczba, b.cena, a.LoadDate" +
                                " order by a.LoadDate desc ";
                    SqlCommand cmd = new SqlCommand(query, conn);

                    //open connection
                    conn.Open();

                    //execute the SQLCommand
                    SqlDataReader dr = cmd.ExecuteReader();

                    // Console.WriteLine(Environment.NewLine + "Wczytywanie Pozycji Zamowien Oraz Ilosci" + Environment.NewLine);
                    // Console.WriteLine("Retrieved records:");

                    string ZamowienieIdHashKey, symbol, ilosc, RecordSource, cena;
                    // double cena;
                    Dictionary<string, Dictionary<string, List<string>>> temp = new Dictionary<string, Dictionary<string, List<string>>>();

                    //check if there are records

                    if (dr.HasRows)
                    {
                        while (dr.Read())
                        {
                            ZamowienieIdHashKey = dr[0] as string ?? default(string);
                            symbol = dr[4] as string ?? default(string);
                            cena = dr[7].ToString() as string ?? default(string);
                            RecordSource = dr[5] as string ?? default(string);
                            ilosc = (dr[6] as int? ?? default(int)).ToString();

                            if (temp.ContainsKey(ZamowienieIdHashKey))
                            {
                                if (temp[ZamowienieIdHashKey].ContainsKey("symbol"))
                                {
                                    temp[ZamowienieIdHashKey]["symbol"].Add(symbol);
                                }
                                else
                                {
                                    temp[ZamowienieIdHashKey]["symbol"] = new List<string>
                                    {
                                        symbol
                                    };
                                }
                                if (temp[ZamowienieIdHashKey].ContainsKey("ilosc"))
                                {
                                    temp[ZamowienieIdHashKey]["ilosc"].Add(ilosc);
                                }
                                else
                                {
                                    temp[ZamowienieIdHashKey]["ilosc"] = new List<string>
                                    {
                                        ilosc
                                    };
                                }
                                if (temp[ZamowienieIdHashKey].ContainsKey("RecordSource"))
                                {
                                    temp[ZamowienieIdHashKey]["RecordSource"].Add(RecordSource);
                                }
                                else
                                {
                                    temp[ZamowienieIdHashKey]["RecordSource"] = new List<string>
                                    {
                                        RecordSource
                                    };
                                }
                                if (temp[ZamowienieIdHashKey].ContainsKey("cena"))
                                {
                                    temp[ZamowienieIdHashKey]["cena"].Add(cena);
                                }
                                else
                                {
                                    temp[ZamowienieIdHashKey]["cena"] = new List<string>();
                                    temp[ZamowienieIdHashKey]["cena"].Add(cena);
                                }
                            }
                            else
                            {
                                temp[ZamowienieIdHashKey] = new Dictionary<string, List<string>>();
                                if (temp[ZamowienieIdHashKey].ContainsKey("symbol"))
                                {
                                    temp[ZamowienieIdHashKey]["symbol"].Add(symbol);
                                }
                                else
                                {
                                    temp[ZamowienieIdHashKey]["symbol"] = new List<string>();
                                    temp[ZamowienieIdHashKey]["symbol"].Add(symbol);
                                }
                                if (temp[ZamowienieIdHashKey].ContainsKey("ilosc"))
                                {
                                    temp[ZamowienieIdHashKey]["ilosc"].Add(ilosc);
                                }
                                else
                                {
                                    temp[ZamowienieIdHashKey]["ilosc"] = new List<string>();
                                    temp[ZamowienieIdHashKey]["ilosc"].Add(ilosc);
                                }
                                if (temp[ZamowienieIdHashKey].ContainsKey("RecordSource"))
                                {
                                    temp[ZamowienieIdHashKey]["RecordSource"].Add(RecordSource);
                                }
                                else
                                {
                                    temp[ZamowienieIdHashKey]["RecordSource"] = new List<string>();
                                    temp[ZamowienieIdHashKey]["RecordSource"].Add(RecordSource);
                                }
                                if (temp[ZamowienieIdHashKey].ContainsKey("cena"))
                                {
                                    temp[ZamowienieIdHashKey]["cena"].Add(cena);
                                }
                                else
                                {
                                    temp[ZamowienieIdHashKey]["cena"] = new List<string>();
                                    temp[ZamowienieIdHashKey]["cena"].Add(cena);
                                }
                            }
                        }
                    }
                    else
                    {
                        Console.WriteLine("No data found.");
                    }

                    //close data reader
                    dr.Close();

                    //close connection
                    conn.Close();
                    return temp;
                }
            }
            catch (Exception ex)
            {
                //display error message
                Console.WriteLine("GetListPozycja exception");
                Console.WriteLine("Exception: " + ex.Message);
            }
            return new Dictionary<string, Dictionary<string, List<string>>>();
        }

        public static Dictionary<string, Dictionary<string, string>> PolaczZBazaSatPozycja()
        {
            //set the connection string
            string connString = conn_string;
            
            try
            {
                //sql connection object
                using (SqlConnection conn = new SqlConnection(connString))
                {
                    //define the SqlCommand object
                    SqlCommand cmd = new SqlCommand(query_pozycja, conn);

                    //open connection
                    conn.Open();

                    //execute the SQLCommand
                    SqlDataReader dr = cmd.ExecuteReader();

                    // Console.WriteLine(Environment.NewLine + "Retrieving data from database...SatPozycja" + Environment.NewLine);
                    // Console.WriteLine("Retrieved records:");

                    string PozycjaIdHashKey, tytul, RecordSource, cena, vat, cena_netto, PozycjaHashDiff;
                    int liczba, ETL_RunId;
                    decimal wartosc_vat;
                    Dictionary<string, Dictionary<string, string>> temp = new Dictionary<string, Dictionary<string, string>>();

                    //check if there are records

                    if (dr.HasRows)
                    {
                        while (dr.Read())
                        {
                            PozycjaIdHashKey = dr[0] as string ?? default(string);
                            ETL_RunId = dr.GetInt32(1) as int? ?? default(int);
                            RecordSource = dr[4] as string ?? default(string);
                            liczba = dr.GetInt32(5) as int? ?? default(int);
                            tytul = dr[6] as string ?? default(string);
                            cena = dr[7] as string ?? default(string);
                            vat = dr[8] as string ?? default(string);
                            cena_netto = dr[9] as string ?? default(string);
                            PozycjaHashDiff = dr[10] as string ?? default(string);
                            wartosc_vat = dr.GetDecimal(11) as decimal? ?? default(decimal);
                            Dictionary<string, string> tempin = new Dictionary<string, string>();
                            tempin["ETL_RunId"] = ETL_RunId.ToString();
                            tempin["RecordSource"] = RecordSource;
                            tempin["Liczba"] = liczba.ToString();
                            tempin["Tytul"] = tytul;
                            tempin["Cena"] = cena;
                            tempin["VAT"] = vat;
                            tempin["Cena_Netto"] = cena_netto;
                            tempin["PozycjaHashDiff"] = PozycjaHashDiff;
                            tempin["Wartosc_vat"] = wartosc_vat.ToString();

                            temp[PozycjaIdHashKey] = tempin;

                        }
                    }
                    else
                    {
                        Console.WriteLine("No data found.");
                    }

                    //close data reader
                    dr.Close();

                    //close connection
                    conn.Close();
                    return temp;
                }
            }
            catch (Exception ex)
            {
                //display error message
                Console.WriteLine("Exception: " + ex.Message);
            }
            return null;

        }

        public static Dictionary<string, Dictionary<string, string>> PolaczZBazaSatZamowienieKlientDaneFirmy()
        {
            //set the connection string
            string connString = conn_string;
            try
            {
                //sql connection object
                using (SqlConnection conn = new SqlConnection(connString))
                {
                    //define the SqlCommand object
                    SqlCommand cmd = new SqlCommand(query_zamowienie_klient_dane_firmy, conn);

                    //open connection
                    conn.Open();

                    //execute the SQLCommand
                    SqlDataReader dr = cmd.ExecuteReader();

                    // Console.WriteLine(Environment.NewLine + "Retrieving data from database...SatZamowienieKlientDaneFirmy" + Environment.NewLine);
                    // Console.WriteLine("Retrieved records:");

                    string ZamowienieIdHashKey, RecordSource, nip,  firma_nazwa, firma_imie, firma_nazwisko, firma_adres, firma_adres2, firma_kod, firma_miasto, firma_kraj, firma_telefon, firma_email, ZamowienieKlientDaneFirmyHashDiff;
                    int ETL_RunId;

                    Dictionary<string, Dictionary<string, string>> temp = new Dictionary<string, Dictionary<string, string>>();

                    //check if there are records
                    if (dr.HasRows)
                    {
                        while (dr.Read())
                        {
                            ZamowienieIdHashKey = dr[0] as string ?? default(string); ;
                            ETL_RunId = dr.GetInt32(1);
                            RecordSource = dr[4] as string ?? default(string);
                            nip = dr[5] as string ?? default(string);
                            firma_nazwa = dr[6] as string ?? default(string);
                            firma_imie = dr[7] as string ?? default(string);
                            firma_nazwisko = dr[8] as string ?? default(string);
                            firma_adres = dr[9] as string ?? default(string);
                            firma_adres2 = dr[10] as string ?? default(string);
                            firma_kod = dr[11] as string ?? default(string);
                            firma_miasto = dr[12] as string ?? default(string);
                            firma_kraj = dr[13] as string ?? default(string);
                            firma_telefon = dr[14] as string ?? default(string);
                            firma_email = dr[15] as string ?? default(string);
                            ZamowienieKlientDaneFirmyHashDiff = dr[16] as string ?? default(string);
                            Dictionary<string, string> tempin = new Dictionary<string, string>();
                            tempin["ETL_RunId"] = ETL_RunId.ToString();
                            tempin["RecordSource"] = RecordSource;
                            tempin["NIP"] = nip;
                            tempin["firma_nazwa"] = firma_nazwa;
                            tempin["firma_imie"] = firma_imie;
                            tempin["firma_nazwisko"] = firma_nazwisko;
                            tempin["firma_adres"] = firma_adres;
                            tempin["firma_adres2"] = firma_adres2;
                            tempin["firma_kod"] = firma_kod;
                            tempin["firma_miasto"] = firma_miasto;
                            tempin["firma_kraj"] = firma_kraj;
                            tempin["firma_telefon"] = firma_telefon;
                            tempin["firma_email"] = firma_email;
                            tempin["ZamowienieKlientDaneFirmyHashDiff"] = ZamowienieKlientDaneFirmyHashDiff;
                            temp[ZamowienieIdHashKey] = tempin;
                        }
                    }
                    else
                    {
                        Console.WriteLine("No data found.");
                    }

                    //close data reader
                    dr.Close();

                    //close connection
                    conn.Close();
                    return temp;
                }
            }
            catch (Exception ex)
            {
                //display error message
                Console.WriteLine("Exception: " + ex.Message);
                
            }
            return new Dictionary<string, Dictionary<string, string>>();
        }

        static void AddZK()
        {
            Console.WriteLine("Podaj NIP klienta // docelowo podawane będzie IDKlienta//: ");
            string nip = Console.ReadLine();
            Console.WriteLine("Podaj ilość produktów w zamówieniu");
            string numberOfProducts = Console.ReadLine();
            int numberOfProductsInt;
            if(!Int32.TryParse(numberOfProducts, out numberOfProductsInt))
            {
                numberOfProductsInt = 1;
            }
            List<string> listaProduktow = new List<string>();
            for (int i = 0; i < numberOfProductsInt; i++)
            {
                Console.WriteLine("Podaj kod produktu:");
                listaProduktow.Add(Console.ReadLine());

            }
            string commentarz;
            Console.WriteLine("Komentarz // docelowo będzie to numer zlecenia z dinara");
            commentarz = Console.ReadLine();
            // DodajZamowienie(nip, listaProduktow, commentarz);
        }
    }
}
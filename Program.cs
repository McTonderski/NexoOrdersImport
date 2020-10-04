﻿using InsERT.Moria.Asortymenty;
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

        static string query_id_zamowienia = "SELECT [RecordSource], [Id] FROM [Dinar].[raw].[LinkZamowienieKlient] where [ZamowienieIdHashKey] = '";
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

        public static Dictionary<string, Dictionary<string, string>> PolaczZBazaSatZamowienieKlientDane()
        {
            //set the connection string
            string connString = conn_string;
            

            try
            {
                //sql connection object
                using (SqlConnection conn = new SqlConnection(connString))
                {
                    //define the SqlCommand object
                    SqlCommand cmd = new SqlCommand(query_zamowienie_klient_dane, conn);

                    //open connection
                    conn.Open();

                    //execute the SQLCommand
                    SqlDataReader dr = cmd.ExecuteReader();


                    string ZamowienieIdHashKey;
                    string RecordSource;
                    string imie;
                    string nazwisko;
                    string zam_osoba;
                    string telefon; 
                    string fax; 
                    string mail; 
                    string ZamowienieKlientDaneHashDiff;

                    Dictionary<string, Dictionary<string, string>> temp = new Dictionary<string, Dictionary<string, string>>();

                    //check if there are records
                    if (dr.HasRows)
                    {
                        while (dr.Read())
                        {
                            ZamowienieIdHashKey = dr[0] as string ?? default(string);
                            RecordSource = dr[4] as string ?? default(string);
                            imie = dr[5] as string ?? default(string);
                            nazwisko = dr[6] as string ?? default(string);
                            zam_osoba = dr[7] as string ?? default(string);
                            telefon = dr[8] as string ?? default(string);
                            fax = dr[9] as string ?? default(string);
                            mail = dr[10] as string ?? default(string);
                            ZamowienieKlientDaneHashDiff = dr[13] as string ?? default(string);
                            Dictionary<string, string> tempin = new Dictionary<string, string>();
                            tempin["RecordSource"] = RecordSource;
                            tempin["imie"] = imie;
                            tempin["nazwisko"] = nazwisko;
                            tempin["zam_osoba"] = zam_osoba;
                            tempin["telefon"] = telefon;
                            tempin["fax"] = fax;
                            tempin["mail"] = mail;
                            tempin["ZamowienieKlientDaneHashDiff"] = ZamowienieKlientDaneHashDiff;

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

        public static Dictionary<string, Dictionary<string, string>> PolaczZBazaSatZamowienieKlientAdres()
        {
            //set the connection string
            string connString = conn_string;
            

            try
            {
                //sql connection object
                using (SqlConnection conn = new SqlConnection(connString))
                {
                    //define the SqlCommand object
                    SqlCommand cmd = new SqlCommand(query_adresy, conn);

                    //open connection
                    conn.Open();

                    //execute the SQLCommand
                    SqlDataReader dr = cmd.ExecuteReader();

                    // Console.WriteLine(Environment.NewLine + "Retrieving data from database...ZamowienieKlientAdres" + Environment.NewLine);
                    // Console.WriteLine("Retrieved records:");

                    string ZamowienieIdHashKey, RecordSource, adres, adres2, kod, miasto, kraj, imie,nazwisko,firma, ZamowienieKlientAdresHashDiff;
                    int ETL_RunId;
                    Dictionary<string, Dictionary<string, string>> temp = new Dictionary<string, Dictionary<string, string>>();
                    //check if there are records
                    if (dr.HasRows)
                    {
                        while (dr.Read())
                        {
                            ZamowienieIdHashKey = dr[0] as string ?? default(string);
                            ETL_RunId = dr.GetInt32(1);
                            RecordSource = dr[4] as string ?? default(string);
                            adres = dr[5] as string ?? default(string);
                            adres2 = dr[6] as string ?? default(string);
                            kod = dr[7] as string ?? default(string);
                            miasto = dr[8] as string ?? default(string);
                            kraj = dr[9] as string ?? default(string);
                            imie = dr[10] as string ?? default(string);
                            nazwisko = dr[11] as string ?? default(string);
                            firma = dr[12] as string ?? default(string);
                            ZamowienieKlientAdresHashDiff = dr.GetString(13);

                            Dictionary<string, string> tempin = new Dictionary<string, string>
                            {
                                ["ETL_RunId"] = ETL_RunId.ToString(),
                                ["RecordSource"] = RecordSource,
                                ["adres"] = adres,
                                ["adres2"] = adres2,
                                ["kod"] = kod,
                                ["miasto"] = miasto,
                                ["kraj"] = kraj,
                                ["imie"] = imie,
                                ["nazwisko"] = nazwisko,
                                ["firma"] = firma,
                                ["ZamowienieKlientAdresHashDiff"] = ZamowienieKlientAdresHashDiff
                            };

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

        public static Dictionary<string, Dictionary<string, string>> PolaczZBazaSatZamowienieDane()
        {
            //set the connection string
            string connString = conn_string;
            

            try
            {
                //sql connection object
                using (SqlConnection conn = new SqlConnection(connString))
                {
                    //define the SqlCommand object
                    SqlCommand cmd = new SqlCommand(query_SatZamowienieDane, conn);

                    //open connection
                    conn.Open();

                    //execute the SQLCommand
                    SqlDataReader dr = cmd.ExecuteReader();

                    // Console.WriteLine(Environment.NewLine + "Retrieving data from database...ZamowienieDane" + Environment.NewLine);
                    // Console.WriteLine("Retrieved records:");
                    decimal suma;
                    string ZamowienieIdHashKey, RecordSource, data, platnosc, uwagi, koszt_przesylki, dostawa, koszt_przesylki_netto, ZamowienieDaneHashDiff;
                    
                    Dictionary<string, Dictionary<string, string>> temp = new Dictionary<string, Dictionary<string, string>>();

                    //check if there are records
                    if (dr.HasRows)
                    {
                        while (dr.Read())
                        {
                            ZamowienieIdHashKey = dr[0] as string ?? default(string);
                            RecordSource = dr[4] as string ?? default(string);
                            data = dr[5] as string ?? default(string);
                            platnosc = dr[7] as string ?? default(string);
                            uwagi = dr[8] as string ?? default(string);
                            suma = dr.GetDecimal(10) as decimal? ?? default(decimal);
                            koszt_przesylki = dr[12] as string ?? default(string);
                            dostawa = dr[13] as string ?? default(string);
                            koszt_przesylki_netto = dr[16] as string ?? default(string);
                            ZamowienieDaneHashDiff = dr[18] as string ?? default(string);

                            Dictionary<string, string> tempin = new Dictionary<string, string>();
                            
                            tempin["RecordSource"] = RecordSource;
                            tempin["data"] = data;
                            tempin["platnosc"] = platnosc;
                            tempin["uwagi"] = uwagi;
                            tempin["suma"] = suma.ToString();
                            tempin["koszt_przesylki"] = koszt_przesylki.ToString();
                            tempin["dostawa"] = dostawa;
                            tempin["koszt_przesylki_vat"] = koszt_przesylki_netto.ToString();
                            tempin["ZamowienieDaneHashDiff"] = ZamowienieDaneHashDiff;

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

        public static Dictionary<string, Dictionary<string, List<string>>> PolaczZBazaLinkPozycjaZamowienieOpakowanie()
        {
            string connString = conn_string;

            //variables to store the query results
            string PozycjaZamowienieOpakowanieHashKey, PozycjaIdHashKey, ZamowienieIdHashKey, OpakowanieIdHashKey, RecordSource;
            int ETL_RunId, Id, id_zamowienie, id_opakowanie, ServerId;
            try
            {
                //sql connection object
                using (SqlConnection conn = new SqlConnection(connString))
                {
                    //define the SqlCommand object
                    SqlCommand cmd = new SqlCommand(query_get_pozycja_zamowienie_opakowanie, conn);

                    //open connection
                    conn.Open();
                    // Console.WriteLine("\nRetrieving data from Database...LinkPozycjaZamowienieOpakowanie \n");
                    //execute the SQLCommand
                    SqlDataReader dr = cmd.ExecuteReader();

                    Dictionary<string, Dictionary<string, List<string>>> temp = new Dictionary<string, Dictionary<string, List<string>>>();


                    //check if there are records
                    if (dr.HasRows)
                    {
                        while (dr.Read())
                        {
                            PozycjaZamowienieOpakowanieHashKey = dr[0] as string ?? default(string);
                            PozycjaIdHashKey = dr[1] as string ?? default(string);
                            ZamowienieIdHashKey = dr[2] as string ?? default(string);
                            OpakowanieIdHashKey = dr[3] as string ?? default(string);
                            ETL_RunId = dr.GetInt32(5);
                            RecordSource = dr[6] as string ?? default(string);
                            ServerId = dr.GetInt32(7);
                            Id = dr.GetInt32(8);
                            id_zamowienie = dr.GetInt32(9);
                            id_opakowanie = dr.GetInt32(10);
                            if(temp.ContainsKey(ZamowienieIdHashKey))
                            {
                                temp[ZamowienieIdHashKey]["PozycjaIdHashKey"].Add(PozycjaIdHashKey);
                                temp[ZamowienieIdHashKey]["OpakowanieIdHashKey"].Add(OpakowanieIdHashKey);
                            }
                            else
                            {
                                temp[ZamowienieIdHashKey] = new Dictionary<string, List<string>>();
                                if (temp[ZamowienieIdHashKey].ContainsKey("PozycjaIdHashKey"))
                                {
                                    temp[ZamowienieIdHashKey]["PozycjaIdHashKey"].Add(PozycjaIdHashKey);
                                }
                                else
                                {
                                    temp[ZamowienieIdHashKey]["PozycjaIdHashKey"] = new List<string>();
                                    temp[ZamowienieIdHashKey]["PozycjaIdHashKey"].Add(PozycjaIdHashKey);
                                }
                                if (temp[ZamowienieIdHashKey].ContainsKey("OpakowanieIdHashKey"))
                                {
                                    temp[ZamowienieIdHashKey]["OpakowanieIdHashKey"].Add(OpakowanieIdHashKey);
                                }
                                else
                                {
                                    temp[ZamowienieIdHashKey]["OpakowanieIdHashKey"] = new List<string>();
                                    temp[ZamowienieIdHashKey]["OpakowanieIdHashKey"].Add(OpakowanieIdHashKey);
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
                    // Console.WriteLine("Retrieving Data OK...");
                    return temp;
                }
            }
            catch (Exception ex)
            {
                //display error message
                Console.WriteLine("Exception: " + ex.Message);
            }

            return new Dictionary<string, Dictionary<string, List<string>>>();

        }

        public static Dictionary<string, string> PolaczZBazaLinkZamowienieKlient()
        {
            //set the connection string
            string connString = conn_string;

            //variables to store the query results

            try
            {
                //sql connection object
                using (SqlConnection conn = new SqlConnection(connString))
                {
                    //define the SqlCommand object
                    SqlCommand cmd = new SqlCommand(query_get_orders, conn);

                    //open connection
                    conn.Open();

                    //execute the SQLCommand
                    SqlDataReader dr = cmd.ExecuteReader();

                    // Console.WriteLine(Environment.NewLine + "Retrieving data from database...LinkZamowienieKlient" + Environment.NewLine);
                    // Console.WriteLine("Retrieved records:");

                    string ZamowienieIdHashKey, KlientIdHashKey, mail, RecordSource;
                    int ETL_RunId, ServerId, Id, id_klienta;

                    Dictionary<string, string> temp = new Dictionary<string, string>();


                    //check if there are records
                    if (dr.HasRows)
                    {
                        while (dr.Read())
                        {
                            ZamowienieIdHashKey = dr.GetString(1);
                            KlientIdHashKey = dr.GetString(2);
                            ETL_RunId = dr.GetInt32(3);
                            RecordSource = dr.GetString(4);
                            ServerId = dr.GetInt32(5);
                            Id = dr.GetInt32(6);
                            id_klienta = dr.GetInt32(7);
                            mail = dr.GetString(8);

                            Dictionary<string, string> tempin = new Dictionary<string, string>();

                            tempin["ZamowienieKlientHashKey"] = ZamowienieIdHashKey;
                            tempin["KlientIdHashKey"] = KlientIdHashKey;
                            tempin["ETL_RunId"] = ETL_RunId.ToString();
                            tempin["RecordSource"] = RecordSource;
                            tempin["ServerId"] = ServerId.ToString();
                            tempin["Id"] = Id.ToString();
                            tempin["id_klienta"] = id_klienta.ToString();
                            tempin["mail"] = mail;
                            temp[ZamowienieIdHashKey] = KlientIdHashKey;
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
            return new Dictionary<string, string>();
        }


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
        public static void getZgodyMatkeringowe()
        {
            //set the connection string
            string connString = conn_string;

            //variables to store the query results

            try
            {
                //sql connection object
                using (SqlConnection conn = new SqlConnection(connString))
                {
                    //define the SqlCommand object
                    SqlCommand cmd = new SqlCommand(query_get_orders, conn);

                    //open connection
                    conn.Open();

                    //execute the SQLCommand
                    SqlDataReader dr = cmd.ExecuteReader();

                    // Console.WriteLine(Environment.NewLine + "Retrieving data from database...LinkZamowienieKlient" + Environment.NewLine);
                    // Console.WriteLine("Retrieved records:");

                    string ZamowienieIdHashKey, KlientIdHashKey, mail, RecordSource;
                    int ETL_RunId, ServerId, Id, id_klienta;

                    Dictionary<string, string> temp = new Dictionary<string, string>();


                    //check if there are records
                    if (dr.HasRows)
                    {
                        while (dr.Read())
                        {
                            ZamowienieIdHashKey = dr.GetString(1);
                            KlientIdHashKey = dr.GetString(2);
                            ETL_RunId = dr.GetInt32(3);
                            RecordSource = dr.GetString(4);
                            ServerId = dr.GetInt32(5);
                            Id = dr.GetInt32(6);
                            id_klienta = dr.GetInt32(7);
                            mail = dr.GetString(8);

                            Dictionary<string, string> tempin = new Dictionary<string, string>();

                            tempin["ZamowienieKlientHashKey"] = ZamowienieIdHashKey;
                            tempin["KlientIdHashKey"] = KlientIdHashKey;
                            tempin["ETL_RunId"] = ETL_RunId.ToString();
                            tempin["RecordSource"] = RecordSource;
                            tempin["ServerId"] = ServerId.ToString();
                            tempin["Id"] = Id.ToString();
                            tempin["id_klienta"] = id_klienta.ToString();
                            tempin["mail"] = mail;
                            temp[ZamowienieIdHashKey] = KlientIdHashKey;
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

                    //return temp;
                }
            }
            catch (Exception ex)
            {
                //display error message
                Console.WriteLine("Exception: " + ex.Message);
            }
            //return new Dictionary<string, string>();
        }

        public static void ZgodyMarketingowe()
        {
            List<string> nazwyZgod = new List<string>();
            nazwyZgod.Add("ZNOIPPKE"); // Otryzmywanie informacji przy pomocy komunikacji elektronicznej
            nazwyZgod.Add("ZNONV"); // otrzymanie newslettera  vendero - nowości
            nazwyZgod.Add("ZNPDO"); // Przetwarzanie Danych osobowych - 
            nazwyZgod.Add("ZNPDWCM"); // Przetwarzanie danych w celach marketingowych - profilowanie 
         
            // https://forum.insert.com.pl/index.php?/topic/12895-cele-przetwarzania-a-sfera/&tab=comments#comment-85363
            IObiektyBibliotekiDokumentow biblioteka = sfera.PodajObiektTypu<IObiektyBibliotekiDokumentow>();
            IPodmioty podmioty = sfera.PodajObiektTypu<IPodmioty>();
            ICelePrzetwarzania cele = sfera.PodajObiektTypu<ICelePrzetwarzania>();
            var podmitoDoEdycji = podmioty.Dane.Wszystkie().Where(p => p.Id == pid).FirstOrDefault();                	
   
            if(podmitoDoEdycji == null) {}
            else
                {                  
                using (IPodmiot podmiot = podmioty.Znajdz(podmitoDoEdycji))
                {
                // ustawienie zgody na określony cel
  	            var cel1 = cele.Dane.Wszystkie().Where(c => c.NazwaSkrocona == "ZNPDO").FirstOrDefault();
  	            var zgoda1 = podmiot.Dane.Zgody.Where(z => z.CelPrzetwarzaniaId == cel1.Id).FirstOrDefault();
                if(zgoda1==null)
  	                {
  	                zgoda1 = new Zgoda();
	                podmiot.Dane.Zgody.Add(zgoda1);
                    zgoda1.CelPrzetwarzania = cel1;
  	                }    
  	            zgoda1.Status = 2;
                podmiot.Zapisz();  
                }
            }
        }
        
        public static void DodajZamowienie(Dictionary<string, List<string>> Pozycje, Dictionary<string, string> Adres, Dictionary<string, string> Dane, String uwagi, String NIP = null, string firma_nazwa = null, Dictionary<string, string> Firma = null, String dostawa = null, String kosztDostawy = null, string source = null)
        {
            // Launching Sfera
            using (var sfera = UruchomSfere())
            {
                //Declaration of variables used by sfera to create ZK
                IAsortymenty asortyment = sfera.PodajObiektTypu<IAsortymenty>();
                IJednostkiMiar jednostkiMiary = sfera.PodajObiektTypu<IJednostkiMiar>();
                IPodmioty podmioty = sfera.PodajObiektTypu<IPodmioty>();
                // Default ZK configuration 
                Konfiguracja konfZk = sfera.PodajObiektTypu<IKonfiguracje>().DaneDomyslne.ZamowienieOdKlienta;
                Console.WriteLine("Sfera is runnin");

                Magazyn mag = sfera.PodajObiektTypu<IMagazyny>().Dane.Wszystkie().Where(m => m.Symbol == "MAG").FirstOrDefault();

                IZamowieniaOdKlientow zamowienia = sfera.PodajObiektTypu<IZamowieniaOdKlientow>();
                // Creating ZK document
                using (IZamowienieOdKlienta zk = zamowienia.Utworz(konfZk))
                {
                    // Declare zk Magazine
                    zk.Dane.Magazyn = mag;
                    // try for creating object of the customer and filling in data.
                    try
                    {
                        // Checking if customer is a company
                        if(NIP == null)
                        {
                            // Filling in customer details to check if already exists
                            var imie = Dane["imie"];
                            var nazwisko = Dane["nazwisko"];
                            var klient = podmioty.Dane.Wszystkie().Where(p => p.Osoba.Imie == imie && p.Osoba.Nazwisko == nazwisko).FirstOrDefault();
                            // If no client in database found then create one.
                            if (klient == null)
                            {
                                IPodmioty pod = sfera.PodajObiektTypu<IPodmioty>();
                                IPanstwa panstwa = sfera.PodajObiektTypu<IPanstwa>();
                                IRodzajeKontaktuDaneDomyslne rodzajeKontaktuDD = sfera.PodajObiektTypu<IRodzajeKontaktu>().DaneDomyslne;
                                IStanowiskaDaneDomyslne stanowiskaDD = sfera.PodajObiektTypu<IStanowiska>().DaneDomyslne;
                                IDzialyPodmiotuDaneDomyslne dzialyDD = sfera.PodajObiektTypu<IDzialyPodmiotu>().DaneDomyslne;

                                ITypyAdresowDaneDomyslne typyAdresuDD = sfera.PodajObiektTypu<ITypyAdresu>().DaneDomyslne;

                                Podmiot przedstawiciel = null;
                                using (IPodmiot przedstawicielBO = pod.UtworzOsobe())
                                {
                                    przedstawicielBO.Dane.Osoba.Imie = imie;
                                    przedstawicielBO.Dane.Osoba.Nazwisko = nazwisko;
                                    int id = podmioty.Dane.Wszystkie().Select(a => a.Id).Max() + 1;
                                    przedstawicielBO.Dane.Sygnatura.PelnaSygnatura = "KLIENT" + id.ToString();

                                    Kontakt kontakt = new Kontakt();
                                    przedstawicielBO.Dane.Kontakty.Add(kontakt);
                                    kontakt.Rodzaj = rodzajeKontaktuDD.Telefon;
                                    kontakt.Wartosc = Dane["telefon"].ToString();
                                    kontakt.Podstawowy = true;
                                        
                                    Kontakt kontakt2 = new Kontakt();
                                    przedstawicielBO.Dane.Kontakty.Add(kontakt2);
                                    kontakt2.Rodzaj = rodzajeKontaktuDD.Email;
                                    kontakt2.Wartosc = Dane["mail"];
                                    kontakt2.Podstawowy = true;

                                    //Dodawanie adresu Podmiotu
                                    AdresPodmiotu adresPO = przedstawicielBO.DodajAdres();
                                    adresPO.Szczegoly.Ulica = Adres["adres"] + " " + Adres["adres2"];
                                    adresPO.Szczegoly.NrDomu = "";
                                    adresPO.Szczegoly.KodPocztowy = Adres["kod"];
                                    adresPO.Szczegoly.Miejscowosc = Adres["miasto"];
                                    adresPO.Panstwo = panstwa.Dane.Wszystkie().Where(p => p.Nazwa.CompareTo("Polska") == 0).FirstOrDefault();
                                        

                                    if (!przedstawicielBO.Zapisz())
                                    {
                                        Console.WriteLine("Nie udało zapisać się podmiotu");
                                        BladInfo[] bledy = sfera.PodajBledy(przedstawicielBO);
                                        foreach (var blad in bledy)
                                        {
                                            Console.WriteLine("Ważność: {0}, Informacja: {1}",
                                                blad.Waznosc.ToString(), blad.Tresc);
                                        }
                                    }  
                                    else
                                    {
                                        przedstawiciel = przedstawicielBO.Dane;
                                        Console.WriteLine("Zapisano przedstawicielBO");
                                    }
                                    klient = przedstawiciel;
                                }
                                zk.Dane.Podmiot = klient;
                                zk.Dane.Uwagi =  uwagi + " \n" +
                                    Dane["mail"] + " \n" +
                                    Dane["telefon"];
                                zk.Dane.DataWprowadzenia = DateTime.Now;
                            }
                            // if client found or created assign him to ZK document
                            else
                            {
                                zk.Dane.Podmiot = klient;
                                zk.Dane.Uwagi = uwagi + " \n" +
                                    Dane["mail"] + " \n" +
                                    Dane["telefon"];
                                zk.Dane.DataWprowadzenia = DateTime.Now;
                            }
                        }
                        else
                        {
                            // if client is a person NOT a company process is the same.
                            var klient = podmioty.Dane.Wszystkie().Where(p => p.NIP == NIP).FirstOrDefault();
                            // check if no client found
                            if (klient == null)
                            {
                                IPodmioty pod = sfera.PodajObiektTypu<IPodmioty>();
                                IPanstwa panstwa = sfera.PodajObiektTypu<IPanstwa>();
                                IRodzajeKontaktuDaneDomyslne rodzajeKontaktuDD = sfera.PodajObiektTypu<IRodzajeKontaktu>().DaneDomyslne;
                                IStanowiskaDaneDomyslne stanowiskaDD = sfera.PodajObiektTypu<IStanowiska>().DaneDomyslne;
                                IDzialyPodmiotuDaneDomyslne dzialyDD = sfera.PodajObiektTypu<IDzialyPodmiotu>().DaneDomyslne;

                                ITypyAdresowDaneDomyslne typyAdresuDD = sfera.PodajObiektTypu<ITypyAdresu>().DaneDomyslne;

                                Podmiot przedstawiciel = null;
                                using (IPodmiot przedstawicielBO = pod.UtworzOsobe())
                                {
                                    przedstawicielBO.Dane.Osoba.Imie = Dane["imie"];
                                    przedstawicielBO.Dane.Osoba.Nazwisko = Dane["nazwisko"];

                                    //Dodawanie adresu Podmiotu
                                    AdresPodmiotu adresPO = przedstawicielBO.DodajAdres();
                                    adresPO.Szczegoly.Ulica = Adres["adres"] + " " + Adres["adres2"];
                                    adresPO.Szczegoly.KodPocztowy = Adres["kod"];
                                    adresPO.Szczegoly.Miejscowosc = Adres["miasto"];
                                    adresPO.Panstwo = panstwa.Dane.Wszystkie().Where(p => p.Nazwa.CompareTo("Polska") == 0).FirstOrDefault();

                                    if (!przedstawicielBO.Zapisz())
                                    {
                                        Console.WriteLine("Nie udało zapisać się podmiotu");
                                        BladInfo[] bledy = sfera.PodajBledy(przedstawicielBO);
                                        foreach (var blad in bledy)
                                        {
                                            Console.WriteLine("Ważność: {0}, Informacja: {1}",
                                                blad.Waznosc.ToString(), blad.Tresc);
                                        }
                                    }
                                    else
                                    {
                                        przedstawiciel = przedstawicielBO.Dane;
                                        Console.WriteLine("Zapisano przedstawicielBO");
                                    }
                                    klient = przedstawiciel;
                                }
                                using (IPodmiot podmiotBO = podmioty.UtworzFirme())
                                {
                                    // podmiotBO.Dane.Gus
                                    // creating company
                                    int id = podmioty.Dane.Wszystkie().Select(a => a.Id).Max() + 1;
                                    podmiotBO.Dane.Sygnatura.PelnaSygnatura = "KLIENT" + id.ToString();
                                    podmiotBO.Dane.Firma.Nazwa = firma_nazwa;
                                    podmiotBO.Dane.NIP = NIP;
                                    podmiotBO.Dane.NazwaSkrocona = podmiotBO.Dane.Firma.Nazwa;

                                    AdresPodmiotu adresFirmy = podmiotBO.DodajAdres();

                                    adresFirmy.Szczegoly.Ulica = Firma["firma_adres"] + " " + Firma["firma_adres2"];
                                    adresFirmy.Szczegoly.Miejscowosc = Firma["firma_miasto"];
                                    adresFirmy.Szczegoly.KodPocztowy = Firma["firma_kod"];
                                    adresFirmy.Panstwo = panstwa.Dane.Wszystkie().Where(p => p.Nazwa.CompareTo("Polska") == 0).FirstOrDefault();

                                    Kontakt kontakt = new Kontakt();
                                    podmiotBO.Dane.Kontakty.Add(kontakt);
                                    kontakt.Rodzaj = rodzajeKontaktuDD.Telefon;
                                    kontakt.Wartosc = Dane["telefon"];
                                    kontakt.Podstawowy = true;

                                    Kontakt kontakt2 = new Kontakt();
                                    podmiotBO.Dane.Kontakty.Add(kontakt2);
                                    kontakt2.Rodzaj = rodzajeKontaktuDD.Email;
                                    kontakt2.Wartosc = Dane["mail"];
                                    kontakt2.Podstawowy = true;

                                    if (przedstawiciel != null)
                                    {
                                        Przedstawiciel p = podmiotBO.Przedstawiciele.Dodaj(przedstawiciel);
                                        p.Stanowisko = stanowiskaDD.Zaopatrzeniowiec;
                                        p.Dzial = dzialyDD.Zaopatrzenie;
                                    }

                                    if (!podmiotBO.Zapisz())
                                    {
                                        Console.WriteLine("Błąd zapisu podmiotu... \n");
                                        BladInfo[] bledy = sfera.PodajBledy(podmiotBO);
                                        foreach (var blad in bledy)
                                        {
                                            Console.WriteLine("Ważność: {0}, Informacja: {1}",
                                                blad.Waznosc.ToString(), blad.Tresc);
                                        }

                                    }
                                }
                                // if company assign to the ZK document
                                zk.Dane.Podmiot = podmioty.Dane.Wszystkie().Where(p => p.NIP == NIP).FirstOrDefault();
                                // Adding order details
                                zk.Dane.Uwagi = uwagi + " \n" +
                                    Dane["mail"] + " \n" +
                                    Dane["telefon"];
                                // Date of adding is filled
                                zk.Dane.DataWprowadzenia = DateTime.Now;
                            }
                            else
                            {
                                // same as company above.
                                zk.Dane.Podmiot = klient;
                                zk.Dane.Uwagi = uwagi + " \n" +
                                    Dane["mail"] + " \n" +
                                    Dane["telefon"];

                                zk.Dane.DataWprowadzenia = DateTime.Now;
                            }


                        }
                           
                        // Adding products to the order 
                        for (int i = 0; i < Pozycje["symbol"].Count; i++)
                        {
                            if(Pozycje["symbol"][i] != "" || Pozycje["symbol"][i] != null)
                            {
                                try
                                {

                                    var pozycja = Pozycje["symbol"][i];
                                    // 
                                    Asortyment a = asortyment.Dane.Wszystkie().Where(t => t.Symbol == pozycja).FirstOrDefault();
                                    var poz = zk.Pozycje.Dodaj(a, Int32.Parse(Pozycje["ilosc"][i]), a.JednostkaSprzedazy);
                                    poz.Cena.BruttoPrzedRabatem = Convert.ToDecimal(Pozycje["cena"][i].Replace(".", ","));
                                    zk.Przelicz();
                                }
                                catch (NullReferenceException)
                                {
                                    Console.WriteLine(Pozycje["symbol"][i]);
                                }
                                    
                            }

                            //else
                            //{
                                //Asortyment a = new Asortyment();
                                //  a.Nazwa = 
                            //}
                               
                        }
                        if (dostawa != null)
                        {
                            if (dostawa.Contains("DPD"))
                            {
                                Asortyment a = asortyment.Dane.Wszystkie().Where(t => t.Symbol == "DPD").FirstOrDefault();
                                var poz = zk.Pozycje.Dodaj(a, 1, a.JednostkaSprzedazy);
                                poz.Cena.BruttoPrzedRabatem = Convert.ToDecimal(kosztDostawy.Replace(".", ","));
                                zk.Przelicz();
                            }
                            if (dostawa.Contains("paczkomacie"))
                            {
                                Asortyment a = asortyment.Dane.Wszystkie().Where(t => t.Symbol == "PACZ WYS").FirstOrDefault();
                                var poz = zk.Pozycje.Dodaj(a, 1, a.JednostkaSprzedazy);
                                poz.Cena.BruttoPrzedRabatem = Convert.ToDecimal(kosztDostawy.Replace(".", ","));
                                zk.Przelicz();
                            }
                                
                        }
                        zk.Dane.TerminRealizacji = DateTime.Today;
                        zk.Dane.WystawilaOsoba = podmioty.Dane.Wszystkie().Where(p => p.Osoba != null && p.NazwaSkrocona == "Dinar Dinar").FirstOrDefault().Osoba;
                        //take a look inside

                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                        
                    if (zk.Zapisz())
                    {
                        Console.WriteLine(zk.Dane.NumerWewnetrzny.PelnaSygnatura);
                    }
                    else
                    {
                        Console.WriteLine("Błędy:");
                        BladInfo[] bledy = sfera.PodajBledy(zk);
                        foreach (var blad in bledy)
                        {
                            Console.WriteLine("Ważność: {0}, Informacja: {1}",
                                blad.Waznosc.ToString(), blad.Tresc);
                        }


                        Console.WriteLine(String.Join(Environment.NewLine, zk.Bledy.Select(b => b.ToString()).ToArray()));
                    }
                    Console.WriteLine("Błędy:");
                    BladInfo[] bledy1 = sfera.PodajBledy(zk);
                    foreach (var blad in bledy1)
                    {
                        Console.WriteLine("Ważność: {0}, Informacja: {1}",
                            blad.Waznosc.ToString(), blad.Tresc);
                    }
                }
            }

            Console.WriteLine("Ending .... ");
        }

        public static void DodajKlienta()
        {
            using (var sfera = UruchomSfere())
            {
                IPodmioty podmioty = sfera.PodajObiektTypu<IPodmioty>();
                IPanstwa panstwa = sfera.PodajObiektTypu<IPanstwa>();
                IRodzajeKontaktuDaneDomyslne rodzajeKontaktuDD = sfera.PodajObiektTypu<IRodzajeKontaktu>().DaneDomyslne;
                IStanowiskaDaneDomyslne stanowiskaDD = sfera.PodajObiektTypu<IStanowiska>().DaneDomyslne;
                IDzialyPodmiotuDaneDomyslne dzialyDD = sfera.PodajObiektTypu<IDzialyPodmiotu>().DaneDomyslne;

                ITypyAdresowDaneDomyslne typyAdresuDD = sfera.PodajObiektTypu<ITypyAdresu>().DaneDomyslne;

                Podmiot przedstawiciel = null;
                using (IPodmiot przedstawicielBO = podmioty.UtworzOsobe())
                {
                    przedstawicielBO.Dane.Osoba.Imie = "Zenon";
                    przedstawicielBO.Dane.Osoba.Nazwisko = "Fijałkowski";
                    
                    //Dodawanie adresu Podmiotu
                    AdresPodmiotu adresPO= przedstawicielBO.DodajAdres();
                    adresPO.Szczegoly.Ulica = "Cyprysowa";
                    adresPO.Szczegoly.NrDomu = "42";
                    adresPO.Szczegoly.KodPocztowy = "43-512";
                    adresPO.Szczegoly.Miejscowosc = "Bestwina";
                    adresPO.Panstwo = panstwa.Dane.Wszystkie().Where(p => p.Nazwa.CompareTo("Polska") == 0).FirstOrDefault();

                    if (!przedstawicielBO.Zapisz())
                        Console.WriteLine("Nie udało zapisać się podmiotu");
                    else
                    {
                        przedstawiciel = przedstawicielBO.Dane;
                        Console.WriteLine("Zapisano przedstawicielBO");
                    }
                        
                }

                using (IPodmiot podmiotBO = podmioty.UtworzFirme())
                {
                    // podmiotBO.Dane.Gus
                    int id = podmioty.Dane.Wszystkie().Select(a => a.Id).Max() + 1;
                    podmiotBO.Dane.Sygnatura.PelnaSygnatura = "KLIENT" + id.ToString();
                    podmiotBO.Dane.Firma.Nazwa = "KLIENT" + id.ToString();
                    podmiotBO.Dane.NIP = "1111111111";
                    podmiotBO.Dane.NazwaSkrocona = podmiotBO.Dane.Firma.Nazwa;

                    AdresPodmiotu adres = podmiotBO.DodajAdres();
                    adres.Szczegoly.Ulica = "ul. Szkolna";
                    adres.Szczegoly.NrDomu = "11";
                    adres.Szczegoly.KodPocztowy = "55-100";
                    adres.Szczegoly.Miejscowosc = "Trzebnica";
                    adres.Panstwo = panstwa.Dane.Wszystkie().Where(p => p.Nazwa.CompareTo("Polska") == 0).FirstOrDefault();

                    AdresPodmiotu adresK = podmiotBO.DodajAdres(typyAdresuDD.Korespondencyjny);
                    adresK.Nazwa = "Prawnicy s.c.";
                    adresK.Linia1 = "Green Tower p. 212";
                    adresK.Linia2 = "Rynek 1";
                    adresK.Linia3 = "55-100 Trzebnica";

                    Kontakt kontakt = new Kontakt();
                    podmiotBO.Dane.Kontakty.Add(kontakt);
                    kontakt.Rodzaj = rodzajeKontaktuDD.Telefon;
                    kontakt.Wartosc = "71 373 88 99";
                    kontakt.Podstawowy = true;

                    RachunekBankowy rachunek = new RachunekBankowy();
                    podmiotBO.Dane.Rachunki.Add(rachunek);
                    rachunek.Nazwa = "Główny rachunek w NASZ Bank";
                    rachunek.Numer = "42 1234 5678 0000 0000 1234 5678";

                    if (przedstawiciel != null)
                    {
                        Przedstawiciel p = podmiotBO.Przedstawiciele.Dodaj(przedstawiciel);
                        p.Stanowisko = stanowiskaDD.Zaopatrzeniowiec;
                        p.Dzial = dzialyDD.Zaopatrzenie;
                    }

                    if (!podmiotBO.Zapisz())
                        Console.WriteLine("Błąd zapisu podmiotu... \n");
                }
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
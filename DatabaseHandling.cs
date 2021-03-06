using InsERT.Moria.Asortymenty;
using InsERT.Moria.BibliotekaDokumentow;
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

        public static void ZgodyMarketingowe()
        {
            // Launching Sfera
            using (var sfera = UruchomSfere())
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
                var podmiotDoEdycji = podmioty.Dane.Wszystkie().Where(p => p.Id == pid).FirstOrDefault();                	

                if(podmiotDoEdycji == null) {}
                else
                    {                  
                    using (IPodmiot podmiot = podmioty.Znajdz(podmiotDoEdycji))
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
    }
}
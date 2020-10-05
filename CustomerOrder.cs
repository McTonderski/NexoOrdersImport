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
    }
}
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
    }
}
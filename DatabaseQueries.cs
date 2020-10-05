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

    }
}
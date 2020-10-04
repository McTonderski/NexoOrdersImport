namespace NexoTestApp
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
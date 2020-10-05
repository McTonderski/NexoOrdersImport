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
}
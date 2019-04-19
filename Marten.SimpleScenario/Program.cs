using System;
using Marten;

//using MartenLib;

namespace MartenCS
{
    internal class AccountTransaction
    {
        public Guid Id { get; set; }
        public DateTime Date { get; set; }
        public string Description { get; set; }
        public decimal Amount { get; set; }
    }

    internal class BankAccount
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public decimal Total { get; set; }

        public BankAccount()
        {
        }

        public BankAccount(Guid id, string name, decimal total)
        {
            Id = id;
            Name = name;
            Total = total;
        }

        public void Apply(AccountTransaction trn)
        {
            Total += trn.Amount;
        }

        public void Apply(BankAccount acc)
        {
            Name = acc.Name;
        }
    }

    internal class Program
    {
        private static Guid streamID = Guid.NewGuid();

        private static void WriteData(DocumentStore store)
        {
            store.Advanced.Clean.CompletelyRemoveAll();

            var bankAccount = new BankAccount { Id = streamID, Name = "Sandeep Chandra", Total = 0 };

            using (var session = store.OpenSession())
            {
                var tran1 = new AccountTransaction { Id = Guid.NewGuid(), Date = DateTime.Now, Description = "Open Account", Amount = 1000m };
                var tran2 = new AccountTransaction { Id = Guid.NewGuid(), Date = DateTime.Now, Description = "Westpac transfer", Amount = 500m };
                session.Events.StartStream<BankAccount>(streamID, bankAccount, tran1, tran2);
                session.SaveChanges();

                var tran3 = new AccountTransaction { Id = Guid.NewGuid(), Date = DateTime.Now, Description = "Transfer to ANZ", Amount = -200m };
                session.Events.Append(streamID, tran3);
                session.SaveChanges();
            }
        }

        private static void ReadData(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                //var events = session.Events.AggregateStream<BankAccount>(streamID);
                var id = Guid.Parse("e037ea1e-c8d2-466a-a8af-9cb1bd12b49c");
                session.Events.FetchStream(id);
                var events = session.Events.AggregateStream<BankAccount>(id);
                System.Diagnostics.Debug.WriteLine(events);
            }
        }

        private static void Main(string[] args)
        {
            var conStr = "User ID=notyou;Password=secret;Host=localhost;Port=5432;Database=MartenTestCS;Pooling=true;";
            var store = DocumentStore.For(x =>
            {
                x.Connection(conStr);
                x.Events.InlineProjections.AggregateStreamsWith<BankAccount>();
                //x.Events.AddEventType(typeof(BankAccount));
                //x.Events.AddEventType(typeof(AccountTransaction));
            });
            //WriteData(store);
            ReadData(store);
        }
    }
}
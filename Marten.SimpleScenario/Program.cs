using System;
using Marten;

//using MartenLib;

namespace MartenCS
{
    internal class AccountTransactionCreated
    {
        public Guid Id { get; set; }
        public DateTime Date { get; set; }
        public string Description { get; set; }
        public decimal Amount { get; set; }
    }

    internal class BankAccountCreated
    {
        public Guid AccountId { get; set; }
        public string Name { get; set; }
        public decimal Total { get; set; }
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

        public void Apply(AccountTransactionCreated @event)
        {
            Total += @event.Amount;
        }

        public void Apply(BankAccountCreated @event)
        {
            Id = @event.AccountId;
            Name = @event.Name;
            Total = 0;
        }
    }

    internal class Program
    {
        private static Guid streamID = Guid.NewGuid();

        private static void WriteData(DocumentStore store)
        {
            store.Advanced.Clean.CompletelyRemoveAll();

            using (var session = store.OpenSession())
            {
                var bankAccountCreated = new BankAccountCreated { AccountId = streamID, Name = "Sandeep Chandra", Total = 0 };
                var firstTransactionCreated = new AccountTransactionCreated { Id = Guid.NewGuid(), Date = DateTime.Now, Description = "Open Account", Amount = 1000m };
                var secondTransactionCreated = new AccountTransactionCreated { Id = Guid.NewGuid(), Date = DateTime.Now, Description = "Westpac transfer", Amount = 500m };
                session.Events.StartStream<BankAccount>(streamID, bankAccountCreated, firstTransactionCreated, secondTransactionCreated);
                session.SaveChanges();

                var thirdTransactionCreated = new AccountTransactionCreated { Id = Guid.NewGuid(), Date = DateTime.Now, Description = "Transfer to ANZ", Amount = -200m };
                session.Events.Append(streamID, thirdTransactionCreated);
                session.SaveChanges();
            }
        }

        private static void ReadData(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Events.FetchStream(streamID);
                var events = session.Events.AggregateStream<BankAccount>(streamID);
                System.Diagnostics.Debug.WriteLine(events);
            }
        }

        private static void Main(string[] args)
        {
            //var conStr = "User ID=notyou;Password=secret;Host=localhost;Port=5432;Database=MartenTestCS;Pooling=true;";
            var conStr = "PORT = 5432; HOST = 127.0.0.1; TIMEOUT = 15; POOLING = True; MINPOOLSIZE = 1; MAXPOOLSIZE = 100; COMMANDTIMEOUT = 20; DATABASE = 'postgres'; PASSWORD = 'Password12!'; USER ID = 'postgres'";
            var store = DocumentStore.For(x =>
            {
                x.Connection(conStr);
                x.Events.InlineProjections.AggregateStreamsWith<BankAccount>();
                //x.Events.AddEventType(typeof(BankAccount));
                //x.Events.AddEventType(typeof(AccountTransaction));
            });
            WriteData(store);
            ReadData(store);
        }
    }
}
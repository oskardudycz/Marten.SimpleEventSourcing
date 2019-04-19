using System;
using System.Collections;
using System.Diagnostics;
using Marten;
using Marten.Events;
using Newtonsoft.Json;

//using MartenLib;

namespace MartenCS
{
    internal interface IAggregate
    {
        Guid Id { get; }

        object[] DequeuePendingEvents();
    }

    internal abstract class Aggregate : IAggregate
    {
        public Guid Id { get; set; }

        [JsonIgnore]
        protected Queue PendingEvents { get; private set; } = new Queue();

        public object[] DequeuePendingEvents()
        {
            var result = PendingEvents.ToArray();
            PendingEvents = new Queue();
            return result;
        }

        protected void Append(object @event)
        {
            PendingEvents.Enqueue(@event);
        }
    }

    internal static class AggregateExtensions
    {
        internal static void Add(this IEventStore eventStore, IAggregate aggregate)
        {
            eventStore.StartStream(aggregate.Id, aggregate.DequeuePendingEvents());
        }

        internal static void Update(this IEventStore eventStore, IAggregate aggregate)
        {
            eventStore.Append(aggregate.Id, aggregate.DequeuePendingEvents());
        }
    }

    internal class BankAccountCreated
    {
        public Guid AccountId { get; }
        public string Name { get; }
        public decimal Total { get; }

        public BankAccountCreated(
            Guid accountId,
            string name,
            decimal total)
        {
            AccountId = accountId;
            Name = name;
            Total = total;
        }
    }

    internal class AccountTransactionCreated
    {
        public Guid TransactionId { get; }
        public DateTime Date { get; }
        public decimal Amount { get; }
        public string Description { get; }

        public AccountTransactionCreated(
            Guid transactionId,
            DateTime date,
            decimal amount,
            string description)
        {
            TransactionId = transactionId;
            Date = date;
            Description = description;
            Amount = amount;
        }
    }

    internal class BankAccount : Aggregate
    {
        public string Name { get; private set; }
        public decimal Total { get; private set; }
        public long TransactionCount { get; private set; }

        public BankAccount()
        {
        }

        public BankAccount(Guid id, string name, decimal total)
        {
            var @event = new BankAccountCreated(
                id,
                name,
                total
            );

            Append(@event);
            Apply(@event);
        }

        public void AddTransaction(decimal amount, string description)
        {
            var @event = new AccountTransactionCreated(
                Guid.NewGuid(),
                DateTime.Now,
                amount,
                description
            );

            Append(@event);
            Apply(@event);
        }

        public void Apply(BankAccountCreated @event)
        {
            Id = @event.AccountId;
            Name = @event.Name;
            Total = @event.Total;
        }

        public void Apply(AccountTransactionCreated @event)
        {
            Total += @event.Amount;
            TransactionCount++;
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
                var account = new BankAccount(streamID, "Sandeep Chandra", 0);
                account.AddTransaction(1000m, "Open Account");
                account.AddTransaction(500m, "Westpac transfer");
                account.AddTransaction(-200m, "Transfer to ANZ");

                session.Events.Add(account);

                session.SaveChanges();
                account.AddTransaction(-100m, "Transfer to GMBH");

                session.Events.Update(account);
                session.SaveChanges();
            }
        }

        private static void ReadData(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Events.FetchStream(streamID);

                var account = session.Events.AggregateStream<BankAccount>(streamID);

                Debug.WriteLine(account);

                Debug.Assert(account.Id == streamID);
                Debug.Assert(account.Name == "Sandeep Chandra");
                Debug.Assert(account.TransactionCount == 4);
                Debug.Assert(account.Total == 1200m);
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
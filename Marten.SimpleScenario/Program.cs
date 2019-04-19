using System;
using System.Collections;
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

    internal class AccountTransactionCreated
    {
        public Guid TransactionId { get; set; }
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

    internal class BankAccount : Aggregate
    {
        public string Name { get; set; }
        public decimal Total { get; set; }

        public BankAccount()
        {
        }

        public BankAccount(Guid id, string name, decimal total)
        {
            var @event = new BankAccountCreated
            {
                AccountId = id,
                Name = name,
                Total = total
            };

            Append(@event);
            Apply(@event);
        }

        public void AddTransaction(decimal amount, string description)
        {
            var @event = new AccountTransactionCreated
            {
                TransactionId = Guid.NewGuid(),
                Date = DateTime.Now,
                Amount = amount,
                Description = description
            };

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
                System.Diagnostics.Debug.WriteLine(account);
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
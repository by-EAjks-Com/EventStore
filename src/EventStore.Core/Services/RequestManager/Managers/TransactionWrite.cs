﻿using System;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Plugins.Authorization;

namespace EventStore.Core.Services.RequestManager.Managers {
	public class TransactionWrite : RequestManagerBase {
		private static readonly Operation Operation = new Operation(Operations.Streams.Write);
		private readonly Event[] _events;
		private long _transactionId;

		public TransactionWrite(
					IPublisher publisher,
					TimeSpan timeout,
					IEnvelope clientResponseEnvelope,
					Guid internalCorrId,
					Guid clientCorrId,
					Event[] events,
					long transactionId,
					CommitSource commitSource)
			: base(
					 publisher,
					 timeout,
					 clientResponseEnvelope,
					 internalCorrId,
					 clientCorrId,
					 expectedVersion: -1,
					 commitSource,
					 //prepareCount: events.Length,
					 transactionId) {
			_events = events;
			_transactionId = transactionId;
			Result = OperationResult.PrepareTimeout; // we need an unknown here
		}

		protected override Message WriteRequestMsg =>
			new StorageMessage.WriteTransactionData(
					InternalCorrId,
					WriteReplyEnvelope,
					TransactionId,
					_events);

		protected override void AllEventsWritten() {
			if (!Registered) {
				var tokenSource = new CancellationTokenSource(Timeout);
				var cancellationToken = tokenSource.Token;
				try {
					CommitSource
						.WaitForReplication(LastEventPosition, cancellationToken)
						.ContinueWith((_) => RequestCompleted());
				} catch {
					CancelRequest();
				} finally { tokenSource.Dispose(); }
				Registered = true;
			}
		}

		protected override Task WaitForLocalCommit() {
			throw new NotImplementedException();
		}

		protected override Task WaitForClusterCommit() {
			throw new NotImplementedException();
		}

		protected override Task WaitForLocalIndex() {
			throw new NotImplementedException();
		}

		public override void Handle(StorageMessage.PrepareAck message) {
			throw new NotImplementedException();
		}

		public override void Handle(StorageMessage.CommitIndexed message) {
			throw new NotImplementedException();
		}

		protected override Message ClientSuccessMsg =>
			 new ClientMessage.TransactionWriteCompleted(
						ClientCorrId,
						TransactionId,
						OperationResult.Success,
						null);
		protected override Message ClientFailMsg =>
			 new ClientMessage.TransactionWriteCompleted(
						ClientCorrId,
						TransactionId,
						Result,
						FailureMessage);
	}
}

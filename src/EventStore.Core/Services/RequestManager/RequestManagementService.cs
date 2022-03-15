﻿using System;
using System.Collections.Generic;
using EventStore.Common.Utils;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.RequestManager.Managers;
using EventStore.Core.Data;
using EventStore.Core.Services.Histograms;
using System.Linq;
using System.Threading;

namespace EventStore.Core.Services.RequestManager {
	public class RequestManagementService :
		IHandle<ClientMessage.WriteEvents>,
		IHandle<ClientMessage.DeleteStream>,
		IHandle<ClientMessage.TransactionStart>,
		IHandle<ClientMessage.TransactionWrite>,
		IHandle<ClientMessage.TransactionCommit>,
		IHandle<StorageMessage.RequestCompleted>,
		IHandle<StorageMessage.AlreadyCommitted>,
		IHandle<StorageMessage.PrepareAck>,
		IHandle<ReplicationTrackingMessage.ReplicatedTo>,
		IHandle<ReplicationTrackingMessage.IndexedTo>,
		IHandle<StorageMessage.CommitIndexed>,
		IHandle<StorageMessage.WrongExpectedVersion>,
		IHandle<StorageMessage.InvalidTransaction>,
		IHandle<StorageMessage.StreamDeleted>,
		IHandle<SystemMessage.StateChangeMessage> {
		private readonly IPublisher _mainBus;
		private readonly IPublisher _requestManagerQueue;
		private readonly Dictionary<Guid, RequestManagerBase> _currentRequests = new Dictionary<Guid, RequestManagerBase>();
		private const string _requestManagerHistogram = "request-manager";
		private readonly TimeSpan _prepareTimeout;
		private readonly TimeSpan _commitTimeout;
		private CommitSource _commitSource;
		private readonly bool _explicitTransactionsSupported;
		private VNodeState _nodeState;
		private DelaySendService _laterSvc;

		public RequestManagementService(
			IPublisher mainBus,
			IPublisher requestManagerQueue,
			TimeSpan prepareTimeout,
			TimeSpan commitTimeout,
			bool explicitTransactionsSupported) {
			Ensure.NotNull(mainBus, nameof(mainBus));
			Ensure.NotNull(requestManagerQueue, nameof(requestManagerQueue));
			_mainBus = mainBus;
			_requestManagerQueue = requestManagerQueue;
			_prepareTimeout = prepareTimeout;
			_commitTimeout = commitTimeout;
			_commitSource = new CommitSource();
			_explicitTransactionsSupported = explicitTransactionsSupported;
			_laterSvc = new DelaySendService(_requestManagerQueue);
		}

		public void Handle(ClientMessage.WriteEvents message) {
			if (_nodeState != VNodeState.Leader) { return; }
			var manager = new WriteEvents(
								_mainBus,
								_commitTimeout,
								message.Envelope,
								message.InternalCorrId,
								message.CorrelationId,
								message.EventStreamId,
								message.ExpectedVersion,
								message.Events,
								_commitSource,
								message.CancellationToken);
			_currentRequests.Add(message.InternalCorrId, manager);
			manager.Start();
		}

		public void Handle(ClientMessage.DeleteStream message) {
			if (_nodeState != VNodeState.Leader) { return; }
			var manager = new DeleteStream(
								_mainBus,
								_commitTimeout,
								message.Envelope,
								message.InternalCorrId,
								message.CorrelationId,
								message.EventStreamId,
								message.ExpectedVersion,
								message.HardDelete,
								_commitSource,
								message.CancellationToken);
			_currentRequests.Add(message.InternalCorrId, manager);
			manager.Start();
		}

		public void Handle(ClientMessage.TransactionStart message) {
			if (_nodeState != VNodeState.Leader) { return; }
			if (!_explicitTransactionsSupported) {
				var reply = new ClientMessage.TransactionStartCompleted(
					message.CorrelationId,
					default,
					OperationResult.InvalidTransaction,
					"Explicit transactions are not supported");
				message.Envelope.ReplyWith(reply);
				return;
			}

			var manager = new TransactionStart(
								_mainBus,
								_prepareTimeout,
								message.Envelope,
								message.InternalCorrId,
								message.CorrelationId,
								message.EventStreamId,
								message.ExpectedVersion,
								_commitSource);
			_currentRequests.Add(message.InternalCorrId, manager);
			manager.Start();
		}

		public void Handle(ClientMessage.TransactionWrite message) {
			if (_nodeState != VNodeState.Leader) { return; }
			if (!_explicitTransactionsSupported) {
				var reply = new ClientMessage.TransactionWriteCompleted(
					message.CorrelationId,
					default,
					OperationResult.InvalidTransaction,
					"Explicit transactions are not supported");
				message.Envelope.ReplyWith(reply);
				return;
			}

			var manager = new TransactionWrite(
								_mainBus,
								_prepareTimeout,
								message.Envelope,
								message.InternalCorrId,
								message.CorrelationId,
								message.Events,
								message.TransactionId,
								_commitSource);
			_currentRequests.Add(message.InternalCorrId, manager);
			manager.Start();
		}

		public void Handle(ClientMessage.TransactionCommit message) {
			if (_nodeState != VNodeState.Leader) { return; }
			if (!_explicitTransactionsSupported) {
				var reply = new ClientMessage.TransactionCommitCompleted(
					message.CorrelationId,
					default,
					OperationResult.InvalidTransaction,
					"Explicit transactions are not supported");
				message.Envelope.ReplyWith(reply);
				return;
			}

			var manager = new TransactionCommit(
								_mainBus,
								_commitTimeout,
								message.Envelope,
								message.InternalCorrId,
								message.CorrelationId,
								message.TransactionId,
								_commitSource);
			_currentRequests.Add(message.InternalCorrId, manager);
			manager.Start();
		}


		public void Handle(SystemMessage.StateChangeMessage message) {
			if (_nodeState != message.State) {
				//state change == epoch change and the commit source is only valid for the current epoch
				var commitSource = new CommitSource();
				var previousEpoch = Interlocked.Exchange(ref _commitSource, commitSource);
				//todo: do we need to do anything with the old epoch or will clearing _currentRequests cover it?
			}
			_nodeState = message.State;

			if (_nodeState != VNodeState.Leader) {
				if (_currentRequests.Any()) {
					foreach (var request in _currentRequests.Values) {
						request.CancelRequest();
					}
				}
			}
		}

		public void Handle(StorageMessage.RequestCompleted message) {

			if (_currentRequests.TryGetValue(message.CorrelationId, out var manager)) {
				HistogramService.SetValue(_requestManagerHistogram, _requestServiceStopwatch.ElapsedMilliseconds - manager.StartOffset);
			}

			if (!_currentRequests.Remove(message.CorrelationId))
				throw new InvalidOperationException("Should never complete request twice.");

			if (_nodeState == VNodeState.ResigningLeader && !_currentRequests.Any()) {
				_mainBus.Publish(new SystemMessage.RequestQueueDrained());
			}
		}		

		public void Handle(ReplicationTrackingMessage.ReplicatedTo message) => _commitSource.Handle(message);
		public void Handle(ReplicationTrackingMessage.IndexedTo message) => _commitSource.Handle(message);

		public void Handle(StorageMessage.AlreadyCommitted message) => DispatchInternal(message.CorrelationId, message);
		public void Handle(StorageMessage.PrepareAck message) => DispatchInternal(message.CorrelationId, message);
		public void Handle(StorageMessage.CommitIndexed message) => DispatchInternal(message.CorrelationId, message);
		public void Handle(StorageMessage.WrongExpectedVersion message) => DispatchInternal(message.CorrelationId, message);
		public void Handle(StorageMessage.InvalidTransaction message) => DispatchInternal(message.CorrelationId, message);
		public void Handle(StorageMessage.StreamDeleted message) => DispatchInternal(message.CorrelationId, message);

		private void DispatchInternal<T>(Guid correlationId, T message) where T : Message {
			if (_currentRequests.TryGetValue(correlationId, out var manager)) {
				var x = manager as IHandle<T>;
				x?.Handle(message);
			}
		}
	}
}

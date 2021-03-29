using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Tests.Fakes;
using NUnit.Framework;
using EventStore.Core.Services.RequestManager.Managers;
using EventStore.Core.Tests.Bus.Helpers;
using EventStore.Core.Tests.Helpers;

namespace EventStore.Core.Tests.Services.RequestManagement.DeleteMgr {
	public class when_delete_stream_gets_timeout_after_commit : RequestManagerSpecification<DeleteStream> {
		private long _commitPosition = 3000;
		protected override DeleteStream OnManager(FakePublisher publisher) {
			return new DeleteStream(
				publisher,
				1,
				CommitTimeout, 
				Envelope,
				InternalCorrId,
				ClientCorrId,
				"test123",
				ExpectedVersion.Any,
				false,
				CommitSource);
		}

		protected override IEnumerable<Message> WithInitialMessages() {
			yield return new StorageMessage.CommitIndexed(InternalCorrId, _commitPosition, 500, 1, 1);
			yield return new ReplicationTrackingMessage.ReplicatedTo(_commitPosition);
		}

		protected override Message When() {			
			Manager.PhaseTimeout(2);
			return new TestMessage();
		}

		[Test]
		public void failed_request_message_is_published() {
			Assert.That(Produced.ContainsSingle<StorageMessage.RequestCompleted>(
				x => x.CorrelationId == InternalCorrId && x.Success == false));
		}

		[Test]
		public void the_envelope_is_replied_to_with_failure() {
			Assert.That(Envelope.Replies.ContainsSingle<ClientMessage.DeleteStreamCompleted>(
				x => x.CorrelationId == ClientCorrId && x.Result == OperationResult.CommitTimeout));
		}
	}
}

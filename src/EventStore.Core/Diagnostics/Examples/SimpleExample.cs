﻿using EventStore.Core.Messaging;

namespace EventStore.Core.Diagnostics.Examples.Simple {
	[StatsGroup("simple-example")]
	public enum MessageType { A, B, C, D, E }

	[StatsMessage(MessageType.A)]
	public partial class A : Message {
	}

	[StatsMessage(MessageType.B)]
	public partial class B : Message {
	}
}

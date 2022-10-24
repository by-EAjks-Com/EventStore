using System;
using EventStore.Common.Utils;
using EventStore.Core.Diagnostics;
using EventStore.Core.Helpers;
using EventStore.Core.Messaging;

namespace EventStore.Core.Bus {
	public class AdHocHandler<T> : IHandle<T> where T : Message {
		private readonly Action<T> _handle;

		public AdHocHandler(Action<T> handle) {
			Ensure.NotNull(handle, "handle");
			_handle = handle;
		}

		public void Handle(T message) {
			_handle(message);
		}
	}

	//qq hopefully we dont need both this class and the above
	public class AdHocHandlerEx<T> : IHandleEx<T> where T : Message {
		private readonly Action<StatInfo, T> _handle;

		public AdHocHandlerEx(Action<StatInfo, T> handle) {
			Ensure.NotNull(handle, "handle");
			_handle = handle;
		}

		public void Handle(StatInfo info, T message) {
			_handle(info, message);
		}
	}

	public struct AdHocHandlerStruct<T> : IHandle<T>, IHandleTimeout where T : Message {
		private readonly Action<T> _handle;
		private readonly Action _timeout;

		public AdHocHandlerStruct(Action<T> handle, Action timeout) {
			Ensure.NotNull(handle, "handle");

			HandlesTimeout = timeout is not null;
			_handle = handle;
			_timeout = timeout.OrNoOp();
		}

		public bool HandlesTimeout { get; }

		public void Handle(T response) {
			_handle(response);
		}

		public void Timeout() {
			_timeout();
		}
	}
}

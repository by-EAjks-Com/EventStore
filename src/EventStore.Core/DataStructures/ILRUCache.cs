using System;
using EventStore.Core.Caching;

namespace EventStore.Core.DataStructures {
	public interface ILRUCache : IAllotment {
		string Name { get; }
		void Clear();
	}

	public interface ILRUCache<TKey, TValue>: ILRUCache {
		bool TryGet(TKey key, out TValue value);
		TValue Put(TKey key, TValue value);

		TValue Put<T>(TKey key, T userData, Func<TKey, T, TValue> addFactory,
			Func<TKey, TValue, T, TValue> updateFactory);
	}

	public interface IStickyLRUCache<TKey, TValue> {
		void Clear();
		bool TryGet(TKey key, out TValue value);
		TValue Put(TKey key, TValue value, int stickiness);
		TValue Put(TKey key, Func<TKey, TValue> addFactory, Func<TKey, TValue, TValue> updateFactory, int stickiness);
	}
}

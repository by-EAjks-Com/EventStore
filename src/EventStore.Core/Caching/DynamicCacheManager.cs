//qq seen
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.TimerService;
using Serilog;

namespace EventStore.Core.Caching {
	public class DynamicCacheManager:
		IHandle<MonitoringMessage.DynamicCacheManagerTick>,
		IHandle<MonitoringMessage.InternalStatsRequest> {

		private readonly IPublisher _bus;
		private readonly Func<long> _getFreeMem;
		private readonly long _totalMem;
		private readonly int _keepFreeMemPercent;
		private readonly long _keepFreeMemBytes;
		private readonly TimeSpan _monitoringInterval;
		private readonly TimeSpan _minResizeInterval;
		private readonly ICacheSettings[] _cachesSettings;
		private readonly int _totalWeight;
		private readonly long[] _maxMemAllocation;
		private readonly object _lock = new();

		private DateTime _lastResize = DateTime.UtcNow;

		public DynamicCacheManager(
			IPublisher bus,
			Func<long> getFreeMem,
			long totalMem,
			int keepFreeMemPercent,
			long keepFreeMemBytes,
			TimeSpan monitoringInterval,
			TimeSpan minResizeInterval,
			params ICacheSettings[] cachesSettings) {

			if (keepFreeMemPercent is < 0 or > 100)
				throw new ArgumentException($"{nameof(keepFreeMemPercent)} must be between 0 to 100 inclusive.");

			if (keepFreeMemBytes < 0)
				throw new ArgumentException($"{nameof(keepFreeMemBytes)} must be non-negative.");

			//qq dont need this check, guaranteed by construction
			//var dynamicWeightsPositive = cachesSettings
			//	.Where(x => x.IsDynamic)
			//	.All(x => x.Weight > 0);

			//if (!dynamicWeightsPositive)
			//	throw new ArgumentException("Weight of all dynamic caches should be positive.");

			_bus = bus;
			_getFreeMem = getFreeMem;
			_totalMem = totalMem;
			_keepFreeMemPercent = keepFreeMemPercent;
			_keepFreeMemBytes = keepFreeMemBytes;
			_monitoringInterval = monitoringInterval;
			_minResizeInterval = minResizeInterval;
			_cachesSettings = cachesSettings;
			_totalWeight = cachesSettings
				.Sum(x => x.Weight);
			_maxMemAllocation = new long[cachesSettings.Length];
			Array.Fill(_maxMemAllocation, -1);

			//qq we might want to kick it off later, after the indexbackend has subscribed to the updates
			InitCacheSizes();
			Tick();
		}

		public void Handle(MonitoringMessage.DynamicCacheManagerTick message) {
			ThreadPool.QueueUserWorkItem(_ => {
				try {
					lock (_lock) { // only to add read/write barriers //qq tasks are wrapped with these already i think
						ResizeCachesIfNeeded();
					}
				} finally {
					Tick();
				}
			});
		}

		public void Handle(MonitoringMessage.InternalStatsRequest message) {
			//qq are all these things thread safe?
			Thread.MemoryBarrier(); // just to ensure we're seeing latest values

			var stats = new Dictionary<string, object>();

			for (int i = 0; i < _cachesSettings.Length; i++) {
				var statNamePrefix = $"es-cache-{_cachesSettings[i].Name}-";
				stats[statNamePrefix + "name"] = _cachesSettings[i].Name;
				stats[statNamePrefix + "weight"] = _cachesSettings[i].Weight;
				stats[statNamePrefix + "mem-used"] = _cachesSettings[i].GetMemoryUsage();
				stats[statNamePrefix + "mem-minAlloc"] = 123; //qq to be removed
				stats[statNamePrefix + "mem-maxAlloc"] = Interlocked.Read(ref _maxMemAllocation[i]); //qq min/max alloc here alloc refers to two different things
			}

			message.Envelope.ReplyWith(new MonitoringMessage.InternalStatsRequestResponse(stats));
		}

		private void InitCacheSizes() {
			var availableMem = CalcAvailableMemory(_getFreeMem(), 0L);
			var cacheIndex = -1;
			foreach (var cacheSettings in _cachesSettings) {
				cacheIndex++;

				var allotment = cacheSettings.CalcMemAllotment(availableMem, _totalWeight);

				Log.Information("{name} cache size configured to ~{alottedMem:N0} bytes.",
					cacheSettings.Name, allotment);
				_maxMemAllocation[cacheIndex] = allotment;
			}
		}

		private void ResizeCachesIfNeeded() {
			var freeMem = _getFreeMem();
			var keepFreeMem = Math.Max(_keepFreeMemBytes, _totalMem * _keepFreeMemPercent / 100);

			if (freeMem >= keepFreeMem && DateTime.UtcNow - _lastResize < _minResizeInterval)
				return;

			if (freeMem < keepFreeMem) {
				Log.Debug("Available system memory is lower than "
				          + "{thresholdPercent}% or {thresholdBytes:N0} bytes: {freeMem:N0} bytes. Resizing caches.",
					_keepFreeMemPercent, _keepFreeMemBytes, freeMem);
			}

			try {
				var cachedMem = _cachesSettings.Sum(cacheSettings => cacheSettings.GetMemoryUsage());
				ResizeCaches(freeMem, cachedMem);
			} finally {
				_lastResize = DateTime.UtcNow;
			}
		}

		private void ResizeCaches(long freeMem, long cachedMem) {
			var availableMem = CalcAvailableMemory(freeMem, cachedMem);

			var sw = new Stopwatch();
			var cacheIndex = -1;

			foreach (var cacheSettings in _cachesSettings) {
				cacheIndex++;

				//qq same here about subtracting static allowances from the availableMem
				// although available mem here does include cachedMem................... maybe
				// we shouldn't be counting the actual memory usage of the static caches in cachedMem
				//qqqq im a bit suspicious that anything that undercounts that true size of the cache (say, because
				// we dont count the cost of the infra, just the data, or if we dont count the whole cost of the data)
				// will cause the caches to continually resize... i'll see if this is the case. but when
				// increasing the size of the cache maybe we should only do so we are increasing it by a lot.

				//qq also if we are resizing down... thats only going to achieve anything if some of the caches are full enough
				// to cause evictions... otherwise it wont free the memory and it will try again every 15s
				var allocatedMem = cacheSettings.CalcMemAllotment(availableMem, _totalWeight);

				// do not resize if the amount of memory allocated to the cache hasn't changed
				if (_maxMemAllocation[cacheIndex] == allocatedMem)
					continue;

				sw.Restart();
				cacheSettings.UpdateMaxMemoryAllocation(allocatedMem);
				sw.Stop();
				Log.Debug(
					"{name} cache resized to ~{allocatedMem:N0} bytes in {elapsed}.",
					cacheSettings.Name, allocatedMem, sw.Elapsed);
				_maxMemAllocation[cacheIndex] = allocatedMem;
			}
		}

		// Memory available for caching
		private long CalcAvailableMemory(long freeMem, long cachedMem) {
			var keepFreeMem = Math.Max(_keepFreeMemBytes, _totalMem * _keepFreeMemPercent / 100);
			var availableMem = Math.Max(0L, freeMem + cachedMem - keepFreeMem);

			Log.Debug("Calculating memory available for caching based on:\n" +
			          "Free memory: {freeMem:N0} bytes\n" +
			          "Total memory: {totalMem:N0} bytes\n" +
			          "Cached memory: ~{cachedMem:N0} bytes\n" +
			          "Keep free %: {keepFreeMemPercent}%\n" +
			          "Keep free bytes: {keepFreeMemBytes:N0} bytes\n\n" +
			          "Memory available for caching: ~{availableMem:N0} bytes\n",
				freeMem, _totalMem, cachedMem,
				_keepFreeMemPercent, _keepFreeMemBytes, availableMem);

			return availableMem;
		}

		private void Tick() {
			_bus.Publish(
				TimerMessage.Schedule.Create(
					_monitoringInterval,
					new PublishEnvelope(_bus),
					new MonitoringMessage.DynamicCacheManagerTick()));
		}
	}
}

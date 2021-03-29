﻿namespace EventStore.Core.Services.RequestManager {
	public enum CommitLevel {
		Replicated, //Write on Cluster Quorum
		Indexed //Indexed on Leader, n.b. only possible after Replicated
	}
}

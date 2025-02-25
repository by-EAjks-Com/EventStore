// Copyright (c) Event Store Ltd and/or licensed to Event Store Ltd under one or more agreements.
// Event Store Ltd licenses this file to you under the Event Store License v2 (see LICENSE.md).

using System.IO;

namespace EventStore.Core.XUnit.Tests.Services.Archive.Storage;

public sealed class AwsCliDirectoryNotFoundException(string path)
	: DirectoryNotFoundException($"Directory '{path}' with config files for AWS CLI doesn't exist");

// Copyright (c) Event Store Ltd and/or licensed to Event Store Ltd under one or more agreements.
// Event Store Ltd licenses this file to you under the Event Store License v2 (see LICENSE.md).

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using DotNext.Buffers;
using DotNext.IO;
using EventStore.Common.Exceptions;
using FluentStorage;
using FluentStorage.AWS.Blobs;
using FluentStorage.Blobs;
using Serilog;

namespace EventStore.Core.Services.Archive.Storage.S3;

public class S3BlobStorage : IBlobStorage {
	protected static readonly ILogger Log = Serilog.Log.ForContext<S3BlobStorage>();

	private readonly S3Options _options;
	private readonly IAwsS3BlobStorage _awsBlobStorage;

	public S3BlobStorage(S3Options options) {
		_options = options;

		if (string.IsNullOrEmpty(options.Bucket))
			throw new InvalidConfigurationException("Please specify an Archive S3 Bucket");

		if (string.IsNullOrEmpty(options.Region))
			throw new InvalidConfigurationException("Please specify an Archive S3 Region");

		_awsBlobStorage = StorageFactory.Blobs.AwsS3(
			awsCliProfileName: options.AwsCliProfileName,
			bucketName: options.Bucket,
			region: options.Region) as IAwsS3BlobStorage;
	}

	public async ValueTask<int> ReadAsync(string name, Memory<byte> buffer, long offset, CancellationToken ct) {
		var request = new GetObjectRequest {
			BucketName = _options.Bucket,
			Key = name,
			ByteRange = GetRange(offset, buffer.Length),
		};

		try {
			var client = _awsBlobStorage.NativeBlobClient;
			using var response = await client.GetObjectAsync(request, ct);
			var length = int.CreateSaturating(response.ContentLength);
			await using var responseStream = response.ResponseStream;
			await responseStream.ReadExactlyAsync(buffer.TrimLength(length), ct);
			return length;
		} catch (AmazonS3Exception ex) {
			if (ex.ErrorCode == "NoSuchKey")
				throw new FileNotFoundException();
			throw;
		}
	}

	public async ValueTask Store(ReadOnlyMemory<byte> sourceData, string name, CancellationToken ct) {
		await using var stream = sourceData.AsStream();
		await _awsBlobStorage.WriteAsync(name, stream, append: false, ct);
	}

	public async ValueTask Store(string input, string name, CancellationToken ct) {
		await _awsBlobStorage.WriteFileAsync(name, filePath: input, ct);
	}

	// ByteRange is inclusive of both start and end
	private static ByteRange GetRange(long offset, int length) => new(
		start: offset,
		end: offset + length - 1L);

	public async ValueTask<BlobMetadata> GetMetadataAsync(string name, CancellationToken token) {
		var response = await _awsBlobStorage.NativeBlobClient.GetObjectMetadataAsync(
			_awsBlobStorage.BucketName, name, token);
		return new(Size: response.ContentLength);
	}
}

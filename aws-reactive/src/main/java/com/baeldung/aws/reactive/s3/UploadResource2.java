package com.baeldung.aws.reactive.s3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

@RestController
@RequestMapping("/inbox2")
@Slf4j
public class UploadResource2 {

    private static final long PART_SIZE = 100 * 1024 * 1024; // S3 Upload part size

    private final S3AsyncClient s3client;
    private final S3ClientConfigurationProperties s3config;

    public UploadResource2(S3AsyncClient s3client, S3ClientConfigurationProperties s3config) {
        this.s3client = s3client;
        this.s3config = s3config;        
    }
    
    /**
     *  Standard file upload: verify with
     *  `time curl -v --header "Content-Type:application/octet-stream" --data-binary '@20GB.bin' localhost:8080/inbox2`
     */
    @PostMapping
    public Mono<ResponseEntity<UploadResult>> uploadHandler(
        @RequestHeader HttpHeaders headers,
        @RequestBody Flux<ByteBuffer> body)
    {
        long length = headers.getContentLength();
        if (length < 0) {
            throw new UploadFailedException(HttpStatus.BAD_REQUEST.value(), Optional.of("required header missing: Content-Length"));
        }

        String fileKey = UUID.randomUUID().toString();
        MediaType mediaType = headers.getContentType();
        if (mediaType == null) {
            mediaType = MediaType.APPLICATION_OCTET_STREAM;
        }

        return createS3MultipartUpload(fileKey, mediaType)
            .flatMap(uploadId -> uploadBody(fileKey, uploadId, body, length))
            .flatMap(res -> completeS3MultipartUpload(fileKey, res.uploadId, res.parts))
            .map(x -> ResponseEntity
                .status(HttpStatus.CREATED)
                .body(new UploadResult(HttpStatus.CREATED, new String[] {fileKey}))
            );
    }

    private Mono<String> createS3MultipartUpload(String key, MediaType mediaType) {
        CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder()
            .bucket(s3config.getBucket())
            .key(key)
            .contentType(mediaType.toString())
            .build();
        return Mono
            .fromFuture(s3client.createMultipartUpload(request))
            .map(result -> checkResult(result).uploadId());
    }

    private static class MultipartUploadResult {
        final String uploadId;
        final List<CompletedPart> parts;

        private MultipartUploadResult(String uploadId, List<CompletedPart> parts) {
            this.uploadId = uploadId;
            this.parts = parts;
        }
    }

    private static class BytesCounter {
        final long totalBytes;
        final long currBytes;
        final ByteBuffer buffer;

        private BytesCounter(long totalBytes, long currBytes, ByteBuffer buffer) {
            this.totalBytes = totalBytes;
            this.currBytes = currBytes;
            this.buffer = buffer;
        }

        static BytesCounter scanner(BytesCounter prev, ByteBuffer buffer) {
            return new BytesCounter(
                prev.totalBytes + prev.currBytes,
                buffer.limit(),
                buffer
            );
        }

        Flux<NumBuffer> split() {
            long partNumber = totalBytes / PART_SIZE;
            int bufferLength = buffer.limit();
            long totalLength = totalBytes + bufferLength;
            long nextPart = totalLength / PART_SIZE;
            if (partNumber != nextPart) {
                int overflow = (int) (totalLength % PART_SIZE);
                if (overflow > 0) {
                    Tuple2<ByteBuffer, ByteBuffer> tuple = splitBuffer(buffer, bufferLength - overflow);
                    return Flux.just(
                        new NumBuffer(partNumber, tuple.getT1()),
                        new NumBuffer(partNumber + 1, tuple.getT2())
                    );
                }
            }
            return Flux.just(new NumBuffer(partNumber, buffer));
        }

        static Tuple2<ByteBuffer, ByteBuffer> splitBuffer(ByteBuffer buffer, int length) {
            buffer.position(length);
            ByteBuffer otherBuffer = buffer.slice();
            buffer.limit(length);
            buffer.position(0);
            return Tuples.of(buffer, otherBuffer);
        }
    }

    private static class NumBuffer {
        final long partNumber;
        final ByteBuffer buffer;

        private NumBuffer(long partNumber, ByteBuffer buffer) {
            this.partNumber = partNumber;
            this.buffer = buffer;
        }
    }

    private Mono<MultipartUploadResult> uploadBody(
        String fileKey, String uploadId, Flux<ByteBuffer> body, long length
    ) {
        ByteBuffer head = ByteBuffer.allocate(0);

        Function<Flux<ByteBuffer>, Flux<BytesCounter>> countBytes = in -> in
            .scan(new BytesCounter(0L, 0, head), BytesCounter::scanner)
            .filter(bytesCounter -> bytesCounter.buffer != head);

        Function<Flux<BytesCounter>, Flux<NumBuffer>> chopParts = in -> in
            .concatMap(BytesCounter::split);

        long parts = length / PART_SIZE + (length % PART_SIZE > 0 ? 1 : 0);
        Function<Flux<Tuple2<Long, Flux<NumBuffer>>>, Flux<CompletedPart>> uploadPart = in -> in
            .concatMap(tuple -> {
                int partN                   = tuple.getT1().intValue() + 1;
                final Flux<ByteBuffer> data = tuple.getT2().map(t -> t.buffer);
                long partLength;
                if (length <= PART_SIZE) {
                    partLength = length;
                } else if (partN == parts) {
                    long left = length % PART_SIZE;
                    partLength = left == 0 ? PART_SIZE : left;
                } else {
                    partLength = PART_SIZE;
                }
                    return uploadS3Part(fileKey, uploadId, partN, partLength, data);
                }
            );

        return body
            .transform(countBytes)
            .transform(chopParts)
            .windowUntilChanged(numBuffer -> numBuffer.partNumber)
            .index()
            .transform(uploadPart)
            .collectList()
            .map(completedParts -> new MultipartUploadResult(uploadId, completedParts));
    }



    private Mono<CompletedPart> uploadS3Part(
        String fileKey, String uploadId, int partNumber, long partSize, Flux<ByteBuffer> data
    ) {
        UploadPartRequest request = UploadPartRequest.builder()
            .bucket(s3config.getBucket())
            .key(fileKey)
            .uploadId(uploadId)
            .partNumber(partNumber)
            .contentLength(partSize)
            .build();

        return Mono
            .fromFuture(s3client.uploadPart(request, AsyncRequestBody.fromPublisher(data)))
            .map(result -> {
                checkResult(result);
                return CompletedPart.builder()
                    .partNumber(partNumber)
                    .eTag(result.eTag())
                    .build();
            });
    }

    private Mono<Void> completeS3MultipartUpload(String fileKey, String uploadId, List<CompletedPart> parts) {
        CompletedMultipartUpload upload = CompletedMultipartUpload.builder()
            .parts(parts)
            .build();
        CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder()
            .bucket(s3config.getBucket())
            .uploadId(uploadId)
            .multipartUpload(upload)
            .key(fileKey)
            .build();
        return Mono.fromFuture(s3client.completeMultipartUpload(request))
            .flatMap(result -> {
                checkResult(result);
                return Mono.empty(); // Void
            });
    }

    private static <T extends SdkResponse> T checkResult(T result) {
        if (result.sdkHttpResponse() == null || !result.sdkHttpResponse().isSuccessful()) {
            throw new UploadFailedException(result);
        }
        return result;
    }

}

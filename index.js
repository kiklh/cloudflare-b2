//
// Proxy Backblaze S3 compatible API requests, sending notifications to a webhook
//
// Adapted from https://github.com/obezuk/worker-signed-s3-template
//
import { AwsClient } from 'aws4fetch'

const UNSIGNABLE_HEADERS = [
    'x-forwarded-proto',
    'x-real-ip',
    'accept-encoding',
    'if-match',
    'if-modified-since',
    'if-none-match',
    'if-range',
    'if-unmodified-since',
];

const HTTPS_PROTOCOL = "https:";
const HTTPS_PORT = "443";

const RANGE_RETRY_ATTEMPTS = 3;

// 根据文件扩展名推断 Content-Type 的映射表
const MIME_TYPES = {
    // 图片
    jpg: 'image/jpeg',
    jpeg: 'image/jpeg',
    png: 'image/png',
    gif: 'image/gif',
    webp: 'image/webp',
    svg: 'image/svg+xml',
    bmp: 'image/bmp',
    ico: 'image/x-icon',
    // 视频
    mp4: 'video/mp4',
    webm: 'video/webm',
    ogg: 'video/ogg',
    ogv: 'video/ogg',
    mov: 'video/quicktime',
    m3u8: 'application/vnd.apple.mpegurl',
    ts: 'video/mp2t',
    // 音频
    mp3: 'audio/mpeg',
    wav: 'audio/wav',
    flac: 'audio/flac',
    m4a: 'audio/mp4',
    // 文档与其他
    pdf: 'application/pdf',
    txt: 'text/plain',
    html: 'text/html',
    htm: 'text/html',
    css: 'text/css',
    js: 'application/javascript',
    json: 'application/json',
    xml: 'application/xml',
    zip: 'application/zip',
};

// 从路径中提取扩展名并返回对应的 MIME 类型
function inferContentType(path) {
    const lastSegment = path.split('/').pop() || '';
    const dotIndex = lastSegment.lastIndexOf('.');
    if (dotIndex === -1) return null;
    const ext = lastSegment.slice(dotIndex + 1).toLowerCase();
    return MIME_TYPES[ext] || null;
}

// 如果响应缺少 Content-Type 或返回了通用的二进制类型，则根据文件扩展名覆盖它
function applyContentType(response, path) {
    const inferred = inferContentType(path);
    if (!inferred) return response;

    const currentType = response.headers.get('content-type') || '';
    // 只在缺失或为 octet-stream 时覆盖，保留 B2 已设置的正确类型
    if (currentType && !currentType.includes('octet-stream')) {
        return response;
    }

    const newHeaders = new Headers(response.headers);
    newHeaders.set('Content-Type', inferred);

    // HEAD 响应没有 body，用 null 构造
    if (!response.body) {
        return new Response(null, {
            headers: newHeaders,
            status: response.status,
            statusText: response.statusText,
        });
    }

    return new Response(response.body, {
        headers: newHeaders,
        status: response.status,
        statusText: response.statusText,
    });
}

function filterHeaders(headers, env) {
    return new Headers(Array.from(headers.entries())
        .filter(pair => !(
            UNSIGNABLE_HEADERS.includes(pair[0])
            || pair[0].startsWith('cf-')
            || ('ALLOWED_HEADERS' in env && !env['ALLOWED_HEADERS'].includes(pair[0]))
        ))
    );
}

function createHeadResponse(response) {
    return new Response(null, {
        headers: response.headers,
        status: response.status,
        statusText: response.statusText
    });
}

function isListBucketRequest(env, path) {
    const pathSegments = path.split('/');

    return (env['BUCKET_NAME'] === "$path" && pathSegments.length < 2)
        || (env['BUCKET_NAME'] !== "$path" && path.length === 0);
}

export default {
    async fetch(request, env) {
        if (!['GET', 'HEAD'].includes(request.method)) {
            return new Response(null, {
                status: 405,
                statusText: "Method Not Allowed"
            });
        }

        const url = new URL(request.url);

        url.protocol = HTTPS_PROTOCOL;
        url.port = HTTPS_PORT;

        let path = url.pathname.replace(/^\//, '');
        path = path.replace(/\/$/, '');

        if (isListBucketRequest(env, path) && String(env['ALLOW_LIST_BUCKET']) !== "true") {
            return new Response(null, {
                status: 404,
                statusText: "Not Found"
            });
        }

        const rcloneDownload = String(env["RCLONE_DOWNLOAD"]) === 'true';

        switch (env['BUCKET_NAME']) {
            case "$path":
                url.hostname = env['B2_ENDPOINT'];
                break;
            case "$host":
                url.hostname = url.hostname.split('.')[0] + '.' + env['B2_ENDPOINT'];
                break;
            default:
                url.hostname = env['BUCKET_NAME'] + "." + env['B2_ENDPOINT'];
                break;
        }

        const headers = filterHeaders(request.headers, env);

        const client = new AwsClient({
            "accessKeyId": env['B2_APPLICATION_KEY_ID'],
            "secretAccessKey": env['B2_APPLICATION_KEY'],
            "service": "s3",
        });

        const requestMethod = request.method;

        if (rcloneDownload) {
            if (env['BUCKET_NAME'] === "$path") {
                url.pathname = path.replace(/^file\//, "");
            } else {
                url.pathname = path.replace(/^file\/[^/]+\//, "");
            }
        }

        const signedRequest = await client.sign(url.toString(), {
            method: 'GET',
            headers: headers
        });

        // 用于推断 Content-Type 的路径（rclone 前缀已去除）
        const contentTypePath = url.pathname.replace(/^\//, '');

        if (signedRequest.headers.has("range")) {
            let attempts = RANGE_RETRY_ATTEMPTS;
            let response;
            do {
                let controller = new AbortController();
                response = await fetch(signedRequest.url, {
                    method: signedRequest.method,
                    headers: signedRequest.headers,
                    signal: controller.signal,
                });
                if (response.headers.has("content-range")) {
                    if (attempts < RANGE_RETRY_ATTEMPTS) {
                        console.log(`Retry for ${signedRequest.url} succeeded - response has content-range header`);
                    }
                    break;
                } else if (response.ok) {
                    attempts -= 1;
                    console.error(`Range header in request for ${signedRequest.url} but no content-range header in response. Will retry ${attempts} more times`);
                    if (attempts > 0) {
                        controller.abort();
                    }
                } else {
                    break;
                }
            } while (attempts > 0);

            if (attempts <= 0) {
                console.error(`Tried range request for ${signedRequest.url} ${RANGE_RETRY_ATTEMPTS} times, but no content-range in response.`);
            }

            if (requestMethod === 'HEAD') {
                // HEAD 请求，返回无 body 的响应，同时修正 Content-Type
                return applyContentType(createHeadResponse(response), contentTypePath);
            }

            // Range 请求的响应同样应用 Content-Type
            return applyContentType(response, contentTypePath);
        }

        const fetchPromise = fetch(signedRequest);

        if (requestMethod === 'HEAD') {
            const response = await fetchPromise;
            // HEAD 请求，返回无 body 的响应，同时修正 Content-Type
            return applyContentType(createHeadResponse(response), contentTypePath);
        }

        // 普通 GET 请求，返回带修正 Content-Type 的响应
        const response = await fetchPromise;
        return applyContentType(response, contentTypePath);
    },
};
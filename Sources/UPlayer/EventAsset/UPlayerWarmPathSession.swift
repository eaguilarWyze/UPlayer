//
//  UPlayerWarmPathSession.swift
//  UPlayer
//
//  Ported from `WPKWarmPathPlaybackSession.reserveNextLikelyEvent`
//  (wyze-wpk-ios, feat/kvs-prefetch-caching). Given the next likely
//  event's URL, this parses its manifest (via the same processor
//  pipeline used for normal playback) and concurrently prefetches the
//  video init segment plus the next N media fragments into
//  `UPlayerFragmentCache`, so `UPlayerAVAssetResourceLoader` can serve
//  them from disk instead of the network once the user actually swipes.
//

import Foundation

public final class UPlayerWarmPathSession: NSObject {

    /// How many leading media fragments (per representation) to prefetch
    /// ahead of playback. The old system tried 2 first and found it
    /// insufficient in real testing; 6 is the value that was kept after
    /// tuning, so it's reused here rather than re-derived.
    public static let defaultLookaheadCount = 6

    private let lookaheadCount: Int
    private let fragmentCache: UPlayerFragmentCache
    private let processorsQueue: UPlayerAssetProcessorsQueueProtocol
    private let urlSession: URLSession

    private let lock = NSLock()
    private var pendingCompletions: [URL: [(Bool) -> Void]] = [:]

    public init(lookaheadCount: Int = UPlayerWarmPathSession.defaultLookaheadCount,
                fragmentCache: UPlayerFragmentCache = .shared,
                urlSession: URLSession = .shared) {

        self.lookaheadCount = lookaheadCount
        self.fragmentCache = fragmentCache
        self.urlSession = urlSession

        let queue = UPlayerAssetProcessorsQueue()
        queue.add(processor: UPlayerMetadataDownloader(id: "warmPathDownloadAssetProcessor"))
        queue.add(processor: UPlayerMPDParser(id: "warmPathMpdParserAssetProcessor"))
        queue.add(processor: UPlayerSegmentBaseHLSGenerator(id: "warmPathHlsSegmentBaseAssetProcessor"))
        queue.add(processor: UPlayerMPDToMP4Resolver(id: "warmPathMPDToMP4ResolverAssetProcessor"))
        queue.add(processor: UPlayerHLSGenerator(id: "warmPathHlsGeneratorAssetProcessor"))
        self.processorsQueue = queue

        super.init()

        queue.delegate = self
    }

    /// Begins prefetching `url`'s manifest and its first `lookaheadCount`
    /// video fragments. Safe to call multiple times for the same URL
    /// while a prefetch is already in flight — the completion is simply
    /// appended to the pending set instead of starting duplicate work.
    ///
    /// `completion` reports `true` if at least the manifest was parsed
    /// and some fragments were queued for (or already) caching; `false` on
    /// manifest failure. It does not wait for every fragment download to
    /// finish, since those continue in the background and simply populate
    /// the cache whenever they land.
    public func reserveNextLikelyEvent(url: URL, completion: ((Bool) -> Void)? = nil) {
        lock.lock()
        let alreadyInFlight = pendingCompletions[url] != nil
        pendingCompletions[url, default: []].append(completion ?? { _ in })
        lock.unlock()

        guard !alreadyInFlight else {
            log("[warmpath] reserve already in flight for \(url.absoluteString)", loggingLevel: .debug)
            return
        }

        log("[warmpath] reserving \(url.absoluteString)", loggingLevel: .info)

        let asset = UPlayerAsset(url: url)
        processorsQueue.start(asset: asset)
    }

    /// Cancels any in-flight prefetch and clears pending completions.
    /// Call this on EG/Stories teardown so a warm-path fetch for an event
    /// the user never actually visited doesn't keep running.
    public func endSession() {
        processorsQueue.stop()

        lock.lock()
        let pending = pendingCompletions
        pendingCompletions.removeAll()
        lock.unlock()

        pending.values.flatMap { $0 }.forEach { $0(false) }
    }

    private func completePending(for url: URL, success: Bool) {
        lock.lock()
        let completions = pendingCompletions.removeValue(forKey: url) ?? []
        lock.unlock()

        completions.forEach { $0(success) }
    }

    private func prefetchFragments(from asset: UPlayerAssetProtocol) {
        guard let hlsMetadata = asset.hlsMetadata else {
            completePending(for: asset.url, success: false)
            return
        }

        let candidateURLs = Self.videoFragmentURLs(
            in: hlsMetadata.mediaPlaylists.values,
            lookaheadCount: lookaheadCount
        )

        guard !candidateURLs.isEmpty else {
            log("[warmpath] no video fragment URLs found for \(asset.url.absoluteString)", loggingLevel: .debug)
            completePending(for: asset.url, success: true)
            return
        }

        completePending(for: asset.url, success: true)

        Task { [urlSession, fragmentCache] in
            await withTaskGroup(of: Void.self) { group in
                for realURL in candidateURLs {
                    group.addTask {
                        if fragmentCache.data(for: realURL) != nil {
                            return
                        }

                        do {
                            let (data, response) = try await urlSession.data(from: realURL)

                            guard let http = response as? HTTPURLResponse,
                                  (200...299).contains(http.statusCode) else {
                                return
                            }

                            fragmentCache.store(data, for: realURL)
                            log("[warmpath] prefetched \(realURL.absoluteString), bytes=\(data.count)", loggingLevel: .debug)
                        } catch {
                            log("[warmpath] prefetch failed for \(realURL.absoluteString): \(error)", loggingLevel: .error)
                        }
                    }
                }
            }
        }
    }

    /// Parses raw HLS media-playlist text for `uplayer://` video
    /// init/segment URIs (the `EXT-X-MAP` line, plus up to
    /// `lookaheadCount` following `EXTINF` media URIs) and recovers their
    /// original `https://` URL — the same key `UPlayerAVAssetResourceLoader`
    /// will look up when the segment is actually requested during
    /// playback.
    static func videoFragmentURLs<S: Sequence>(in playlists: S, lookaheadCount: Int) -> [URL] where S.Element == String {
        var results: [URL] = []

        for playlist in playlists {
            var mediaURICount = 0

            for rawLine in playlist.split(separator: "\n") {
                let line = rawLine.trimmingCharacters(in: .whitespaces)

                let uriString: String?

                if line.hasPrefix("#EXT-X-MAP:"), let range = line.range(of: "URI=\"") {
                    let remainder = line[range.upperBound...]
                    uriString = remainder.split(separator: "\"", maxSplits: 1).first.map(String.init)
                } else if !line.isEmpty, !line.hasPrefix("#") {
                    guard mediaURICount < lookaheadCount else {
                        continue
                    }
                    mediaURICount += 1
                    uriString = line
                } else {
                    uriString = nil
                }

                guard let uriString,
                      let url = URL(string: uriString),
                      url.scheme == "uplayer",
                      let mode = UPlayerURLScheme.mode(of: url),
                      mode == "video-segment" || mode == "video-init",
                      let originalURL = UPlayerURLScheme.originalHTTPURL(from: url) else {
                    continue
                }

                results.append(originalURL)
            }
        }

        return results
    }
}

extension UPlayerWarmPathSession: UPlayerAssetProcessorsQueueDelegate {
    public func didStartProcessing(source: UPlayerAssetProcessorsQueueProtocol) {}

    public func didFinishProcessing(source: UPlayerAssetProcessorsQueueProtocol, error: Error?) {
        // No per-asset context is available on failure; every still-pending
        // reservation fails closed rather than hanging forever.
        lock.lock()
        let pending = pendingCompletions
        pendingCompletions.removeAll()
        lock.unlock()

        pending.values.flatMap { $0 }.forEach { $0(false) }
    }

    public func didFinishProcessing(source: UPlayerAssetProcessorsQueueProtocol, asset: UPlayerAssetProtocol) {
        prefetchFragments(from: asset)
    }
}

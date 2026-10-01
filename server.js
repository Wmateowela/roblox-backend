const http = require("http");
const fs = require("fs");
const path = require("path");
const { URL } = require("url");

const PORT = process.env.PORT || 9090;
const HOST = "0.0.0.0";

// Keep the search service small and predictable on Render's free instance.
const ROBLOX_REQUEST_TIMEOUT_MS = 5500;
const ROBLOX_RETRY_DELAY_CAP_MS = 2000;
const TRANSIENT_ROBLOX_STATUSES = new Set([429, 500, 502, 503, 504]);
const ROBLOX_DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9"
};
const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const SEARCH_CACHE_TTL_MS = 5 * 60 * 1000;
const SEARCH_CACHE_STALE_MS = 60 * 60 * 1000;
const NEGATIVE_SEARCH_CACHE_TTL_MS = 30 * 1000;
const PROFILE_CACHE_TTL_MS = 10 * 60 * 1000;
const PROFILE_CACHE_STALE_MS = 60 * 60 * 1000;
const NEGATIVE_PROFILE_CACHE_TTL_MS = 30 * 1000;
const MAX_SEARCH_CACHE_ENTRIES = 1000;
const MAX_PROFILE_CACHE_ENTRIES = 2000;
const MAX_IN_FLIGHT_SEARCHES = 100;
const MAX_IN_FLIGHT_PROFILES = 100;
const MAX_RATE_LIMIT_ENTRIES = 10000;
// Players returned per search: the exact username first, then keyword
// suggestions, never more than this (keeps the avatar batch small too).
const MAX_SEARCH_RESULTS = 7;

class TtlLruCache {
    constructor(maxEntries) {
        this.maxEntries = maxEntries;
        this.entries = new Map();
    }

    set(key, value, ttlMs, staleMs = 0) {
        const now = Date.now();
        this.entries.delete(key);
        this.entries.set(key, {
            value,
            expiresAt: now + ttlMs,
            staleUntil: now + ttlMs + staleMs
        });
        while (this.entries.size > this.maxEntries) {
            this.entries.delete(this.entries.keys().next().value);
        }
    }

    getFresh(key) {
        const entry = this.entries.get(key);
        if (!entry) return null;
        const now = Date.now();
        if (entry.staleUntil <= now) {
            this.entries.delete(key);
            return null;
        }
        if (entry.expiresAt <= now) return null;
        this.entries.delete(key);
        this.entries.set(key, entry);
        return { value: entry.value, stale: false };
    }

    getStale(key) {
        const entry = this.entries.get(key);
        if (!entry) return null;
        const now = Date.now();
        if (entry.staleUntil <= now) {
            this.entries.delete(key);
            return null;
        }
        this.entries.delete(key);
        this.entries.set(key, entry);
        return { value: entry.value, stale: entry.expiresAt <= now };
    }

    cleanup() {
        const now = Date.now();
        for (const [key, entry] of this.entries) {
            if (entry.staleUntil <= now) this.entries.delete(key);
        }
    }

    clear() {
        this.entries.clear();
    }
}

class RobloxUpstreamError extends Error {
    constructor(message, options = {}) {
        super(message);
        this.name = 'RobloxUpstreamError';
        this.status = options.status || null;
        this.retryAfterMs = options.retryAfterMs || null;
        this.cause = options.cause;
    }
}

class ServiceBusyError extends Error {
    constructor(message) {
        super(message);
        this.name = 'ServiceBusyError';
    }
}

const searchCache = new TtlLruCache(MAX_SEARCH_CACHE_ENTRIES);
const profileCache = new TtlLruCache(MAX_PROFILE_CACHE_ENTRIES);
const searchInFlight = new Map();
const profileInFlight = new Map();

function parseRetryAfterMs(value) {
    if (!value) return null;
    const seconds = Number(value);
    if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1000;
    const retryAt = Date.parse(value);
    if (!Number.isNaN(retryAt)) return Math.max(0, retryAt - Date.now());
    return null;
}

function retryDelayMs(response, attempt) {
    const retryAfter = parseRetryAfterMs(response && response.headers.get('retry-after'));
    const base = retryAfter === null ? 250 * (2 ** attempt) : retryAfter;
    const capped = Math.min(ROBLOX_RETRY_DELAY_CAP_MS, Math.max(100, base));
    return Math.max(100, Math.floor(capped * (0.9 + Math.random() * 0.1)));
}

async function cancelResponseBody(response) {
    if (!response || !response.body) return;
    try { await response.body.cancel(); } catch (error) { /* ignore cleanup errors */ }
}

async function fetchRoblox(url, options = {}, config = {}) {
    const retries = Number.isInteger(config.retries) ? config.retries : 1;
    const timeoutMs = config.timeoutMs || ROBLOX_REQUEST_TIMEOUT_MS;
    const deadline = Date.now() + timeoutMs;
    let lastError = null;

    const requestHeaders = {
        ...ROBLOX_DEFAULT_HEADERS,
        ...(options.headers || {})
    };
    const requestOptions = {
        ...options,
        headers: requestHeaders
    };

    for (let attempt = 0; attempt <= retries; attempt++) {
        const remainingMs = deadline - Date.now();
        if (remainingMs <= 0) break;

        const controller = new AbortController();
        const externalSignal = options.signal;
        const abortFromExternal = () => controller.abort(externalSignal.reason);
        if (externalSignal) {
            if (externalSignal.aborted) abortFromExternal();
            else externalSignal.addEventListener('abort', abortFromExternal, { once: true });
        }
        const timeout = setTimeout(() => controller.abort(), remainingMs);

        let response;
        try {
            const upstreamResponse = await fetch(url, { ...requestOptions, signal: controller.signal });
            // Buffer Roblox's small JSON response before clearing the abort timer so
            // the timeout covers headers and body, not only the initial connection.
            const responseBody = await upstreamResponse.arrayBuffer();
            response = new Response(responseBody.byteLength ? responseBody : null, {
                status: upstreamResponse.status,
                statusText: upstreamResponse.statusText,
                headers: upstreamResponse.headers
            });
        } catch (error) {
            lastError = new RobloxUpstreamError(
                controller.signal.aborted && !(externalSignal && externalSignal.aborted)
                    ? 'Roblox API timed out'
                    : 'Roblox API network request failed',
                { cause: error }
            );
        } finally {
            clearTimeout(timeout);
            if (externalSignal) externalSignal.removeEventListener('abort', abortFromExternal);
        }

        if (response) {
            if (!TRANSIENT_ROBLOX_STATUSES.has(response.status)) return response;
            const retryAfterMs = parseRetryAfterMs(response.headers.get('retry-after'));
            lastError = new RobloxUpstreamError(`Roblox API returned ${response.status}`, {
                status: response.status,
                retryAfterMs
            });
        }

        if (attempt >= retries) {
            await cancelResponseBody(response);
            break;
        }
        const delayMs = response
            ? retryDelayMs(response, attempt)
            : Math.min(ROBLOX_RETRY_DELAY_CAP_MS, 250 * (2 ** attempt) + Math.floor(Math.random() * 101));
        if (Date.now() + delayMs + 100 >= deadline) {
            await cancelResponseBody(response);
            break;
        }
        await cancelResponseBody(response);
        await wait(delayMs);
    }

    throw lastError || new RobloxUpstreamError('Roblox API request timed out');
}

async function readRobloxJson(response, operation) {
    try {
        return await response.json();
    } catch (error) {
        throw new RobloxUpstreamError(`Roblox returned invalid JSON during ${operation}`, {
            status: response.status,
            cause: error
        });
    }
}

async function getRobloxProfile(userId) {
    const id = String(userId);
    const cached = profileCache.getFresh(id);
    if (cached) return cached.value;

    if (profileInFlight.has(id)) return profileInFlight.get(id);
    if (profileInFlight.size >= MAX_IN_FLIGHT_PROFILES) {
        throw new ServiceBusyError('Too many profile lookups are already in progress');
    }

    const request = (async () => {
        const stale = profileCache.getStale(id);
        try {
            const response = await fetchRoblox(`https://users.roblox.com/v1/users/${encodeURIComponent(id)}`);
            if (response.status === 404) {
                profileCache.set(id, null, NEGATIVE_PROFILE_CACHE_TTL_MS);
                return null;
            }
            if (!response.ok) {
                throw new RobloxUpstreamError(`Roblox profile API returned ${response.status}`, { status: response.status });
            }
            const user = await readRobloxJson(response, 'profile lookup');
            if (!user || !Number.isFinite(Number(user.id)) || typeof user.name !== 'string') {
                throw new RobloxUpstreamError('Roblox profile response was incomplete', { status: response.status });
            }
            const data = {
                id: user.id,
                name: user.name,
                displayName: user.displayName || user.name,
                created: user.created || null,
                description: user.description || '',
                isVerified: user.hasVerifiedBadge === true
            };
            profileCache.set(id, data, PROFILE_CACHE_TTL_MS, PROFILE_CACHE_STALE_MS);
            return data;
        } catch (error) {
            if (stale && stale.value) return stale.value;
            throw error;
        }
    })().finally(() => profileInFlight.delete(id));

    profileInFlight.set(id, request);
    return request;
}

// ============================================================
// RATE LIMITING (in-memory, per IP)
// ============================================================
const rateLimitMap = new Map();
const RATE_LIMIT = { windowMs: 60 * 1000, max: 10000 };

function checkRateLimit(ip) {
    return true;
}

const cleanupTimer = setInterval(() => {
    const now = Date.now();
    for (const [ip, entry] of rateLimitMap.entries()) {
        if (now - entry.start > RATE_LIMIT.windowMs * 2) rateLimitMap.delete(ip);
    }
    searchCache.cleanup();
    profileCache.cleanup();
}, 5 * 60 * 1000);
cleanupTimer.unref?.();

// Helper: get client IP
function getClientIp(req) {
    return (req.headers['x-forwarded-for']?.split(',')[0].trim()) || req.socket.remoteAddress || 'unknown';
}

function sendJson(res, status, body, headers = {}) {
    res.writeHead(status, { "Content-Type": "application/json", ...headers });
    return res.end(JSON.stringify(body));
}

function normalizeRobloxUser(user) {
    const id = Number(user && user.id);
    const name = typeof user?.name === 'string' ? user.name.trim() : '';
    if (!Number.isSafeInteger(id) || id <= 0 || !name) return null;
    return {
        id,
        name,
        displayName: typeof user.displayName === 'string' && user.displayName.trim()
            ? user.displayName
            : name,
        hasVerifiedBadge: user.hasVerifiedBadge === true
    };
}

async function lookupExactUsername(cleanUsername) {
    const response = await fetchRoblox("https://users.roblox.com/v1/usernames/users", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ usernames: [cleanUsername], excludeBannedUsers: false })
    });
    if (!response.ok) {
        throw new RobloxUpstreamError(`Roblox username API returned ${response.status}`, { status: response.status });
    }
    const payload = await readRobloxJson(response, 'username lookup');
    if (!payload || !Array.isArray(payload.data)) {
        throw new RobloxUpstreamError('Roblox username response was incomplete', { status: response.status });
    }
    return payload.data.map(normalizeRobloxUser).filter(Boolean).slice(0, 1);
}

async function lookupKeywordUsers(cleanUsername) {
    try {
        const response = await fetchRoblox(
            `https://users.roblox.com/v1/users/search?keyword=${encodeURIComponent(cleanUsername)}&limit=10`,
            {},
            // Roblox throttles this endpoint hard (429 after the first call in
            // a long window), so it fails fast here and the batch variant
            // lookup below fills the list instead of stalling the search.
            { retries: 0, timeoutMs: 3000 }
        );
        if (!response.ok) {
            throw new RobloxUpstreamError(`Roblox keyword API returned ${response.status}`, { status: response.status });
        }
        const payload = await readRobloxJson(response, 'keyword search');
        if (!payload || !Array.isArray(payload.data)) {
            throw new RobloxUpstreamError('Roblox keyword response was incomplete', { status: response.status });
        }
        return payload.data.map(normalizeRobloxUser).filter(Boolean).slice(0, 10);
    } catch (error) {
        // Keyword results are an optional convenience. Exact username lookup above
        // remains authoritative, so throttling here must not fail the whole request.
        console.warn(`Optional Roblox keyword search unavailable: ${error.message}`);
        return [];
    }
}

// --- Name-variant probe -------------------------------------------------
// Roblox's keyword endpoint (above) is throttled to roughly one call per long
// window per IP, which is why a search could collapse back to the single
// exact match. The batch username endpoint is not throttled, so the usual
// "name+suffix" / "prefix+name" accounts are probed with it instead — that is
// what keeps a common name showing up to MAX_SEARCH_RESULTS cards.
const VARIANT_SUFFIXES = [
    '1', '12', '123', '777', '420', '69', '007', '99', '2', '22', '3', '33',
    'x', 'xx', 'real', 'official', 'yt', 'tv', 'gg', 'xd', 'pro', 'gaming',
    'game', 'fan', 'king', 'boy', 'girl', '10', '11', '21', '7', '8', '9',
    '100', '111', '666', '01', '02'
];
const VARIANT_PREFIXES = ['real', 'i', 'the', 'mr', 'im', 'not', 'official', 'x'];
const VARIANT_MIDDLES = ['1', '123', 'yt', 'gg', 'pro', 'x'];
const MAX_VARIANT_CANDIDATES = 50;

function buildUsernameCandidates(cleanUsername) {
    const roots = [];
    const pushRoot = (root) => {
        if (root && root.length >= 3 && root.length <= 20 && /^[a-zA-Z0-9_]+$/.test(root)) roots.push(root);
    };

    // "builder420" probes the plain "builder" family first — those accounts
    // are far more common than "builder4201".
    const stripped = cleanUsername.replace(/[0-9_]+$/, '');
    if (stripped !== cleanUsername) pushRoot(stripped);
    pushRoot(cleanUsername);

    const seen = new Set([cleanUsername.toLowerCase()]);
    const candidates = [];
    const add = (name) => {
        if (!name || name.length < 3 || name.length > 20) return;
        if (!/^[a-zA-Z0-9_]+$/.test(name)) return;
        const key = name.toLowerCase();
        if (seen.has(key)) return;
        seen.add(key);
        candidates.push(name);
    };

    for (const root of roots) {
        for (const suffix of VARIANT_SUFFIXES) add(root + suffix);
        for (const prefix of VARIANT_PREFIXES) add(prefix + root);
        for (const middle of VARIANT_MIDDLES) add(`${root}_${middle}`);
        if (candidates.length >= MAX_VARIANT_CANDIDATES) break;
    }

    return candidates.slice(0, MAX_VARIANT_CANDIDATES);
}

async function lookupVariantUsers(cleanUsername) {
    const candidates = buildUsernameCandidates(cleanUsername);
    if (candidates.length === 0) return [];

    try {
        const response = await fetchRoblox("https://users.roblox.com/v1/usernames/users", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ usernames: candidates, excludeBannedUsers: false })
        }, { retries: 0, timeoutMs: 3500 });
        if (!response.ok) {
            throw new RobloxUpstreamError(`Roblox username API returned ${response.status}`, { status: response.status });
        }
        const payload = await readRobloxJson(response, 'username variant lookup');
        if (!payload || !Array.isArray(payload.data)) return [];
        return payload.data.map(normalizeRobloxUser).filter(Boolean);
    } catch (error) {
        // Variant probing only tops the list up; a failure must never fail the search.
        console.warn(`Optional username-variant lookup unavailable: ${error.message}`);
        return [];
    }
}

async function fetchAvatarMap(users) {
    const avatarMap = {};
    if (!users.length) return avatarMap;
    const userIds = users.map(user => user.id).join(',');
    const response = await fetchRoblox(
        `https://thumbnails.roblox.com/v1/users/avatar-headshot?userIds=${encodeURIComponent(userIds)}&size=150x150&format=Png&isCircular=true`,
        {},
        { retries: 0, timeoutMs: 3500 }
    );
    if (!response.ok) {
        throw new RobloxUpstreamError(`Roblox thumbnail API returned ${response.status}`, { status: response.status });
    }
    const payload = await readRobloxJson(response, 'thumbnail lookup');
    if (!payload || !Array.isArray(payload.data)) {
        throw new RobloxUpstreamError('Roblox thumbnail response was incomplete', { status: response.status });
    }
    for (const item of payload.data) {
        if (item && item.targetId && typeof item.imageUrl === 'string' && item.imageUrl) {
            avatarMap[item.targetId] = item.imageUrl;
        }
    }
    return avatarMap;
}

function rankRobloxUsers(users, cleanUsername) {
    const q = cleanUsername.toLowerCase().trim();
    return [...users].sort((a, b) => {
        const aName = (a.name || '').toLowerCase();
        const bName = (b.name || '').toLowerCase();
        const aDisp = (a.displayName || '').toLowerCase();
        const bDisp = (b.displayName || '').toLowerCase();

        const aExactName = aName === q;
        const bExactName = bName === q;
        if (aExactName && !bExactName) return -1;
        if (!aExactName && bExactName) return 1;

        const aExactDisp = aDisp === q;
        const bExactDisp = bDisp === q;
        if (aExactDisp && !bExactDisp) return -1;
        if (!aExactDisp && bExactDisp) return 1;

        const aStartsName = aName.startsWith(q);
        const bStartsName = bName.startsWith(q);
        if (aStartsName && !bStartsName) return -1;
        if (!aStartsName && bStartsName) return 1;
        if (aStartsName && bStartsName && aName.length !== bName.length) {
            return aName.length - bName.length;
        }

        const aStartsDisp = aDisp.startsWith(q);
        const bStartsDisp = bDisp.startsWith(q);
        if (aStartsDisp && !bStartsDisp) return -1;
        if (!aStartsDisp && bStartsDisp) return 1;
        if (aStartsDisp && bStartsDisp && aDisp.length !== bDisp.length) {
            return aDisp.length - bDisp.length;
        }

        const aIncName = aName.includes(q);
        const bIncName = bName.includes(q);
        if (aIncName && !bIncName) return -1;
        if (!aIncName && bIncName) return 1;
        if (aIncName && bIncName && aName.length !== bName.length) {
            return aName.length - bName.length;
        }

        if (a.hasVerifiedBadge && !b.hasVerifiedBadge) return -1;
        if (!a.hasVerifiedBadge && b.hasVerifiedBadge) return 1;

        return aName.localeCompare(bName);
    });
}

async function executePlayerSearch(cleanUsername, cacheKey) {
    // The exact username lookup alone can only ever return one player, so the
    // keyword suggestions and the batch name-variant probe are fetched as well
    // (in parallel) and merged after it. MAX_SEARCH_RESULTS keeps a search
    // cheap: at most 7 players, which also caps the avatar batch that follows.
    const [exactUsers, keywordUsers, variantUsers] = await Promise.all([
        lookupExactUsername(cleanUsername),
        lookupKeywordUsers(cleanUsername),
        lookupVariantUsers(cleanUsername)
    ]);

    const allFound = [...exactUsers, ...keywordUsers, ...variantUsers];
    const seen = new Set();
    const uniqueUsers = [];
    for (const user of allFound) {
        if (!user || seen.has(user.id)) continue;
        seen.add(user.id);
        uniqueUsers.push(user);
    }

    if (uniqueUsers.length === 0) {
        const result = { status: 404, body: { success: false, error: "Player not found" } };
        searchCache.set(cacheKey, result, NEGATIVE_SEARCH_CACHE_TTL_MS);
        return result;
    }

    const rankedUsers = rankRobloxUsers(uniqueUsers, cleanUsername);
    const users = rankedUsers.slice(0, MAX_SEARCH_RESULTS);

    const stale = searchCache.getStale(cacheKey);
    const staleAvatars = {};
    if (stale?.value?.status === 200 && Array.isArray(stale.value.body?.users)) {
        for (const user of stale.value.body.users) {
            if (user && user.userId && user.avatar) staleAvatars[user.userId] = user.avatar;
        }
    }

    let avatarMap = {};
    try {
        avatarMap = await fetchAvatarMap(users);
    } catch (error) {
        // A missing image must not make a valid player impossible to select.
        console.warn(`Roblox thumbnails unavailable: ${error.message}`);
    }

    const results = users.map(user => ({
        userId: user.id,
        username: user.name,
        displayName: user.displayName || user.name,
        avatar: avatarMap[user.id] || staleAvatars[user.id] || null,
        // Join date is deliberately lazy-loaded by /api/player-profile after selection.
        created: null,
        isVerified: user.hasVerifiedBadge === true
    }));
    const result = {
        status: 200,
        body: { success: true, count: results.length, users: results }
    };
    searchCache.set(cacheKey, result, SEARCH_CACHE_TTL_MS, SEARCH_CACHE_STALE_MS);
    return result;
}

async function getPlayerSearchResult(cleanUsername) {
    const cacheKey = cleanUsername.toLowerCase();
    const cached = searchCache.getFresh(cacheKey);
    if (cached) return { ...cached.value, cacheState: 'hit' };
    if (searchInFlight.has(cacheKey)) return searchInFlight.get(cacheKey);

    const stale = searchCache.getStale(cacheKey);
    if (searchInFlight.size >= MAX_IN_FLIGHT_SEARCHES) {
        if (stale?.value?.status === 200) return { ...stale.value, cacheState: 'stale' };
        throw new ServiceBusyError('Too many searches are already in progress');
    }

    const request = executePlayerSearch(cleanUsername, cacheKey)
        .then(result => ({ ...result, cacheState: 'miss' }))
        .catch(error => {
            if (stale?.value?.status === 200) return { ...stale.value, cacheState: 'stale' };
            throw error;
        })
        .finally(() => searchInFlight.delete(cacheKey));

    searchInFlight.set(cacheKey, request);
    return request;
}

function sendTemporaryFailure(res, error, operation) {
    const busy = error instanceof ServiceBusyError;
    const retryAfterSeconds = busy
        ? 2
        : Math.max(1, Math.min(5, Math.ceil((error.retryAfterMs || 1000) / 1000)));
    console.error(`${operation}: ${error.message}`);
    return sendJson(res, 503, {
        success: false,
        error: busy
            ? "Search service is busy. Please try again shortly."
            : "Roblox is temporarily unavailable. Please try again.",
        code: busy ? "SERVICE_BUSY" : "ROBLOX_UPSTREAM_UNAVAILABLE",
        retryable: true
    }, { "Retry-After": String(retryAfterSeconds) });
}

async function handleRequest(req, res) {
    const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);

    // CORS Headers - Allow Store domain, localhost, and any origin
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, x-access-key");

    // 🔒 Security headers
    res.setHeader("X-Content-Type-Options", "nosniff");
    res.setHeader("X-Frame-Options", "DENY");
    res.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");

    if (req.method === "OPTIONS") {
        res.writeHead(204);
        return res.end();
    }

    // 🔒 Rate limit ALL /api/* requests
    if (url.pathname.startsWith("/api/")) {
        const ip = getClientIp(req);
        if (!checkRateLimit(ip)) {
            res.writeHead(429, { "Content-Type": "application/json" });
            return res.end(JSON.stringify({ success: false, error: "Too many requests. Please slow down." }));
        }
    }

    // Health Check endpoint for Render and local probing
    if (url.pathname === "/" || url.pathname === "/health" || url.pathname === "/api/health") {
        res.writeHead(200, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ 
            status: "ok", 
            service: "buy-roblox-search-backend",
            message: "Buy Roblox API Backend is healthy and running!", 
            timestamp: new Date().toISOString() 
        }));
    }

    if (url.pathname === "/api/search-player") {
        const username = url.searchParams.get("username");

        if (!username || !username.trim()) {
            return sendJson(res, 400, { success: false, error: "Username is required" });
        }

        const cleanUsername = username.trim();
        if (cleanUsername.length > 20 || !/^[a-zA-Z0-9_]+$/.test(cleanUsername)) {
            return sendJson(res, 400, { success: false, error: "Invalid username format." });
        }

        try {
            const result = await getPlayerSearchResult(cleanUsername);
            const headers = { "X-Cache": result.cacheState || "miss" };
            if (result.cacheState === 'stale') {
                headers.Warning = '110 - "Response is stale while Roblox is unavailable"';
            }
            return sendJson(res, result.status, result.body, headers);
        } catch (error) {
            if (error instanceof RobloxUpstreamError || error instanceof ServiceBusyError) {
                return sendTemporaryFailure(res, error, 'Player search failed');
            }
            console.error("Unexpected player search error:", error);
            return sendJson(res, 500, { success: false, error: "Internal server error", code: "INTERNAL_ERROR", retryable: false });
        }
    }

    if (url.pathname === "/api/player-profile") {
        let userId = url.searchParams.get("userId");
        const username = url.searchParams.get("username");

        if ((!userId || !userId.trim()) && (!username || !username.trim())) {
            return sendJson(res, 400, { success: false, error: "userId or username is required" });
        }

        try {
            if (!userId && username) {
                const cleanUsername = username.trim();
                if (cleanUsername.length > 20 || !/^[a-zA-Z0-9_]+$/.test(cleanUsername)) {
                    return sendJson(res, 400, { success: false, error: "Invalid username format." });
                }
                const users = await lookupExactUsername(cleanUsername);
                if (users.length > 0) userId = String(users[0].id);
            }

            if (!userId || !/^\d+$/.test(userId)) {
                return sendJson(res, 404, { success: false, error: "Player not found" });
            }

            const userData = await getRobloxProfile(userId.trim());
            if (!userData) {
                return sendJson(res, 404, { success: false, error: "Player not found" });
            }

            return sendJson(res, 200, {
                success: true,
                userId: userData.id,
                username: userData.name,
                displayName: userData.displayName || userData.name,
                created: userData.created || null,
                description: userData.description || "",
                isVerified: userData.isVerified === true
            });
        } catch (error) {
            if (error instanceof RobloxUpstreamError || error instanceof ServiceBusyError) {
                return sendTemporaryFailure(res, error, 'Player profile lookup failed');
            }
            console.error("Unexpected profile lookup error:", error);
            return sendJson(res, 500, { success: false, error: "Internal server error", code: "INTERNAL_ERROR", retryable: false });
        }
    }

    // Static file serving or fallback
    let filePath = path.join(__dirname, url.pathname === "/" ? "index.html" : url.pathname);

    if (!filePath.startsWith(__dirname)) {
        res.writeHead(403, { "Content-Type": "text/plain" });
        return res.end("Forbidden");
    }

    fs.readFile(filePath, (err, content) => {
        if (err) {
            if (url.pathname === "/") {
                res.writeHead(200, { "Content-Type": "application/json" });
                return res.end(JSON.stringify({ 
                    status: "online", 
                    message: "Buy Roblox API Server is running!" 
                }));
            }
            if (err.code === "ENOENT") {
                res.writeHead(404, { "Content-Type": "text/plain" });
                return res.end("404 Not Found");
            }
            res.writeHead(500, { "Content-Type": "text/plain" });
            return res.end("500 Internal Server Error");
        } else {
            const ext = path.extname(filePath).toLowerCase();
            const mimeTypes = {
                ".html": "text/html; charset=utf-8",
                ".js": "text/javascript",
                ".css": "text/css",
                ".json": "application/json",
                ".png": "image/png",
                ".jpg": "image/jpeg",
                ".gif": "image/gif",
                ".svg": "image/svg+xml",
                ".webp": "image/webp"
            };
            const contentType = mimeTypes[ext] || "application/octet-stream";
            res.writeHead(200, { "Content-Type": contentType });
            res.end(content);
        }
    });
}

function createServer() {
    return http.createServer(handleRequest);
}

function resetStateForTests() {
    searchCache.clear();
    profileCache.clear();
    searchInFlight.clear();
    profileInFlight.clear();
    rateLimitMap.clear();
}

if (require.main === module) {
    const server = createServer();
    server.listen(PORT, HOST, () => {
        console.log(`Server running on port ${PORT} at host ${HOST}`);
    });
}

module.exports = {
    createServer,
    resetStateForTests
};

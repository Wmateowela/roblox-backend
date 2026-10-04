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
const GLOBAL_SEARCH_RATE_LIMIT = { windowMs: 1000, max: 100 };
const DEVICE_SEARCH_ABUSE_LIMIT = { windowMs: 1000, max: 50 };
const SEARCH_ABUSE_BLOCK_MS = 10 * 60 * 1000;
const SEARCH_AVATAR_TIMEOUT_MS = 1000;
const FRIEND_BOOTSTRAP_SIZE = 15;
const FRIEND_BOOTSTRAP_CANDIDATES = 50;
const FRIEND_BOOTSTRAP_DAILY_LIMIT = 10;
const ONE_DAY_MS = 24 * 60 * 60 * 1000;
// Players returned per search: the exact username first, then keyword
// suggestions, never more than this (keeps the avatar batch small too).
const MAX_SEARCH_RESULTS = 12;

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
const keywordCache = new TtlLruCache(500);
const searchInFlight = new Map();
const profileInFlight = new Map();
const friendBootstrapDailyMap = new Map();
let robloxUsernamePool = null;
// When Roblox throttles keyword searches, stop hammering the API for a short
// cooldown. Otherwise every search pays the timeout/retry cost for suggestions
// that Roblox will never return, making the site feel slow.
let keywordSearchCooldownUntil = 0;
// Shared upstream brake: when Roblox 429-throttles ANY endpoint, all Roblox
// calls pause for a cooldown instead of burning retries on every request.
let robloxRateLimitCooldownUntil = 0;
let robloxCooldownLogAt = 0;
function noteRobloxRateLimit() {
    robloxRateLimitCooldownUntil = Date.now() + 60 * 1000;
    keywordSearchCooldownUntil = robloxRateLimitCooldownUntil;
    const now = Date.now();
    if (now - robloxCooldownLogAt > 55 * 1000) {
        robloxCooldownLogAt = now;
        console.warn('Roblox rate-limited us (429). Pausing all Roblox API calls for 60s.');
    }
}
function robloxCooldownRemainingMs() {
    return Math.max(0, robloxRateLimitCooldownUntil - Date.now());
}

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
            const upstreamResponse = await fetch(url, { ...options, signal: controller.signal });
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
            if (response.status === 429) {
                // Only user-facing search endpoints trigger the global brake;
                // avatar image calls 429 too often and would block everything.
                if (/users\.roblox\.com\/v1\/(usernames\/users|users\/search|users\/\d)/.test(url)) {
                    noteRobloxRateLimit();
                }
            }
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

    const cooldown = robloxCooldownRemainingMs();
    if (cooldown > 0) {
        const staleNow = profileCache.getStale(id);
        if (staleNow && staleNow.value) return staleNow.value;
        throw new RobloxUpstreamError('Roblox API rate-limited; cooling down', { status: 429, retryAfterMs: cooldown });
    }

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
// SEARCH RATE LIMITING (in-memory)
// ============================================================
const rateLimitMap = new Map();
const searchAbuseBlocks = new Map();
let globalSearchWindow = { start: 0, count: 0 };

// The service may accept at most 100 player-search requests per second across
// all visitors. A single device that sends more than 50 in that same second is
// a scripted flood, so only that device is temporarily blocked. Ordinary UI
// searches issue one request and never come close to either threshold.
function checkSearchRateLimit(ip) {
    const now = Date.now();
    const blockedUntil = searchAbuseBlocks.get(ip) || 0;
    if (blockedUntil > now) {
        return { allowed: false, abuse: true, retryAfterMs: blockedUntil - now };
    }
    if (blockedUntil) searchAbuseBlocks.delete(ip);

    let entry = rateLimitMap.get(ip);
    if (!entry || (now - entry.start) >= DEVICE_SEARCH_ABUSE_LIMIT.windowMs) {
        if (!entry && rateLimitMap.size >= MAX_RATE_LIMIT_ENTRIES) {
            rateLimitMap.delete(rateLimitMap.keys().next().value);
        }
        entry = { start: now, count: 0 };
    }
    entry.count++;
    rateLimitMap.delete(ip);
    rateLimitMap.set(ip, entry);
    if (entry.count > DEVICE_SEARCH_ABUSE_LIMIT.max) {
        const until = now + SEARCH_ABUSE_BLOCK_MS;
        searchAbuseBlocks.set(ip, until);
        return { allowed: false, abuse: true, retryAfterMs: SEARCH_ABUSE_BLOCK_MS };
    }

    if (!globalSearchWindow.start || now - globalSearchWindow.start >= GLOBAL_SEARCH_RATE_LIMIT.windowMs) {
        globalSearchWindow = { start: now, count: 0 };
    }
    globalSearchWindow.count++;
    if (globalSearchWindow.count > GLOBAL_SEARCH_RATE_LIMIT.max) {
        return {
            allowed: false,
            abuse: false,
            retryAfterMs: Math.max(1, GLOBAL_SEARCH_RATE_LIMIT.windowMs - (now - globalSearchWindow.start))
        };
    }

    return { allowed: true, abuse: false, retryAfterMs: 0 };
}

function consumeFriendBootstrapQuota(ip) {
    const now = Date.now();
    const day = Math.floor(now / ONE_DAY_MS);
    const current = friendBootstrapDailyMap.get(ip);
    const entry = current && current.day === day ? current : { day, count: 0 };
    if (entry.count >= FRIEND_BOOTSTRAP_DAILY_LIMIT) {
        return {
            allowed: false,
            retryAfterMs: ((day + 1) * ONE_DAY_MS) - now
        };
    }
    entry.count++;
    if (!current && friendBootstrapDailyMap.size >= MAX_RATE_LIMIT_ENTRIES) {
        friendBootstrapDailyMap.delete(friendBootstrapDailyMap.keys().next().value);
    }
    friendBootstrapDailyMap.delete(ip);
    friendBootstrapDailyMap.set(ip, entry);
    return { allowed: true, retryAfterMs: 0 };
}

const cleanupTimer = setInterval(() => {
    const now = Date.now();
    for (const [ip, entry] of rateLimitMap.entries()) {
        if (now - entry.start > DEVICE_SEARCH_ABUSE_LIMIT.windowMs * 2) rateLimitMap.delete(ip);
    }
    for (const [ip, blockedUntil] of searchAbuseBlocks.entries()) {
        if (blockedUntil <= now) searchAbuseBlocks.delete(ip);
    }
    const currentDay = Math.floor(now / ONE_DAY_MS);
    for (const [ip, entry] of friendBootstrapDailyMap.entries()) {
        if (!entry || entry.day !== currentDay) friendBootstrapDailyMap.delete(ip);
    }
    searchCache.cleanup();
    profileCache.cleanup();
    keywordCache.cleanup();
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
    if (robloxCooldownRemainingMs() > 0) {
        throw new RobloxUpstreamError('Roblox API rate-limited; cooling down', { status: 429, retryAfterMs: robloxCooldownRemainingMs() });
    }
    const response = await fetchRoblox("https://users.roblox.com/v1/usernames/users", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ usernames: [cleanUsername], excludeBannedUsers: false })
    }, { retries: 1, timeoutMs: 3500 });
    if (!response.ok) {
        throw new RobloxUpstreamError(`Roblox username API returned ${response.status}`, { status: response.status });
    }
    const payload = await readRobloxJson(response, 'username lookup');
    if (!payload || !Array.isArray(payload.data)) {
        throw new RobloxUpstreamError('Roblox username response was incomplete', { status: response.status });
    }
    return payload.data.map(normalizeRobloxUser).filter(Boolean).slice(0, 1);
}

async function lookupExactUsernames(usernames) {
    const response = await fetchRoblox("https://users.roblox.com/v1/usernames/users", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ usernames, excludeBannedUsers: false })
    }, { retries: 1, timeoutMs: 5500 });
    if (!response.ok) {
        throw new RobloxUpstreamError(`Roblox username batch API returned ${response.status}`, { status: response.status });
    }
    const payload = await readRobloxJson(response, 'username batch lookup');
    if (!payload || !Array.isArray(payload.data)) {
        throw new RobloxUpstreamError('Roblox username batch response was incomplete', { status: response.status });
    }
    return payload.data.map(normalizeRobloxUser).filter(Boolean);
}

async function lookupKeywordUsers(cleanUsername) {
    const cacheKey = cleanUsername.toLowerCase();
    const cached = keywordCache.getFresh(cacheKey);
    if (cached) return cached.value;

    // Cooldown after Roblox throttling: suggestions are optional, so skip the
    // network call entirely instead of failing slowly on every keystroke.
    if (Date.now() < keywordSearchCooldownUntil) {
        return [];
    }

    try {
        const response = await fetchRoblox(
            `https://users.roblox.com/v1/users/search?keyword=${encodeURIComponent(cleanUsername)}&limit=25`,
            {},
            // Suggestions are optional and must never hold the visible exact
            // username result for several seconds when Roblox throttles them.
            // Increased timeout to 4000ms to handle deployed server latency.
            { retries: 0, timeoutMs: 2500 }
        );
        if (!response.ok) {
            throw new RobloxUpstreamError(`Roblox keyword API returned ${response.status}`, { status: response.status });
        }
        const payload = await readRobloxJson(response, 'keyword search');
        if (!payload || !Array.isArray(payload.data)) {
            throw new RobloxUpstreamError('Roblox keyword response was incomplete', { status: response.status });
        }
        const users = payload.data.map(normalizeRobloxUser).filter(Boolean).slice(0, 25);
        keywordCache.set(cacheKey, users, SEARCH_CACHE_TTL_MS);
        return users;
    } catch (error) {
        // Keyword results are an optional convenience. Exact username lookup above
        // remains authoritative, so throttling here must not fail the whole request.
        if (error && error.status === 429) {
            noteRobloxRateLimit();
            keywordCache.set(cacheKey, [], 30 * 1000);
        } else {
            keywordCache.set(cacheKey, [], 30 * 1000);
            console.warn(`Optional Roblox keyword search unavailable: ${error.message}`);
        }
        return [];
    }
}

async function fetchAvatarMap(users, timeoutMs = SEARCH_AVATAR_TIMEOUT_MS) {
    const avatarMap = {};
    if (!users.length) return avatarMap;
    const userIds = users.map(user => user.id).join(',');
    const response = await fetchRoblox(
        `https://thumbnails.roblox.com/v1/users/avatar-headshot?userIds=${encodeURIComponent(userIds)}&size=150x150&format=Png&isCircular=true`,
        {},
        // A thumbnail is cosmetic. Never make a valid player wait several
        // seconds when Roblox's image service is slow or throttled.
        { retries: 0, timeoutMs }
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

// Small built-in fallback used only when RobloxUserName.txt is missing from
// the deployment. Real usernames that reliably resolve on Roblox.
const FALLBACK_USERNAME_POOL = [
    'Roblox', 'Builderman', 'Telamon', 'Stickmasterluke', 'Shedletsky',
    'DominusCamorani', 'NotPeteHealy', 'Kleeck', 'Quackity', 'FlamingoBen',
    'lisadenn57', 'AxolotlJayFish', 'Ninja', 'Dream', 'Technoblade',
    'CaptainSparklez', 'HipMC', 'GeorgeNotFound', 'Sapnap', 'BadLion',
    'itsSUSO', 'Mini Ladd', 'Skeppy', 'TheOdd1sOut', 'DanTDM',
    'PewDiePie', 'Markiplier', 'MrBeast', 'LoganPaul', 'JakePaul',
    'VanossGaming', 'PrestonPlayz', 'Unspeakable', 'Popularmmos', 'Muselk'
];

function getRobloxUsernamePool() {
    if (robloxUsernamePool) return robloxUsernamePool;
    const candidates = [
        path.join(__dirname, 'RobloxUserName.txt'),
        path.join(__dirname, '..', 'Frontend', 'RobloxUserName.txt')
    ];
    const sourcePath = candidates.find(candidate => fs.existsSync(candidate));
    if (!sourcePath) {
        // File missing (e.g. not committed to the Render rootDir). Fall back to a
        // small built-in pool instead of failing every friend bootstrap call.
        console.warn('RobloxUserName.txt was not found. Using built-in fallback username pool.');
        robloxUsernamePool = FALLBACK_USERNAME_POOL.slice();
        return robloxUsernamePool;
    }

    const seen = new Set();
    robloxUsernamePool = fs.readFileSync(sourcePath, 'utf8')
        .split(/\r?\n/)
        .map(value => value.trim())
        .filter(value => {
            const key = value.toLowerCase();
            if (!/^[a-zA-Z0-9_]{3,20}$/.test(value) || seen.has(key)) return false;
            seen.add(key);
            return true;
        });
    if (robloxUsernamePool.length < FRIEND_BOOTSTRAP_SIZE) {
        throw new Error('RobloxUserName.txt does not contain enough valid usernames');
    }
    return robloxUsernamePool;
}

function randomSample(values, count) {
    const sample = values.slice();
    const limit = Math.min(count, sample.length);
    for (let index = 0; index < limit; index++) {
        const swapIndex = index + Math.floor(Math.random() * (sample.length - index));
        [sample[index], sample[swapIndex]] = [sample[swapIndex], sample[index]];
    }
    return sample.slice(0, limit);
}

async function createRandomFriendProfiles() {
    const candidates = randomSample(getRobloxUsernamePool(), FRIEND_BOOTSTRAP_CANDIDATES);
    const resolved = await lookupExactUsernames(candidates);
    const unique = [];
    const seen = new Set();
    for (const user of randomSample(resolved, resolved.length)) {
        if (seen.has(user.id)) continue;
        seen.add(user.id);
        unique.push(user);
        if (unique.length === FRIEND_BOOTSTRAP_SIZE) break;
    }
    if (unique.length < FRIEND_BOOTSTRAP_SIZE) {
        throw new RobloxUpstreamError('Roblox returned fewer than 15 usable friend profiles');
    }

    let avatarMap = {};
    try {
        avatarMap = await fetchAvatarMap(unique, 2500);
    } catch (error) {
        console.warn(`Roblox friend thumbnails unavailable: ${error.message}`);
    }
    return unique.map(user => ({
        userId: user.id,
        username: user.name,
        displayName: user.displayName || user.name,
        avatar: avatarMap[user.id] || null,
        created: null,
        isVerified: user.hasVerifiedBadge === true
    }));
}

// Rank merge order: an exact username always leads, then names starting with
// the query, then the rest — so "roblox" shows Roblox first and similar names
// after it instead of Roblox's own relevance order.
function rankSearchResults(users, cleanUsername) {
    const needle = cleanUsername.toLowerCase();
    return users
        .map((user, index) => ({ user, index, rank: (() => {
            const name = user.name.toLowerCase();
            if (name === needle) return 0;
            if (name.startsWith(needle)) return 1;
            if (name.includes(needle)) return 2;
            return 3;
        })() }))
        .sort((a, b) => (a.rank - b.rank) || (a.index - b.index))
        .map(entry => entry.user);
}

async function executePlayerSearch(cleanUsername, cacheKey) {
    // Resolve the authoritative exact username and similar-name suggestions
    // together. Ranking below always pins the true exact match to the top,
    // while the bounded merge restores the wider player picker list.
    const [exactResult, keywordUsers] = await Promise.all([
        lookupExactUsername(cleanUsername)
            .then(users => ({ users }))
            .catch(error => ({ error })),
        lookupKeywordUsers(cleanUsername)
    ]);
    const exactUsers = exactResult.users || [];

    // Both sources empty: if the exact lookup itself blew up, that is an
    // upstream outage (stale cache / 503 path) — not "player not found".
    if (!exactUsers.length && !keywordUsers.length && exactResult.error) {
        throw exactResult.error;
    }

    const merged = [];
    const seen = new Set();
    for (const user of [...exactUsers, ...keywordUsers]) {
        if (!user || seen.has(user.id)) continue;
        seen.add(user.id);
        merged.push(user);
    }
    const users = rankSearchResults(merged, cleanUsername).slice(0, MAX_SEARCH_RESULTS);

    if (users.length === 0) {
        const result = { status: 404, body: { success: false, error: "Player not found" } };
        searchCache.set(cacheKey, result, NEGATIVE_SEARCH_CACHE_TTL_MS);
        return result;
    }

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

    // Search-only budget: profile details and health probes do not count as a
    // new player search. This keeps the one-search/one-count rule predictable.
    if (url.pathname === "/api/search-player") {
        const ip = getClientIp(req);
        const limit = checkSearchRateLimit(ip);
        if (!limit.allowed) {
            const retryAfter = Math.max(1, Math.ceil(limit.retryAfterMs / 1000));
            return sendJson(res, 429, {
                success: false,
                error: limit.abuse
                    ? "Search temporarily blocked for this device because more than 50 requests were sent in one second."
                    : "Search capacity reached. Please try again in a moment.",
                code: limit.abuse ? "SEARCH_ABUSE_BLOCKED" : "SEARCH_RATE_LIMITED",
                retryable: !limit.abuse,
                retryAfter
            }, { "Retry-After": String(retryAfter) });
        }
    }

    if (url.pathname === "/api/friends-bootstrap") {
        if (req.method !== "POST") {
            return sendJson(res, 405, { success: false, error: "Method not allowed" }, { "Allow": "POST" });
        }
        const quota = consumeFriendBootstrapQuota(getClientIp(req));
        if (!quota.allowed) {
            const retryAfter = Math.max(1, Math.ceil(quota.retryAfterMs / 1000));
            return sendJson(res, 429, {
                success: false,
                error: "Daily friend refresh limit reached.",
                code: "FRIEND_BOOTSTRAP_DAILY_LIMIT",
                retryable: false,
                retryAfter
            }, { "Retry-After": String(retryAfter) });
        }
        try {
            const users = await createRandomFriendProfiles();
            return sendJson(res, 200, { success: true, count: users.length, users });
        } catch (error) {
            if (error instanceof RobloxUpstreamError || error instanceof ServiceBusyError) {
                return sendTemporaryFailure(res, error, 'Friend bootstrap failed');
            }
            console.error("Unexpected friend bootstrap error:", error);
            return sendJson(res, 500, { success: false, error: "Internal server error", code: "INTERNAL_ERROR", retryable: false });
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
    searchAbuseBlocks.clear();
    friendBootstrapDailyMap.clear();
    globalSearchWindow = { start: 0, count: 0 };
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

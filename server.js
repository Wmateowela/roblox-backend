const http = require("http");
const fs = require("fs");
const path = require("path");
const { URL } = require("url");

const PORT = process.env.PORT || 9090;
const HOST = "0.0.0.0";

// Reduce Roblox upstream load and tolerate a short temporary outage.
const profileCache = new Map();
const PROFILE_CACHE_MS = 10 * 60 * 1000;
const TRANSIENT_ROBLOX_STATUSES = new Set([429, 500, 502, 503, 504]);
const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function fetchRoblox(url, options = {}, retries = 1) {
    let lastError;
    for (let attempt = 0; attempt <= retries; attempt++) {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 9000);
        try {
            const response = await fetch(url, { ...options, signal: controller.signal });
            if (response.ok || !TRANSIENT_ROBLOX_STATUSES.has(response.status) || attempt === retries) return response;
            lastError = new Error(`Roblox API returned ${response.status}`);
        } catch (error) {
            lastError = error;
            if (attempt === retries) throw error;
        } finally {
            clearTimeout(timeout);
        }
        await wait(350 * (attempt + 1));
    }
    throw lastError || new Error('Roblox API request failed');
}

async function getRobloxProfile(userId) {
    const id = String(userId);
    const cached = profileCache.get(id);
    if (cached && Date.now() - cached.savedAt < PROFILE_CACHE_MS) return cached.data;
    const response = await fetchRoblox(`https://users.roblox.com/v1/users/${encodeURIComponent(id)}`);
    if (!response.ok) return null;
    const user = await response.json();
    const data = {
        id: user.id, name: user.name, displayName: user.displayName || user.name,
        created: user.created || null, description: user.description || '',
        isVerified: user.hasVerifiedBadge === true
    };
    profileCache.set(id, { savedAt: Date.now(), data });
    return data;
}

// ============================================================
// RATE LIMITING (in-memory, per IP)
// ============================================================
const rateLimitMap = new Map();
const RATE_LIMIT = { windowMs: 60 * 1000, max: 30 }; // 30 req/min per IP

function checkRateLimit(ip) {
    const now = Date.now();
    let entry = rateLimitMap.get(ip);
    if (!entry || (now - entry.start) > RATE_LIMIT.windowMs) {
        entry = { start: now, count: 0 };
    }
    entry.count++;
    rateLimitMap.set(ip, entry);
    return entry.count <= RATE_LIMIT.max;
}

setInterval(() => {
    const now = Date.now();
    for (const [ip, entry] of rateLimitMap.entries()) {
        if (now - entry.start > RATE_LIMIT.windowMs * 2) rateLimitMap.delete(ip);
    }
}, 5 * 60 * 1000);

// Helper: get client IP
function getClientIp(req) {
    return (req.headers['x-forwarded-for']?.split(',')[0].trim()) || req.socket.remoteAddress || 'unknown';
}

const server = http.createServer(async (req, res) => {
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
            res.writeHead(400, { "Content-Type": "application/json" });
            return res.end(JSON.stringify({ success: false, error: "Username is required" }));
        }

        // 🔒 Input validation
        if (username.length > 32 || !/^[a-zA-Z0-9_]+$/.test(username)) {
            res.writeHead(400, { "Content-Type": "application/json" });
            return res.end(JSON.stringify({ success: false, error: "Invalid username format." }));
        }

        try {
            const cleanUsername = username.trim();
            console.log(`Searching Roblox user & similar players for: ${cleanUsername}`);
            
            // Build variation candidates to search up to 15-20 related names
            const base = cleanUsername;
            const variations = [
                base,
                base + "1", base + "2", base + "3", base + "4", base + "5",
                base + "6", base + "7", base + "8", base + "9", base + "0",
                base + "_1", base + "_2", base + "_real", base + "123", base + "x",
                base + "_yt", base + "99", base + "01", base + "_dev"
            ];

            const robloxRes = await fetchRoblox("https://users.roblox.com/v1/usernames/users", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ usernames: variations, excludeBannedUsers: false })
            });

            if (!robloxRes.ok) {
                console.error(`Roblox Users API failed with status ${robloxRes.status}`);
                res.writeHead(502, { "Content-Type": "application/json" });
                return res.end(JSON.stringify({ success: false, error: "Roblox API request failed" }));
            }

            const data = await robloxRes.json();
            let foundUsers = data.data || [];

            if (foundUsers.length === 0) {
                // Fallback: Try Roblox keyword search to find users matching or containing this name
                try {
                    const kwRes = await fetchRoblox(`https://users.roblox.com/v1/users/search?keyword=${encodeURIComponent(cleanUsername)}&limit=10`);
                    if (kwRes.ok) {
                        const kwData = await kwRes.json();
                        if (kwData && Array.isArray(kwData.data) && kwData.data.length > 0) {
                            foundUsers = kwData.data;
                        }
                    }
                } catch (e) {}
            }

            if (foundUsers.length === 0) {
                res.writeHead(404, { "Content-Type": "application/json" });
                return res.end(JSON.stringify({ success: false, error: "Player not found" }));
            }

            // Sort so exact match comes first
            foundUsers.sort((a, b) => {
                const aExact = a.name.toLowerCase() === cleanUsername.toLowerCase();
                const bExact = b.name.toLowerCase() === cleanUsername.toLowerCase();
                if (aExact && !bExact) return -1;
                if (!aExact && bExact) return 1;
                return 0;
            });

            // Cap to maximum 15 users
            const users = foundUsers.slice(0, 15);
            const userIds = users.map(u => u.id).join(",");

            // Fetch headshot avatars in a single batch request for maximum speed
            const avatarMap = {};
            try {
                const thumbRes = await fetchRoblox(
                    `https://thumbnails.roblox.com/v1/users/avatar-headshot?userIds=${userIds}&size=150x150&format=Png&isCircular=true`
                );
                if (thumbRes.ok) {
                    const thumbData = await thumbRes.json();
                    if (thumbData.data) {
                        thumbData.data.forEach(item => {
                            if (item.targetId && item.imageUrl) {
                                avatarMap[item.targetId] = item.imageUrl;
                            }
                        });
                    }
                }
            } catch (thumbErr) {
                console.error("Error fetching avatar headshots:", thumbErr.message);
            }

            // Fetch user creation dates (real join dates) in parallel
            const profileMap = {};
            await Promise.allSettled(
                users.map(async (u) => {
                    try {
                        const profData = await getRobloxProfile(u.id);
                        if (profData) {
                            profileMap[u.id] = profData;
                        }
                    } catch (profErr) {}
                })
            );

            const results = users.map(u => ({
                userId: u.id,
                username: u.name,
                displayName: u.displayName || u.name,
                avatar: avatarMap[u.id] || null,
                created: profileMap[u.id]?.created || null,
                isVerified: profileMap[u.id]?.isVerified === true
            }));

            res.writeHead(200, { "Content-Type": "application/json" });
            return res.end(JSON.stringify({
                success: true,
                count: results.length,
                users: results
            }));

        } catch (error) {
            console.error("Server error during search:", error.message);
            res.writeHead(500, { "Content-Type": "application/json" });
            return res.end(JSON.stringify({ success: false, error: "Internal server error" }));
        }
    }

    if (url.pathname === "/api/player-profile") {
        let userId = url.searchParams.get("userId");
        const username = url.searchParams.get("username");

        if ((!userId || !userId.trim()) && (!username || !username.trim())) {
            res.writeHead(400, { "Content-Type": "application/json" });
            return res.end(JSON.stringify({ success: false, error: "userId or username is required" }));
        }

        try {
            if (!userId && username) {
                const uRes = await fetchRoblox("https://users.roblox.com/v1/usernames/users", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ usernames: [username.trim()], excludeBannedUsers: false })
                });
                if (uRes.ok) {
                    const uData = await uRes.json();
                    if (uData.data && uData.data.length > 0) {
                        userId = String(uData.data[0].id);
                    }
                }
            }

            if (!userId || !/^\d+$/.test(userId)) {
                res.writeHead(404, { "Content-Type": "application/json" });
                return res.end(JSON.stringify({ success: false, error: "Player not found" }));
            }

            const userData = await getRobloxProfile(userId.trim());
            if (!userData) {
                res.writeHead(502, { "Content-Type": "application/json" });
                return res.end(JSON.stringify({ success: false, error: "Roblox API request failed" }));
            }

            res.writeHead(200, { "Content-Type": "application/json" });
            return res.end(JSON.stringify({
                success: true,
                userId: userData.id,
                username: userData.name,
                displayName: userData.displayName || userData.name,
                created: userData.created || null,
                description: userData.description || "",
                isVerified: userData.isVerified === true
            }));
        } catch (error) {
            console.error("Server error during profile fetch:", error.message);
            res.writeHead(500, { "Content-Type": "application/json" });
            return res.end(JSON.stringify({ success: false, error: "Internal server error" }));
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
});

server.listen(PORT, HOST, () => {
    console.log(`Server running on port ${PORT} at host ${HOST}`);
});

const http = require("http");
const fs = require("fs");
const path = require("path");
const { URL } = require("url");

const PORT = process.env.PORT || 9090;
const HOST = "0.0.0.0";

const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);

    // CORS Headers - Allow Netlify domain, localhost, and any origin
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");

    if (req.method === "OPTIONS") {
        res.writeHead(204);
        return res.end();
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

            const robloxRes = await fetch("https://users.roblox.com/v1/usernames/users", {
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
            const foundUsers = data.data || [];

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
                const thumbRes = await fetch(
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

            const results = users.map(u => ({
                userId: u.id,
                username: u.name,
                displayName: u.displayName || u.name,
                avatar: avatarMap[u.id] || null
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
        const userId = url.searchParams.get("userId");

        if (!userId || !userId.trim()) {
            res.writeHead(400, { "Content-Type": "application/json" });
            return res.end(JSON.stringify({ success: false, error: "userId is required" }));
        }

        try {
            const userRes = await fetch(`https://users.roblox.com/v1/users/${encodeURIComponent(userId.trim())}`);

            if (!userRes.ok) {
                res.writeHead(502, { "Content-Type": "application/json" });
                return res.end(JSON.stringify({ success: false, error: "Roblox API request failed" }));
            }

            const userData = await userRes.json();

            res.writeHead(200, { "Content-Type": "application/json" });
            return res.end(JSON.stringify({
                success: true,
                userId: userData.id,
                username: userData.name,
                displayName: userData.displayName || userData.name,
                created: userData.created || null,
                description: userData.description || ""
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

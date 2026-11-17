// The single PM2 app of market_replay (besides pg-domestic-futures).
// It serves gRPC :18312 + HTTP :18313, supervises the reference / CME / MCP
// services and runs the daily jobs; see config/market_replay_server.toml.
// Start: pm2 start deploy/pm2/market-replay-server.config.cjs && pm2 save
module.exports = {
  apps: [{
    name: "market-replay-server",
    namespace: "market_replay",
    cwd: "/home/fanghaizhou/market_replay",
    script: "/mnt/nvme-raid0-28t/fanghaizhou/market_replay_deploy/current/bin/market-replay-server",
    args: "--config /home/fanghaizhou/market_replay/config/market_replay_server.toml",
    interpreter: "none",
    autorestart: true,
    restart_delay: 5000,
    // SIGINT -> children get SIGTERM with their own grace (<= 30 s each, in
    // parallel); keep this above that so PM2 never SIGKILLs mid-shutdown.
    kill_timeout: 90000,
    // The server stops its own children (separate process groups) gracefully.
    treekill: false,
    env: { RUST_BACKTRACE: "1" },
  }],
};

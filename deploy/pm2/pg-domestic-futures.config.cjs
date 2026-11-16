// Domestic futures metadata PostgreSQL 16 (market_metadata, port 5433), owned by
// fanghaizhou. PM2 sends SIGINT on stop (postgres fast shutdown); kill_timeout
// must stay generous so PM2 never SIGKILLs the postmaster.
module.exports = {
  apps: [{
    name: "pg-domestic-futures",
    script: __dirname + "/run-pg-domestic-futures.sh",
    interpreter: "none",
    autorestart: true,
    max_restarts: 10,
    min_uptime: "30s",
    restart_delay: 5000,
    kill_timeout: 60000,
    out_file: "/mnt/nvme-raid0-28t/fanghaizhou/postgresql/domestic_futures/16/log/pm2-out.log",
    error_file: "/mnt/nvme-raid0-28t/fanghaizhou/postgresql/domestic_futures/16/log/pm2-err.log",
    env: { TZ: "UTC" },
  }],
};

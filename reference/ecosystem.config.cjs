module.exports = {
  apps: [
    {
      name: "cn-futures-reference-api",
      cwd: __dirname,
      script: "scripts/run-api.sh",
      interpreter: "none",
      autorestart: true,
      max_restarts: 10,
      min_uptime: "10s",
      kill_timeout: 10000,
      env: {
        NODE_ENV: "production",
        PYTHONUNBUFFERED: "1",
        TZ: "Asia/Shanghai"
      },
      env_production: {
        NODE_ENV: "production",
        PYTHONUNBUFFERED: "1",
        TZ: "Asia/Shanghai"
      }
    },
    {
      name: "cn-futures-reference-daily-sync",
      cwd: __dirname,
      script: "scripts/run-daily-sync.sh",
      interpreter: "none",
      autorestart: true,
      max_restarts: 10,
      min_uptime: "10s",
      kill_timeout: 30000,
      env: {
        NODE_ENV: "production",
        PYTHONUNBUFFERED: "1",
        TZ: "Asia/Shanghai"
      },
      env_production: {
        NODE_ENV: "production",
        PYTHONUNBUFFERED: "1",
        TZ: "Asia/Shanghai"
      }
    }
  ]
};

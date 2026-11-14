module.exports = {
  apps: [{
    name: "cn-cme-exchange-mcp",
    cwd: __dirname,
    script: "scripts/run-mcp.sh",
    interpreter: "/bin/sh",
    autorestart: true,
    max_restarts: 10,
    min_uptime: "10s",
    restart_delay: 2000,
    kill_timeout: 10000,
    env: {
      EXCHANGE_MCP_HOST: "0.0.0.0",
      EXCHANGE_MCP_PORT: "8780",
      EXCHANGE_MCP_ALLOWED_HOSTS: "172.16.30.38",
      PYTHONUNBUFFERED: "1",
      TZ: "UTC"
    }
  }]
};

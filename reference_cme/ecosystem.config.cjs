module.exports = {
  apps: [
    {
      name: "cme-exchange-info-summary",
      cwd: __dirname,
      script: ".venv/bin/python",
      args: "-m cme_reference.server",
      interpreter: "none",
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      max_restarts: 10,
      min_uptime: "10s",
      restart_delay: 2000,
      kill_timeout: 10000,
      out_file: "logs/pm2-out.log",
      error_file: "logs/pm2-error.log",
      merge_logs: true,
      time: true,
      env: {
        CME_REFERENCE_HOST: "0.0.0.0",
        CME_REFERENCE_PORT: "8770",
        PYTHONUNBUFFERED: "1",
        TZ: "UTC"
      }
    }
  ]
};
